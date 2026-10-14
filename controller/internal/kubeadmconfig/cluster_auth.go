// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package kubeadmconfig

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/config"
	"github.com/dsx-ai-factory/nvke/controller/internal/controllerconfig"
	"github.com/dsx-ai-factory/nvke/controller/internal/nvkerbac"
)

// SupportsAuthenticationConfiguration reports whether kube-apiserver at
// kubernetesVersion can read the v1 AuthenticationConfiguration this package
// renders, which needs 1.34 or later.
func SupportsAuthenticationConfiguration(kubernetesVersion string) bool {
	return supportsAnonymousAuthConfig(kubernetesVersion)
}

// ClusterAuthentication is what a cluster's AuthenticationConfiguration is built from.
type ClusterAuthentication struct {
	ClusterName                   string
	Shared                        config.NvkeOIDCAuth
	SharedEnabled                 bool
	DSXIMDS                       config.DSXImdsConfig
	Providers                     []controllerconfig.OIDCProvider
	TrustDomain                   string
	WorkerBootstrapIdentitySource config.WorkerBootstrapIdentitySource
}

// Needed reports whether the cluster has any OIDC provider.
func (c ClusterAuthentication) Needed() bool {
	hasDSXIMDSBootstrap := c.workerBootstrapIdentitySource() == config.WorkerBootstrapIdentitySourceIMDS && c.DSXIMDS.IssuerURL != ""
	return c.SharedEnabled || hasDSXIMDSBootstrap || len(c.Providers) > 0
}

// BuildClusterAuthenticationConfig builds a cluster's AuthenticationConfiguration.
// A cluster with only the shared provider gets exactly what
// BuildOIDCAuthenticationConfig builds.
func BuildClusterAuthenticationConfig(ctx context.Context, c client.Client, in ClusterAuthentication) (*OIDCAuthenticationConfig, error) {
	if !in.Needed() {
		return nil, errors.New("the cluster uses neither the shared OIDC provider, DSX IMDS, nor fleet-wide providers")
	}

	var caBundle []byte
	if in.SharedEnabled {
		if ref := in.Shared.CABundleSecret; !ref.Empty() {
			var err error
			caBundle, err = ReadOIDCCABundle(ctx, c, ref)
			if err != nil {
				return nil, err
			}
		}
		if len(in.Providers) == 0 {
			authn, err := BuildOIDCAuthenticationConfig(in.ClusterName, in.Shared, caBundle)
			if err != nil {
				return nil, err
			}
			return applyBootstrapAuthenticator(authn, in)
		}
	}
	if !in.SharedEnabled && len(in.Providers) == 0 {
		return applyBootstrapAuthenticator(&OIDCAuthenticationConfig{}, in)
	}

	policy, err := LoadOperatorJWTPolicy(ctx, c, in.TrustDomain)
	if err != nil {
		return nil, err
	}
	authn, err := BuildCombinedAuthenticationConfig(in.ClusterName, controllerconfig.ManagementOIDCAuthConfig{
		Enabled:      in.SharedEnabled,
		IssuerURL:    in.Shared.IssuerURL,
		DiscoveryURL: in.Shared.DiscoveryURL,
	}, caBundle, in.Providers, policy)
	if err != nil {
		return nil, err
	}
	return applyBootstrapAuthenticator(authn, in)
}

func (c ClusterAuthentication) workerBootstrapIdentitySource() config.WorkerBootstrapIdentitySource {
	if c.WorkerBootstrapIdentitySource == "" {
		return config.WorkerBootstrapIdentitySourceIMDS
	}
	return c.WorkerBootstrapIdentitySource
}

func applyBootstrapAuthenticator(authn *OIDCAuthenticationConfig, in ClusterAuthentication) (*OIDCAuthenticationConfig, error) {
	switch in.workerBootstrapIdentitySource() {
	case config.WorkerBootstrapIdentitySourceIMDS:
		return appendDSXIMDSAuthenticator(authn, in.ClusterName, in.DSXIMDS)
	case config.WorkerBootstrapIdentitySourceSPIRE:
		if !in.SharedEnabled {
			return nil, errors.New("SPIRE worker bootstrap requires the shared OIDC provider")
		}
		authn.JWT[0].ClaimMappings.Groups.Expression = spireJoinBootstrapGroupExpression()
		return authn, nil
	default:
		return nil, fmt.Errorf("unsupported worker bootstrap identity source %q", in.WorkerBootstrapIdentitySource)
	}
}

// LoadOperatorJWTPolicy reserves the management SPIFFE trust domain and the User
// subjects nke-rbac binds. With nke-rbac present, the trust domain is required.
func LoadOperatorJWTPolicy(ctx context.Context, reader client.Reader, trustDomain string) (OperatorJWTPolicy, error) {
	if reader == nil {
		return OperatorJWTPolicy{}, errors.New("reading nke-rbac needs a management-cluster client")
	}
	policy := OperatorJWTPolicy{ManagementJWTTrustDomain: trustDomain}

	manifest, err := nvkerbac.ReadManifest(ctx, reader)
	if apierrors.IsNotFound(err) {
		return policy, nil
	}
	if err != nil {
		return OperatorJWTPolicy{}, fmt.Errorf("reading the identities fleet-wide OIDC providers must not claim: %w", err)
	}
	if trustDomain == "" {
		return OperatorJWTPolicy{}, errors.New("nke-rbac binds NVKE identities, but the management SPIFFE trust domain is unknown")
	}

	policy.ReservedSubjects, err = nvkerbac.UserSubjects(manifest)
	if err != nil {
		return OperatorJWTPolicy{}, err
	}
	return policy, nil
}
