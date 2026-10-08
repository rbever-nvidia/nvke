// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/GehirnInc/crypt/sha512_crypt"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/utils"
)

// TrustBundleGetter resolves the SPIFFE trust domain and public trust bundle.
type TrustBundleGetter func(context.Context, config.Config) (string, string, error)

// BootstrapDataInput contains the configuration and per-machine values used to render bootstrap data.
type BootstrapDataInput struct {
	Config                   config.Config
	ControllerClientCABundle string
	TrustDomain              string
	GetTrustDomainBundle     TrustBundleGetter
	OSProfileDir             string
	Hostname                 string
	SpiffeEnabled            bool
	DiskSelectionStrategy    db.DiskSelectionStrategy
	KubernetesVersion        string
	Cluster                  db.Cluster
	Teleport                 TeleportBootstrapConfig
	IncludeSpireTrustBundle  bool
}

// TeleportBootstrapConfig contains optional Teleport settings exposed to bootstrap-data templates.
type TeleportBootstrapConfig struct {
	Enabled      bool
	TokenName    string
	ProxyAddress string
	JoinMethod   string
	Labels       string
}

// RenderOSProfileBootstrapData loads and renders the bootstrap-data template in the selected OS profile.
func RenderOSProfileBootstrapData(ctx context.Context, input BootstrapDataInput) (string, error) {
	templateBytes, err := os.ReadFile(path.Join(input.OSProfileDir, "userData.tmpl"))
	if err != nil {
		return "", fmt.Errorf("reading user data template: %w", err)
	}
	return RenderBootstrapData(ctx, string(templateBytes), input)
}

// RenderBootstrapData renders an OS-profile bootstrap-data template.
func RenderBootstrapData(ctx context.Context, templateText string, input BootstrapDataInput) (string, error) {
	userDataTemplate, err := template.New("userData").Funcs(template.FuncMap{
		"indent":     utils.Indent,
		"quote":      utils.Quote,
		"trimPrefix": utils.TrimPrefix,
	}).Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("parsing user data template: %w", err)
	}

	trustDomain := ""
	trustBundle := ""
	if input.SpiffeEnabled {
		trustDomain = input.TrustDomain
		if input.IncludeSpireTrustBundle {
			if input.GetTrustDomainBundle == nil {
				return "", fmt.Errorf("SPIFFE trust bundle getter is not configured")
			}
			trustDomain, trustBundle, err = input.GetTrustDomainBundle(ctx, input.Config)
			if err != nil {
				return "", fmt.Errorf("getting SPIFFE trust domain and bundle: %w", err)
			}
		} else if trustDomain == "" {
			return "", fmt.Errorf("SPIFFE trust domain is empty")
		}
	}

	cniVersion := db.EffectiveCNIVersion(input.Cluster.CniPlugin, strings.TrimSpace(input.Cluster.CniVersion))
	cniImagePackage, cniChartsPackage, err := cniPackages(input.Cluster.CniPlugin, cniVersion)
	if err != nil {
		return "", err
	}
	files, err := BuildNodeAgentBootstrapFiles(input.Config, input.ControllerClientCABundle)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(input.Config.DgxcTenantUser) == "" {
		return "", fmt.Errorf("DGXC_TENANT_USER is required for Forge OS profile user-data rendering")
	}
	if strings.TrimSpace(input.Config.DgxcTenantPassword) == "" {
		return "", fmt.Errorf("DGXC_TENANT_PASSWORD is required for Forge OS profile user-data rendering")
	}
	hashedPassword, err := sha512_crypt.New().Generate([]byte(input.Config.DgxcTenantPassword), nil)
	if err != nil {
		return "", fmt.Errorf("hashing DGXC tenant password: %w", err)
	}

	data := struct {
		CalicoEnabled         bool
		CNIChartsPackage      string
		CNIImagePackage       string
		DiskSelectionStrategy string
		Files                 []db.CloudInitFile
		HashedPassword        string
		Hostname              string
		KubernetesVersion     string
		NodeAgentPort         string
		OvnEnabled            bool
		SpiffeEnabled         bool
		SpireServerAddress    string
		SpireServerPort       string
		TrustBundle           string
		TrustDomain           string
		TeleportEnabled       bool
		TeleportTokenName     string
		TeleportProxyAddress  string
		TeleportJoinMethod    string
		TeleportLabels        string
		Username              string
	}{
		CalicoEnabled:         input.Cluster.CniPlugin == db.CNICalico,
		CNIChartsPackage:      cniChartsPackage,
		CNIImagePackage:       cniImagePackage,
		DiskSelectionStrategy: string(input.DiskSelectionStrategy),
		Files:                 files,
		HashedPassword:        hashedPassword,
		Hostname:              input.Hostname,
		KubernetesVersion:     input.KubernetesVersion,
		NodeAgentPort:         input.Config.NodeAgent.Port,
		OvnEnabled:            input.Cluster.CniPlugin == db.CNIOvnKubernetes,
		SpiffeEnabled:         input.SpiffeEnabled,
		SpireServerAddress:    input.Config.SpireServerAddress,
		SpireServerPort:       input.Config.SpireServerPort,
		TrustBundle:           trustBundle,
		TrustDomain:           trustDomain,
		TeleportEnabled:       input.Teleport.Enabled,
		TeleportTokenName:     input.Teleport.TokenName,
		TeleportProxyAddress:  input.Teleport.ProxyAddress,
		TeleportJoinMethod:    input.Teleport.JoinMethod,
		TeleportLabels:        input.Teleport.Labels,
		Username:              input.Config.DgxcTenantUser,
	}

	var rendered bytes.Buffer
	if err := userDataTemplate.Execute(&rendered, data); err != nil {
		return "", fmt.Errorf("rendering user data template: %w", err)
	}
	return rendered.String(), nil
}

// BootstrapProfile is the subset of rendered cloud-init that the template-backed CAPI bootstrap path can carry.
type BootstrapProfile struct {
	Files               []db.CloudInitFile
	PreKubeadmCommands []string
}

// RenderBootstrapProfileForCAPI renders a release machineInit template and translates it into CAPI bootstrap fields.
func RenderBootstrapProfileForCAPI(ctx context.Context, templateText string, input BootstrapDataInput) (BootstrapProfile, error) {
	rendered, err := RenderBootstrapData(ctx, templateText, input)
	if err != nil {
		return BootstrapProfile{}, err
	}
	bootstrap, err := translateWorkerProfile(rendered, input.Cluster.CniPlugin)
	if err != nil {
		return BootstrapProfile{}, err
	}
	files := make([]db.CloudInitFile, 0, len(bootstrap.Files))
	for _, file := range bootstrap.Files {
		files = append(files, db.CloudInitFile{
			Path:        file.Path,
			Content:     file.Content,
			Owner:       file.Owner,
			Permissions: file.Permissions,
			Encoding:    string(file.Encoding),
		})
	}
	return BootstrapProfile{
		Files:               files,
		PreKubeadmCommands: append([]string(nil), bootstrap.PreKubeadmCommands...),
	}, nil
}

// BuildNodeAgentBootstrapFiles builds the node-agent configuration files included in OS profile user data.
func BuildNodeAgentBootstrapFiles(cfg config.Config, controllerClientCABundle string) ([]db.CloudInitFile, error) {
	files := make([]db.CloudInitFile, 0, 2)
	args := []string{"--secure-server-port", cfg.NodeAgent.Port}
	if cfg.NodeAgentExternalMTLSEnabled {
		if controllerClientCABundle == "" {
			return nil, fmt.Errorf("controller client CA bundle is required when external node-agent mTLS is enabled")
		}
		files = append(files, db.CloudInitFile{
			Path:        config.NodeAgentControllerCAPath,
			Owner:       "root:root",
			Permissions: "0644",
			Encoding:    "b64",
			Content:     base64.StdEncoding.EncodeToString([]byte(controllerClientCABundle)),
		})
		args = append(args, "--controller-client-ca-bundle="+config.NodeAgentControllerCAPath)
	}
	if cfg.NodeAgent.TLSJWT.Enabled {
		args = append(args, "--grpc-auth-mode=tls-jwt", "--dsx-identity-exchange-url="+cfg.NodeAgent.TLSJWT.DSXIdentityExchangeURL)
	}
	files = append(files, db.CloudInitFile{
		Path:        config.NodeAgentConfigPath,
		Owner:       "root:root",
		Permissions: "0644",
		Content:     fmt.Sprintf("AGENT_ARGS=%q\n", strings.Join(args, " ")),
	})
	return files, nil
}

func cniPackages(plugin db.CniPlugin, version string) (string, string, error) {
	switch plugin {
	case db.CNICalico:
		return "nke-cni-calico-images-" + version, "nke-cni-calico-manifests-" + version, nil
	case db.CNIOvnKubernetes:
		return "nke-cni-ovn-images-" + version, "nke-cni-ovn-charts-" + version, nil
	default:
		return "", "", fmt.Errorf("unsupported CNI plugin: %s", plugin)
	}
}
