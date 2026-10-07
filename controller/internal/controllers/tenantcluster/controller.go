// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	capibuild "github.com/dsx-ai-factory/nke/controller/internal/capi"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/kubeadmconfig"
	enginev1 "github.com/dsx-ai-factory/nke/management-engine/apis/v1"
)

const (
	controllerName     = "tenant-cluster-capi-sync"
	templateHashLength = 16

	labelNKECluster = "internal.nke.nvidia.com/nkecluster-name"
	labelNodePool   = "internal.nke.nvidia.com/node-pool-name"

	workerCRISocket = "unix:///var/run/containerd/containerd.sock"
)

// Controller reconciles the tenant-facing NKECluster API directly into CAPI resources.
type Controller struct {
	cfg    config.Config
	k8s    client.Client
	logger *slog.Logger
}

// NewController returns a tenant NKECluster controller.
func NewController(cfg config.Config, k8s client.Client, logger *slog.Logger) *Controller {
	return &Controller{
		cfg:    cfg,
		k8s:    k8s,
		logger: logger,
	}
}

// SetupWithManager registers the controller with controller-runtime.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		For(&enginev1.NKECluster{}).
		Complete(c)
}

// Reconcile applies MachineDeployments for worker pools embedded in the tenant-facing NKECluster.
func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var cluster enginev1.NKECluster
	if err := c.k8s.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting NKECluster %s: %w", req.NamespacedName, err)
	}

	if len(cluster.Spec.NodePools) == 0 {
		return reconcile.Result{}, nil
	}

	if cluster.Spec.Network == nil {
		c.logger.Info("waiting for NKECluster network before applying node pools", "nkeCluster", req.NamespacedName)
		return reconcile.Result{}, nil
	}

	for _, pool := range cluster.Spec.NodePools {
		if err := c.applyNodePool(ctx, cluster, pool); err != nil {
			return reconcile.Result{}, fmt.Errorf("applying node pool %s: %w", pool.Name, err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) applyNodePool(ctx context.Context, cluster enginev1.NKECluster, pool enginev1.NodePool) error {
	name := capibuild.MachineDeploymentName(cluster.Name, string(pool.Name))
	version := normalizeVersion(cluster.Spec.Release.Channel.Version)
	labels := machineLabels(cluster.Name, string(pool.Name))

	infraTemplate, err := c.buildNicoMachineTemplate(cluster, pool, name, labels)
	if err != nil {
		return err
	}
	bootstrapTemplate, err := buildKubeadmConfigTemplate(cluster, name, labels)
	if err != nil {
		return err
	}
	machineDeployment, err := buildMachineDeployment(cluster, pool, name, infraTemplate.Name, bootstrapTemplate.Name, version, labels)
	if err != nil {
		return err
	}

	for _, object := range []client.Object{infraTemplate, bootstrapTemplate, machineDeployment} {
		if err := c.k8s.Patch(ctx, object, client.Apply, client.FieldOwner(c.cfg.FieldManagerName), client.ForceOwnership); err != nil {
			return fmt.Errorf("applying %s %s: %w", object.GetObjectKind().GroupVersionKind().Kind, object.GetName(), err)
		}
	}
	c.logger.Debug("applied tenant NKECluster node pool", "nkeCluster", cluster.Name, "nodePool", pool.Name, "machineDeployment", machineDeployment.Name)
	return nil
}

func (c *Controller) buildNicoMachineTemplate(
	cluster enginev1.NKECluster,
	pool enginev1.NodePool,
	baseName string,
	labels map[string]string,
) (*nicov1.NicoMachineTemplate, error) {
	spec := nicov1.NicoMachineSpec{
		VPCID: cluster.Spec.Network.VPCID,
		Interfaces: []nicov1.NicoMachineInterface{
			{SubnetID: pool.VPCSubnetID},
		},
		InstanceTypeID:          string(pool.InstanceType),
		IpxeScript:              tenantPOCIPXEScript(pool),
		CloudInitInjectHostname: true,
		Labels:                  labels,
	}
	name, err := templateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming NicoMachineTemplate: %w", err)
	}
	return &nicov1.NicoMachineTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoMachineTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: nicov1.NicoMachineTemplateSpec{
			Template: nicov1.NicoMachineTemplateResource{Spec: spec},
		},
	}, nil
}

func buildKubeadmConfigTemplate(
	cluster enginev1.NKECluster,
	baseName string,
	labels map[string]string,
) (*bootstrapv1.KubeadmConfigTemplate, error) {
	patch := kubeadmconfig.KubeletConfigurationPatch("", "")
	spec := bootstrapv1.KubeadmConfigSpec{
		Files: []bootstrapv1.File{
			{
				Path:        patch.Path,
				Owner:       patch.Owner,
				Permissions: fmt.Sprintf("%04o", patch.Mode),
				Content:     string(patch.Content),
			},
			{
				Path:        constants.CAPISpireTrustBundlePath,
				Owner:       "root:root",
				Permissions: "0644",
				ContentFrom: bootstrapv1.FileSource{
					Secret: bootstrapv1.SecretFileSource{
						Name: constants.CAPISpireTrustBundleObjectName,
						Key:  constants.CAPISpireTrustBundleSecretKey,
					},
				},
			},
		},
		JoinConfiguration: bootstrapv1.JoinConfiguration{
			NodeRegistration: bootstrapv1.NodeRegistrationOptions{
				CRISocket: workerCRISocket,
			},
			Discovery: bootstrapv1.Discovery{
				File: bootstrapv1.FileDiscovery{
					KubeConfigPath: constants.CAPIDiscoveryKubeconfigPath,
					KubeConfig: bootstrapv1.FileDiscoveryKubeConfig{
						User: bootstrapv1.KubeConfigUser{
							Exec: bootstrapv1.KubeConfigAuthExec{
								APIVersion: constants.CAPISpireExecPluginAPIVersion,
								Command:    constants.CAPISpireExecPluginCommand,
								Env: []bootstrapv1.KubeConfigAuthExecEnv{
									{Name: "SPIFFE_ENDPOINT_SOCKET", Value: constants.CAPIWorkerSpiffeEndpointSocket},
									{Name: "SPIFFE_JWT_AUDIENCE", Value: cluster.Name},
									{Name: "SPIFFE_JWT_HINT", Value: constants.CAPIWorkerJoinSpiffeHint},
								},
							},
						},
					},
				},
			},
			Patches: bootstrapv1.Patches{Directory: path.Dir(patch.Path)},
		},
	}
	name, err := templateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming KubeadmConfigTemplate: %w", err)
	}
	return &bootstrapv1.KubeadmConfigTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: bootstrapv1.GroupVersion.String(),
			Kind:       "KubeadmConfigTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: bootstrapv1.KubeadmConfigTemplateSpec{
			Template: bootstrapv1.KubeadmConfigTemplateResource{Spec: spec},
		},
	}, nil
}

func buildMachineDeployment(
	cluster enginev1.NKECluster,
	pool enginev1.NodePool,
	name, infraTemplateName, bootstrapTemplateName, kubernetesVersion string,
	labels map[string]string,
) (*clusterv1.MachineDeployment, error) {
	if pool.Replicas == nil {
		return nil, fmt.Errorf("replicas is nil")
	}
	replicas := *pool.Replicas
	return &clusterv1.MachineDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "MachineDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: clusterv1.MachineDeploymentSpec{
			ClusterName: cluster.Name,
			Replicas:    &replicas,
			Deletion: clusterv1.MachineDeploymentDeletionSpec{
				Order: clusterv1.OldestMachineSetDeletionOrder,
			},
			Rollout: clusterv1.MachineDeploymentRolloutSpec{
				Strategy: clusterv1.MachineDeploymentRolloutStrategy{
					Type: clusterv1.OnDeleteMachineDeploymentStrategyType,
				},
			},
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Template: clusterv1.MachineTemplateSpec{
				ObjectMeta: clusterv1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						constants.MachineAnnotationKeyDisruptionAuthorizationHook: "",
					},
				},
				Spec: clusterv1.MachineSpec{
					ClusterName: cluster.Name,
					Version:     kubernetesVersion,
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							APIGroup: bootstrapv1.GroupVersion.Group,
							Kind:     "KubeadmConfigTemplate",
							Name:     bootstrapTemplateName,
						},
					},
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{
						APIGroup: nicov1.GroupVersion.Group,
						Kind:     "NicoMachineTemplate",
						Name:     infraTemplateName,
					},
				},
			},
		},
	}, nil
}

func templateName(base string, spec any) (string, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshaling template spec: %w", err)
	}
	sum := sha256.Sum256(payload)
	suffix := hex.EncodeToString(sum[:])[:templateHashLength]
	maxBaseLength := 63 - len(suffix) - 1
	if len(base) > maxBaseLength {
		base = strings.TrimRight(base[:maxBaseLength], "-")
	}
	return fmt.Sprintf("%s-%s", base, suffix), nil
}

func machineLabels(clusterName, nodePoolName string) map[string]string {
	return map[string]string{
		clusterv1.ClusterNameLabel: clusterName,
		labelNKECluster:            clusterName,
		labelNodePool:              nodePoolName,
	}
}

func normalizeVersion(version string) string {
	if version == "" {
		return "v0.0.0"
	}
	if strings.HasPrefix(version, "v") {
		return version
	}
	return "v" + version
}

func tenantPOCIPXEScript(pool enginev1.NodePool) string {
	profile := strings.SplitN(string(pool.NodeProfile), ":", 2)[0]
	return fmt.Sprintf("#!ipxe\necho tenant direct CAPI POC for node profile %s\n", profile)
}
