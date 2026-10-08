// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/kubeadmconfig"
)

const (
	workerCRISocket                 = "unix:///var/run/containerd/containerd.sock"
	workerOpenVSwitchPreKubeadmUnit = "systemctl start nke-agent-openvswitch.service"
	templateHashLength              = 16
)

type workerBootstrap struct {
	Files              []bootstrapv1.File
	PreKubeadmCommands []string
}

// ApplyMachineDeployment builds and applies the CAPI resources for a MachineDeployment-owned worker NodePool.
func ApplyMachineDeployment(
	ctx context.Context,
	cfg config.Config,
	k8sClient client.Client,
	trustDomain string,
	cluster db.Cluster,
	nodePool db.NodePool,
) (*clusterv1.MachineDeployment, error) {
	if nodePool.ControlPlane {
		return nil, fmt.Errorf("node pool %s is a control-plane pool", nodePool.ID)
	}
	if !nodePool.UsesMachineDeployment() {
		return nil, fmt.Errorf("node pool %s is not MachineDeployment-owned", nodePool.ID)
	}
	if nodePool.DesiredCount < 0 || nodePool.DesiredCount > math.MaxInt32 {
		return nil, fmt.Errorf("node pool %s desired count %d cannot be represented as MachineDeployment replicas", nodePool.ID, nodePool.DesiredCount)
	}
	// FIXME: Integrate with teleport -> controller plan
	if cfg.Teleport.Enabled && cluster.TeleportEnabled && nodePool.TeleportEnabled {
		return nil, fmt.Errorf("node pool %s enables Teleport, which is not supported for MachineDeployment workers", nodePool.ID)
	}

	version := nodePool.KubernetesVersion
	if version == "" {
		version = cluster.KubernetesVersion
	}
	if version == "" {
		return nil, fmt.Errorf("kubernetes version is empty for node pool %s", nodePool.ID)
	}
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}

	name := MachineDeploymentName(cluster.Name, nodePool.Name)
	infraTemplate, err := buildNicoMachineTemplate(cfg, cluster, nodePool, name)
	if err != nil {
		return nil, err
	}
	poolProvider, err := nodePool.NicoProvider()
	if err != nil {
		return nil, err
	}
	controllerClientCABundle := ""
	if cfg.NodeAgentExternalMTLSEnabled {
		caPEM, err := os.ReadFile(config.NodeAgentExternalTLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("reading controller client CA bundle: %w", err)
		}
		controllerClientCABundle = string(caPEM)
	}
	renderedProfile, err := RenderOSProfileBootstrapData(ctx, BootstrapDataInput{
		Config:                   cfg,
		ControllerClientCABundle: controllerClientCABundle,
		TrustDomain:              trustDomain,
		OSProfileDir:             path.Join(cfg.ProfilesDir, "osProfiles", poolProvider.OSProfile),
		Hostname:                 name,
		SpiffeEnabled:            cfg.SpiffeEnabled,
		DiskSelectionStrategy:    poolProvider.DiskSelectionStrategy,
		KubernetesVersion:        version,
		Cluster:                  cluster,
		IncludeSpireTrustBundle:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("rendering worker bootstrap: %w", err)
	}
	bootstrap, err := translateWorkerProfile(renderedProfile, cluster.CniPlugin)
	if err != nil {
		return nil, fmt.Errorf("translating worker bootstrap: %w", err)
	}
	bootstrap.Files, err = useSpireTrustBundleSecret(bootstrap.Files)
	if err != nil {
		return nil, err
	}
	bootstrapTemplate, err := buildWorkerKubeadmConfigTemplate(cluster, nodePool, name, version, bootstrap)
	if err != nil {
		return nil, err
	}
	machineDeployment, err := buildWorkerMachineDeployment(
		cluster,
		nodePool,
		name,
		infraTemplate.Name,
		bootstrapTemplate.Name,
		version,
	)
	if err != nil {
		return nil, err
	}

	for _, object := range []client.Object{infraTemplate, bootstrapTemplate, machineDeployment} {
		if err := k8sClient.Patch(
			ctx,
			object,
			client.Apply,
			client.FieldOwner(cfg.FieldManagerName),
			client.ForceOwnership,
		); err != nil {
			return nil, fmt.Errorf("applying %s %s: %w", object.GetObjectKind().GroupVersionKind().Kind, object.GetName(), err)
		}
	}
	return machineDeployment, nil
}

// MachineDeploymentName returns the stable MachineDeployment name for a node pool.
func MachineDeploymentName(clusterName, nodePoolName string) string {
	return fmt.Sprintf("%s-%s", clusterName, nodePoolName)
}

func workerTemplateName(base string, spec any) (string, error) {
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

func buildNicoMachineTemplate(
	cfg config.Config,
	cluster db.Cluster,
	nodePool db.NodePool,
	baseName string,
) (*nicov1.NicoMachineTemplate, error) {
	clusterProvider, err := cluster.NicoProvider()
	if err != nil {
		return nil, err
	}
	poolProvider, err := nodePool.NicoProvider()
	if err != nil {
		return nil, err
	}

	ipxeScript, err := RenderIPXEScriptFromProfile(
		path.Join(cfg.ProfilesDir, "osProfiles", poolProvider.OSProfile),
		string(poolProvider.DiskSelectionStrategy),
	)
	if err != nil {
		return nil, err
	}

	machine, err := BuildNicoMachine(NicoMachineInput{
		Name:                     baseName,
		Namespace:                constants.NamespaceNKESystem,
		ClusterName:              cluster.Name,
		VPCID:                    clusterProvider.VpcID,
		InstanceTypeID:           poolProvider.InstanceTypeID,
		SSHKeyGroupIDs:           poolProvider.SSHKeyGroupIDs,
		SubnetIDs:                poolProvider.SubnetIDs,
		VPCPrefixIDs:             poolProvider.VpcPrefixIDs,
		InfinibandPartitionID:    poolProvider.InfinibandPartitionID,
		NVLinkLogicalPartitionID: poolProvider.NvLinkLogicalPartitionID,
		IPXEScript:               ipxeScript,
		InstanceLabels: map[string]string{
			constants.LabelKeyClusterID:  cluster.ID.String(),
			constants.LabelKeyNodePoolID: nodePool.ID.String(),
		},
	})
	if err != nil {
		return nil, err
	}
	spec := *machine.Spec.DeepCopy()
	spec.CloudInitInjectHostname = true
	name, err := workerTemplateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming NicoMachineTemplate: %w", err)
	}
	return &nicov1.NicoMachineTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoMachineTemplate",
		},
		ObjectMeta: machineDeploymentTemplateMetadata(name, cluster.Name, nodePool.ID.String()),
		Spec: nicov1.NicoMachineTemplateSpec{
			Template: nicov1.NicoMachineTemplateResource{Spec: spec},
		},
	}, nil
}

func buildWorkerKubeadmConfigTemplate(
	cluster db.Cluster,
	nodePool db.NodePool,
	baseName, kubernetesVersion string,
	bootstrap workerBootstrap,
) (*bootstrapv1.KubeadmConfigTemplate, error) {
	patch := kubeadmconfig.KubeletConfigurationPatch(nodePool.SystemReservedMemory, nodePool.KubeReservedMemory)
	files := append(append([]bootstrapv1.File(nil), bootstrap.Files...), bootstrapv1.File{
		Path:        patch.Path,
		Owner:       patch.Owner,
		Permissions: fmt.Sprintf("%04o", patch.Mode),
		Content:     string(patch.Content),
	})
	spec := bootstrapv1.KubeadmConfigSpec{
		Files:              files,
		PreKubeadmCommands: bootstrap.PreKubeadmCommands,
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
	name, err := workerTemplateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming KubeadmConfigTemplate: %w", err)
	}
	return &bootstrapv1.KubeadmConfigTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: bootstrapv1.GroupVersion.String(),
			Kind:       "KubeadmConfigTemplate",
		},
		ObjectMeta: machineDeploymentTemplateMetadata(name, cluster.Name, nodePool.ID.String()),
		Spec: bootstrapv1.KubeadmConfigTemplateSpec{
			Template: bootstrapv1.KubeadmConfigTemplateResource{Spec: spec},
		},
	}, nil
}

func buildWorkerMachineDeployment(
	cluster db.Cluster,
	nodePool db.NodePool,
	name, infraTemplateName, bootstrapTemplateName, kubernetesVersion string,
) (*clusterv1.MachineDeployment, error) {
	replicas := int32(nodePool.DesiredCount)
	labels := nodePoolLabels(cluster.Name, nodePool.ID.String())
	annotations := map[string]string{}
	if nodePool.AutoProvisionDisabled {
		annotations[clusterv1.PausedAnnotation] = "true"
	}
	return &clusterv1.MachineDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "MachineDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   constants.NamespaceNKESystem,
			Labels:      labels,
			Annotations: annotations,
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

func useSpireTrustBundleSecret(files []bootstrapv1.File) ([]bootstrapv1.File, error) {
	result := append([]bootstrapv1.File(nil), files...)
	index := -1
	for i := range result {
		if result[i].Path != constants.CAPISpireTrustBundlePath {
			continue
		}
		if index >= 0 {
			return nil, fmt.Errorf("bootstrap contains multiple SPIRE trust bundle files")
		}
		if result[i].Content != "" {
			return nil, fmt.Errorf("SPIRE trust bundle must not be rendered inline")
		}
		index = i
	}
	if index < 0 {
		result = append(result, bootstrapv1.File{
			Path:        constants.CAPISpireTrustBundlePath,
			Owner:       "root:root",
			Permissions: "0644",
		})
		index = len(result) - 1
	}
	result[index].ContentFrom = bootstrapv1.FileSource{
		Secret: bootstrapv1.SecretFileSource{
			Name: constants.CAPISpireTrustBundleObjectName,
			Key:  constants.CAPISpireTrustBundleSecretKey,
		},
	}
	return result, nil
}

func machineDeploymentTemplateMetadata(name, clusterName, nodePoolID string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: constants.NamespaceNKESystem,
		Labels:    nodePoolLabels(clusterName, nodePoolID),
	}
}

func nodePoolLabels(clusterName, nodePoolID string) map[string]string {
	return map[string]string{
		clusterv1.ClusterNameLabel:   clusterName,
		constants.LabelKeyNodePoolID: nodePoolID,
	}
}

func workerPreKubeadmCommands(cniPlugin db.CniPlugin, profileCommands []string) []string {
	commands := append([]string(nil), profileCommands...)
	if cniPlugin != db.CNIOvnKubernetes {
		return commands
	}
	return append(commands, workerOpenVSwitchPreKubeadmUnit)
}

type workerProfileCloudConfig struct {
	WriteFiles    []workerProfileCloudInitFile `yaml:"write_files"`
	RunCmd        []workerProfileRunCmd        `yaml:"runcmd"`
	PackageUpdate *bool                        `yaml:"package_update"`
	PowerState    *yaml.Node                   `yaml:"power_state"`
}

type workerProfileCloudInitFile struct {
	Path        string `yaml:"path"`
	Owner       string `yaml:"owner"`
	Permissions string `yaml:"permissions"`
	Encoding    string `yaml:"encoding"`
	Content     string `yaml:"content"`
}

type workerProfileRunCmd struct {
	command string
}

func (c *workerProfileRunCmd) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		c.command = value.Value
		return nil
	}
	if value.Kind != yaml.SequenceNode {
		return fmt.Errorf("runcmd entries must be strings or lists of strings")
	}
	argv := make([]string, 0, len(value.Content))
	for _, item := range value.Content {
		if item.Kind != yaml.ScalarNode {
			return fmt.Errorf("runcmd argv entries must be strings")
		}
		argv = append(argv, quoteShellArgument(item.Value))
	}
	c.command = strings.Join(argv, " ")
	return nil
}

func translateWorkerProfile(userData string, cniPlugin db.CniPlugin) (workerBootstrap, error) {
	var cfg workerProfileCloudConfig
	if err := yaml.Unmarshal([]byte(userData), &cfg); err != nil {
		return workerBootstrap{}, fmt.Errorf("parsing rendered cloud-config: %w", err)
	}
	if cfg.PowerState != nil {
		return workerBootstrap{}, fmt.Errorf("cloud-config power_state cannot be honored before a CABPK-generated join")
	}

	files := make([]bootstrapv1.File, 0, len(cfg.WriteFiles))
	for _, file := range cfg.WriteFiles {
		if file.Path == "" {
			return workerBootstrap{}, fmt.Errorf("cloud-config has a write_files entry with no path")
		}
		files = append(files, bootstrapv1.File{
			Path:        file.Path,
			Owner:       file.Owner,
			Permissions: file.Permissions,
			Encoding:    bootstrapv1.Encoding(file.Encoding),
			Content:     file.Content,
		})
	}
	commands := make([]string, 0, len(cfg.RunCmd)+1)
	if cfg.PackageUpdate != nil && *cfg.PackageUpdate {
		commands = append(commands, "apt-get update")
	}
	for _, command := range cfg.RunCmd {
		commands = append(commands, command.command)
	}
	commands = workerPreKubeadmCommands(cniPlugin, commands)
	return workerBootstrap{Files: files, PreKubeadmCommands: commands}, nil
}

func quoteShellArgument(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
