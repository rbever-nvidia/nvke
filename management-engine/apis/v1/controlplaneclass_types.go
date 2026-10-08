// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// FIXME: POC: This is just a placeholder for now for POC work.
// The shape was just agent derived to meet the current needs of the POC, not intended to be the final shape.
//
// ControlPlaneClassSpec defines reusable control-plane sizing and provider configuration.
type ControlPlaneClassSpec struct {
	// Replicas defines the control-plane size this class requests by default.
	Replicas ControlPlaneClassReplicas `json:"replicas"`
	// Machine defines provider-specific control-plane machine template inputs.
	Machine ControlPlaneClassMachine `json:"machine"`
	// Bootstrap defines extra CABPK bootstrap inputs for control-plane machines.
	// +optional
	Bootstrap ControlPlaneClassBootstrap `json:"bootstrap,omitempty"`
}

// ControlPlaneClassReplicas defines default control-plane sizing.
type ControlPlaneClassReplicas struct {
	// Default is the number of control-plane machines for clusters using this class.
	// +kubebuilder:validation:Minimum=1
	Default int `json:"default"`
	// MaxSurge is the maximum number of extra control-plane machines during replacement.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxSurge int `json:"maxSurge,omitempty"`
}

// ControlPlaneClassMachine defines provider inputs for control-plane machines.
type ControlPlaneClassMachine struct {
	// MachineClass selects machine-size-dependent provider and kubelet policy.
	// +optional
	MachineClass ResourceName `json:"machineClass,omitempty"`
	// FIXME: POC: These raw templates still carry NICo placement such as vpcID
	// and vpcPrefixID. That data likely belongs in provider-specific cluster
	// placement, not in a reusable ControlPlaneClass.
	// InfrastructureTemplatePreferences are ordered infrastructure machine templates.
	// The controller tries them in order when provisioning or replacing control-plane machines.
	// +kubebuilder:validation:MinItems=1
	InfrastructureTemplatePreferences []runtime.RawExtension `json:"infrastructureTemplatePreferences"`
	// SpireAttestationMethod selects the SPIRE node attestation method for control-plane machines.
	// +optional
	// +kubebuilder:validation:Enum=tpm;joinToken;dsxImds
	SpireAttestationMethod string `json:"spireAttestationMethod,omitempty"`
}

// ControlPlaneClassBootstrap defines CABPK bootstrap additions for control-plane machines.
type ControlPlaneClassBootstrap struct {
	// Files are additional cloud-init files rendered into the KubeadmConfig.
	// +optional
	Files []ControlPlaneClassBootstrapFile `json:"files,omitempty"`
	// PreKubeadmCommands run before kubeadm on control-plane machines.
	// +optional
	PreKubeadmCommands []string `json:"preKubeadmCommands,omitempty"`
}

// ControlPlaneClassBootstrapFile defines one control-plane bootstrap file.
type ControlPlaneClassBootstrapFile struct {
	Path        string `json:"path"`
	Content     string `json:"content"`
	Owner       string `json:"owner,omitempty"`
	Permissions string `json:"permissions,omitempty"`
	Encoding    string `json:"encoding,omitempty"`
}

// ControlPlaneClass publishes reusable control-plane policy and provider configuration.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,path=controlplaneclasses,singular=controlplaneclass
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=".spec.replicas.default"
type ControlPlaneClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ControlPlaneClassSpec `json:"spec"`
}

// ControlPlaneClassList is a list of control-plane classes.
// +kubebuilder:object:root=true
type ControlPlaneClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ControlPlaneClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ControlPlaneClass{}, &ControlPlaneClassList{})
		return nil
	})
}
