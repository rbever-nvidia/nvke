// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// FIXME: POC: MachineClass is a provisional shape for exploring where
// machine-size-dependent provider and kubelet policy should live. The inline
// provider block is the current throwaway shape, kept so this branch can
// resolve a NICo instance type without adding another CRD. The shape to
// evaluate is an NKE MachineClass that owns kubelet policy and points at
// provider infrastructure policy, for example:
//
//	spec.infrastructure.classRef:
//	  apiGroup: internal.nke.nvidia.com or infrastructure.cluster.x-k8s.io
//	  kind: NicoMachineClass
//	  name: nico-vms-cpu2-mem2gb-disk30gb
//
// Example referenced provider class:
//
//	apiVersion: internal.nke.nvidia.com/v1
//	kind: NicoMachineClass
//	metadata:
//	  name: nico-vms-cpu2-mem2gb-disk30gb
//	spec:
//	  instanceTypeID: a1b2c3d4-0005-4000-8000-000000000001
//
// In that model, NicoMachineClass carries instanceTypeID while NKECluster still
// supplies network placement such as VPC, subnet, and VPC prefix.
//
// MachineClassSpec defines operator-owned machine policy selected by tenant pools
// and control-plane classes.
type MachineClassSpec struct {
	// Provider selects the infrastructure provider contract used by this class.
	// +kubebuilder:validation:Enum=nico
	Provider string `json:"provider"`
	// NICo defines provider-specific inputs for NICo-backed machines.
	// +optional
	NICo *MachineClassNICo `json:"nico,omitempty"`
	// Kubelet defines kubelet policy that depends on the selected machine size.
	// +optional
	Kubelet MachineClassKubelet `json:"kubelet,omitempty"`
}

// MachineClassNICo defines NICo-specific machine sizing.
type MachineClassNICo struct {
	// InstanceTypeID is the NICo instance type used for machines in this class.
	// +kubebuilder:validation:Format=uuid
	// +kubebuilder:validation:MaxLength=36
	InstanceTypeID string `json:"instanceTypeID"`
}

// MachineClassKubelet defines machine-size-dependent kubelet reservations.
type MachineClassKubelet struct {
	// SystemReservedMemory is reserved for system daemons.
	// +optional
	SystemReservedMemory string `json:"systemReservedMemory,omitempty"`
	// KubeReservedMemory is reserved for Kubernetes daemons.
	// +optional
	KubeReservedMemory string `json:"kubeReservedMemory,omitempty"`
}

// MachineClass publishes reusable provider and kubelet policy for a machine size.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,path=machineclasses,singular=machineclass
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=".spec.provider"
// +kubebuilder:printcolumn:name="InstanceType",type=string,JSONPath=".spec.nico.instanceTypeID"
type MachineClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MachineClassSpec `json:"spec"`
}

// MachineClassList is a list of published machine classes.
// +kubebuilder:object:root=true
type MachineClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MachineClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MachineClass{}, &MachineClassList{})
		return nil
	})
}
