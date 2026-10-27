// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeadmv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsControlledByMachine reports whether obj's controller owner reference points at machine.
func IsControlledByMachine(obj client.Object, machine *clusterv1.Machine) bool {
	owner := metav1.GetControllerOf(obj)
	return owner != nil &&
		owner.APIVersion == clusterv1.GroupVersion.String() &&
		owner.Kind == "Machine" &&
		owner.Name == machine.Name &&
		owner.UID == machine.UID
}

func IsKubeadmConfigRef(ref clusterv1.ContractVersionedObjectReference) bool {
	return ref.APIGroup == kubeadmv1.GroupVersion.Group && ref.Kind == "KubeadmConfig" && ref.Name != ""
}

func IsNicoMachineRef(ref clusterv1.ContractVersionedObjectReference) bool {
	return ref.APIGroup == nicov1.GroupVersion.Group && ref.Kind == "NicoMachine" && ref.Name != ""
}
