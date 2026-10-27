// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/keys"
)

const (
	// BootstrapIdentityResolvedCondition reports whether the Machine's expected bootstrap username has been resolved.
	BootstrapIdentityResolvedCondition = keys.Domain + "/BootstrapIdentityResolved"

	// BootstrapIdentityResolvedReason means the Machine bootstrap identity annotation was published.
	BootstrapIdentityResolvedReason = "BootstrapIdentityResolved"
	// BootstrapIdentityPublishFailedReason means publishing the resolved bootstrap identity failed.
	BootstrapIdentityPublishFailedReason = "BootstrapIdentityPublishFailed"
	// BootstrapIdentityNicoMachineRefInvalidReason means the selected resolver requires a NicoMachine infrastructure ref.
	BootstrapIdentityNicoMachineRefInvalidReason = "NicoMachineRefInvalid"
	// BootstrapIdentityWaitingForNicoMachineReason means the controller is waiting for the referenced NicoMachine.
	BootstrapIdentityWaitingForNicoMachineReason = "WaitingForNicoMachine"
	// BootstrapIdentityNicoMachineOwnershipInvalidReason means the referenced NicoMachine is not controlled by the Machine.
	BootstrapIdentityNicoMachineOwnershipInvalidReason = "NicoMachineOwnershipInvalid"
	// BootstrapIdentityWaitingForNicoTPMHashReason means the controller is waiting for the NicoMachine TPM EK public hash.
	BootstrapIdentityWaitingForNicoTPMHashReason = "WaitingForNicoTPMHash"
	// BootstrapIdentitySpireWorkloadEntryConfigInvalidReason means the KubeadmConfig SPIRE workload entry config annotation is invalid.
	BootstrapIdentitySpireWorkloadEntryConfigInvalidReason = "SpireWorkloadEntryConfigInvalid"
)

func SetBootstrapIdentityResolvedCondition(
	ctx context.Context,
	k8sClient client.Client,
	machine *clusterv1.Machine,
	status metav1.ConditionStatus,
	reason string,
	message string,
	fieldOwner string,
) error {
	conditions := []metav1.Condition{}
	if current := meta.FindStatusCondition(machine.Status.Conditions, BootstrapIdentityResolvedCondition); current != nil {
		conditions = append(conditions, *current)
	}

	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               BootstrapIdentityResolvedCondition,
		Status:             status,
		ObservedGeneration: machine.Generation,
		Reason:             reason,
		Message:            message,
	})

	applyMachine := &clusterv1.Machine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Machine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      machine.Name,
			Namespace: machine.Namespace,
		},
		Status: clusterv1.MachineStatus{
			Conditions: conditions,
		},
	}

	if err := k8sClient.Status().Patch(
		ctx,
		applyMachine,
		client.Apply,
		client.FieldOwner(fieldOwner),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch Machine %s condition: %w", BootstrapIdentityResolvedCondition, err)
	}

	return nil
}
