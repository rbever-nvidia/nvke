// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/keys"
)

func applyBootstrapIdentityAnnotation(
	ctx context.Context,
	k8sClient client.Client,
	machine *clusterv1.Machine,
	bootstrapIdentity string,
	fieldOwner string,
) error {
	applyMachine := &clusterv1.Machine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Machine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      machine.Name,
			Namespace: machine.Namespace,
			Annotations: map[string]string{
				keys.MachineBootstrapIdentity.String(): bootstrapIdentity,
			},
		},
	}

	if err := k8sClient.Patch(
		ctx,
		applyMachine,
		client.Apply,
		client.FieldOwner(fieldOwner),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to apply machine bootstrap identity annotation: %w", err)
	}
	return nil
}

func removeBootstrapIdentity(
	ctx context.Context,
	k8sClient client.Client,
	logger *slog.Logger,
	machine *clusterv1.Machine,
	expectedIdentity string,
) error {
	if keys.MachineBootstrapIdentity.Value(machine.GetAnnotations()) != expectedIdentity {
		return nil
	}

	patchMachine := machine.DeepCopy()
	annotations := patchMachine.GetAnnotations()
	keys.MachineBootstrapIdentity.Delete(annotations)
	patchMachine.SetAnnotations(annotations)

	logger.Info("Removing Machine bootstrap identity annotation", "machineNamespace", machine.Namespace, "machineName", machine.Name)
	if err := k8sClient.Patch(ctx, patchMachine, client.MergeFrom(machine)); err != nil {
		return fmt.Errorf("failed to remove machine bootstrap identity annotation: %w", err)
	}
	return nil
}
