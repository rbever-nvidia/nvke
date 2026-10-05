// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodeprovisioningmonitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/metrics"
)

func (c *Controller) reconcileMachineDeployment(ctx context.Context, nodePoolID uuid.UUID) (reconcile.Result, error) {
	nodePool, err := c.db.GetNodePool(ctx, nodePoolID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting node pool %s: %w", nodePoolID, err)
	}
	if !nodePool.UsesMachineDeployment() ||
		nodePool.ControlPlane ||
		nodePool.Conditions.Deleting.Status == db.ConditionStatusTrue ||
		nodePool.ProvisioningMonitorDisabled {
		return reconcile.Result{}, nil
	}

	machines := &clusterv1.MachineList{}
	if err := c.k8sClient.List(
		ctx,
		machines,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePool.ID.String()},
	); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing Machines for node pool %s: %w", nodePool.ID, err)
	}

	now := time.Now()
	var nextCheck time.Duration
	for _, machine := range machines.Items {
		// Skip Machines that are already being deleted or have already joined.
		if !machine.DeletionTimestamp.IsZero() ||
			machine.Status.NodeRef.IsDefined() ||
			!machineSetOwnedWorker(machine) {
			continue
		}

		provisioningAge := now.Sub(machine.CreationTimestamp.Time)
		if provisioningAge < c.cfg.NodeProvisioningTimeout {
			// Re-calculate the next check time to the shortest remaining timeout across all unjoined Machines.
			untilTimeout := c.cfg.NodeProvisioningTimeout - provisioningAge
			if nextCheck == 0 || untilTimeout < nextCheck {
				nextCheck = untilTimeout
			}
			continue
		}

		cluster, err := c.db.GetCluster(ctx, nodePool.ClusterID)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("getting cluster for node pool %s: %w", nodePool.ID, err)
		}

		c.logger.Warn(
			"Deleting worker Machine that never joined within the provisioning timeout",
			"clusterID", nodePool.ClusterID,
			"nodePoolID", nodePool.ID,
			"nodePoolName", nodePool.Name,
			"machine", machine.Name,
			"nodeProvisioningTimeout", c.cfg.NodeProvisioningTimeout.String(),
			"provisioningAge", provisioningAge.String(),
		)
		if err := c.k8sClient.Delete(ctx, &machine); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("deleting Machine %s stuck provisioning: %w", machine.Name, err)
		}
		
		metrics.RecordNodeProvisioningGiveup(
			cluster.NicoSiteID().String(),
			nodePool.ClusterID.String(),
			nodePool.ID.String(),
			cluster.Name,
			nodePool.Name,
		)
	}

	return reconcile.Result{RequeueAfter: nextCheck}, nil
}

func machineSetOwnedWorker(machine clusterv1.Machine) bool {
	if _, controlPlane := machine.Labels[clusterv1.MachineControlPlaneLabel]; controlPlane {
		return false
	}
	for _, owner := range machine.OwnerReferences {
		if owner.Kind == "MachineSet" {
			return true
		}
	}
	return false
}
