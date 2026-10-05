// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodepool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	"github.com/dsx-ai-factory/nke/controller/internal/capi"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

type machineDeploymentReconciler struct {
	cfg                      config.Config
	db                       *dbwrapper.DB
	k8sClient                client.Client
	trustDomain              string
	logger                   *slog.Logger
}

func (a *machineDeploymentReconciler) Reconcile(ctx context.Context, key dbreconcile.Key) (reconcile.Result, error) {
	nodePool, err := a.db.GetNodePool(ctx, key.ID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return reconcile.Result{}, a.deleteResources(ctx, key.ID)
		}
		return reconcile.Result{}, fmt.Errorf("getting node pool %s: %w", key.ID, err)
	}
	if nodePool.ControlPlane || !nodePool.UsesMachineDeployment() {
		return reconcile.Result{}, nil
	}
	if nodePool.Conditions.Deleting.Status == db.ConditionStatusTrue {
		if err := a.deleteResources(ctx, nodePool.ID); err != nil {
			return reconcile.Result{}, err
		}
		if err := a.db.DeleteNodePool(ctx, nodePool.ID); err != nil {
			return reconcile.Result{}, fmt.Errorf("deleting node pool %s: %w", nodePool.ID, err)
		}
		return reconcile.Result{}, nil
	}

	cluster, err := a.db.GetCluster(ctx, nodePool.ClusterID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting cluster %s: %w", nodePool.ClusterID, err)
	}
	machineDeployment, err := capi.ApplyMachineDeployment(
		ctx,
		a.cfg,
		a.k8sClient,
		a.trustDomain,
		cluster,
		nodePool,
	)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("applying MachineDeployment for node pool %s: %w", nodePool.ID, err)
	}

	a.logger.Debug(
		"Applied MachineDeployment for node pool",
		"cluster", cluster.Name,
		"nodePoolID", nodePool.ID,
		"machineDeployment", machineDeployment.Name,
		"replicas", nodePool.DesiredCount,
	)

	return reconcile.Result{}, nil
}

func (a *machineDeploymentReconciler) deleteResources(ctx context.Context, nodePoolID uuid.UUID) error {
	options := []client.DeleteAllOfOption{
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePoolID.String()},
	}
	objects := []client.Object{
		&clusterv1.MachineDeployment{},
		&nicov1.NicoMachineTemplate{},
		&bootstrapv1.KubeadmConfigTemplate{},
	}
	for _, object := range objects {
		if err := a.k8sClient.DeleteAllOf(ctx, object, options...); err != nil {
			return fmt.Errorf("deleting %T resources for node pool %s: %w", object, nodePoolID, err)
		}
	}
	return nil
}
