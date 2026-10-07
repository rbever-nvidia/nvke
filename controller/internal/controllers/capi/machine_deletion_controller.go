// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant/nodeconditions"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/kube"
)

const machineDeletionAuthorizationRequeue = 15 * time.Second

// MachineDeletionController releases the worker Machine pre-drain hook after
// NKE disruption policy authorizes deletion.
type MachineDeletionController struct {
	cfg                  config.Config
	k8sClient            client.Client
	db                   db.DB
	tenantClientProvider kube.TenantClientProvider
	logger               *slog.Logger
}

func NewMachineDeletionController(
	cfg config.Config,
	k8sClient client.Client,
	database db.DB,
	tenantClientProvider kube.TenantClientProvider,
	logger *slog.Logger,
) *MachineDeletionController {
	if logger == nil {
		logger = slog.Default()
	}
	return &MachineDeletionController{
		cfg:                  cfg,
		k8sClient:            k8sClient,
		db:                   database,
		tenantClientProvider: tenantClientProvider,
		logger:               logger,
	}
}

func (c *MachineDeletionController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1.Machine{}).
		Named("machine-deletion").
		Complete(c)
}

func (c *MachineDeletionController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	machine := &clusterv1.Machine{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, machine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting Machine %s: %w", req.NamespacedName, err)
	}

	if machine.DeletionTimestamp.IsZero() || !machineSetOwnedWorker(machine) {
		return ctrl.Result{}, nil
	}
	if _, waiting := machine.Annotations[constants.MachineAnnotationKeyDisruptionAuthorizationHook]; !waiting {
		return ctrl.Result{}, nil
	}

	authorized, authorizationReason, err := c.disruptionAuthorized(ctx, machine)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !authorized {
		c.logger.Debug("Waiting for worker Machine disruption authorization", "machineNamespace", machine.Namespace, "machineName", machine.Name, "nodeName", machine.Status.NodeRef.Name, "reason", authorizationReason)
		return ctrl.Result{RequeueAfter: machineDeletionAuthorizationRequeue}, nil
	}

	// Removing the pre-drain lifecycle hook allows the CAPI Machine controller
	// to continue draining and deleting the Machine.
	before := machine.DeepCopy()
	delete(machine.Annotations, constants.MachineAnnotationKeyDisruptionAuthorizationHook)
	if err := c.k8sClient.Patch(ctx, machine, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("releasing disruption authorization hook on Machine %s/%s: %w", machine.Namespace, machine.Name, err)
	}

	c.logger.Info(
		"Authorized worker Machine disruption",
		"machineNamespace", machine.Namespace,
		"machineName", machine.Name,
		"nodeName", machine.Status.NodeRef.Name,
		"reason", authorizationReason,
	)
	return ctrl.Result{}, nil
}

// disruptionAuthorized permits deletion when it was:
// - node-signal disruption authorization is disabled
// - explicitly authorized via NKE machine annotation
// - the Machine never joined
// - its parent CAPI Cluster or node pool is absent or deleting
// - the tenant Node has already been removed
// - nv-sentinel has placed the termination label on the tenant Node
func (c *MachineDeletionController) disruptionAuthorized(ctx context.Context, machine *clusterv1.Machine) (bool, string, error) {
	// If node-signal disruption authorization is disabled, normal CAPI Machine deletion is always permitted.
	if !c.cfg.NodeSignalDisruptionAuthorizationEnabled {
		return true, "node-signal disruption authorization is disabled", nil
	}

	// Explicit authorization via machine annotation.
	if _, authorized := machine.Annotations[constants.MachineAnnotationKeyDisruptionAuthorized]; authorized {
		return true, "Machine has explicit disruption authorization", nil
	}

	// Machine has not joined yet.
	if machine.Status.NodeRef.Name == "" {
		return true, "Machine has not joined", nil
	}

	// Machine's parent CAPI Cluster is absent or deleting.
	cluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.ClusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return true, "CAPI Cluster is absent", nil
		}
		return false, "", fmt.Errorf("getting CAPI Cluster for Machine %s/%s: %w", machine.Namespace, machine.Name, err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return true, "CAPI Cluster is deleting", nil
	}

	// Machine's parent node pool is absent or deleting.
	nodePoolID, err := uuid.Parse(machine.Labels[constants.LabelKeyNodePoolID])
	if err != nil {
		return false, "", fmt.Errorf("Machine %s/%s has invalid %s label: %w", machine.Namespace, machine.Name, constants.LabelKeyNodePoolID, err)
	}
	nodePool, err := c.db.GetNodePool(ctx, nodePoolID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return true, "node pool is absent", nil
		}
		return false, "", fmt.Errorf("getting node pool %s for Machine %s/%s: %w", nodePoolID, machine.Namespace, machine.Name, err)
	}
	if nodePool.Conditions.Deleting.Status == db.ConditionStatusTrue {
		return true, "node pool is deleting", nil
	}
	if !nodePool.UsesMachineDeployment() {
		return false, "", fmt.Errorf("Machine %s/%s belongs to legacy node pool %s", machine.Namespace, machine.Name, nodePool.ID)
	}

	// Check if the tenant Node is already absent.
	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, cluster)
	if err != nil {
		return false, "", fmt.Errorf("getting tenant client for Machine %s/%s: %w", machine.Namespace, machine.Name, err)
	}
	node := &corev1.Node{}
	if err := tenantClient.Get(ctx, client.ObjectKey{Name: machine.Status.NodeRef.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return true, "tenant Node is absent", nil
		}
		return false, "", fmt.Errorf("getting tenant Node %s for Machine %s/%s: %w", machine.Status.NodeRef.Name, machine.Namespace, machine.Name, err)
	}
	if err := nodeconditions.ApplyNeedsRecycle(ctx, tenantClient, machine.Status.NodeRef.Name, "NKE Machine deletion is waiting for disruption authorization"); err != nil {
		return false, "", fmt.Errorf("publishing recycle signal for Machine %s/%s: %w", machine.Namespace, machine.Name, err)
	}

	// Check if the tenant Node has the termination label.
	_, authorized := node.Labels[constants.LabelKeyNodeLabelTerminate]
	if authorized {
		return true, "tenant Node has termination label", nil
	}
	return false, "", nil
}
