// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package automode

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant/nodeconditions"
	enginev1 "github.com/dsx-ai-factory/nke/management-engine/apis/v1"
)

const (
	labelNKEClusterName = "internal.nke.nvidia.com/nkecluster-name"
	labelNKEClusterPool = "internal.nke.nvidia.com/node-pool-name"
)

func (c *Controller) setupNKEClusterAutoMode(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("nkecluster-auto-mode").
		For(&enginev1.NKECluster{}).
		Watches(
			&clusterv1.Machine{},
			handler.EnqueueRequestsFromMapFunc(nkeClusterRequestsForMachine),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				labels := obj.GetLabels()
				return labels[labelNKEClusterName] != "" && labels[labelNKEClusterPool] != ""
			})),
		).
		Complete(&nkeClusterAutoMode{controller: c})
}

type nkeClusterAutoMode struct {
	controller *Controller
}

func (r *nkeClusterAutoMode) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return r.controller.reconcileNKECluster(ctx, req)
}

func nkeClusterRequestsForMachine(_ context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	name := labels[labelNKEClusterName]
	if name == "" || labels[labelNKEClusterPool] == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: name},
	}}
}

func (c *Controller) reconcileNKECluster(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	cluster := &enginev1.NKECluster{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, cluster); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	requeue := false
	for _, pool := range cluster.Spec.NodePools {
		replicas := ptr.Deref(pool.Replicas, 0)
		budget, err := disruptionBudget(pool.Rollout, replicas)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("reading rollout budget for node pool %s: %w", pool.Name, err)
		}
		if budget <= 0 {
			continue
		}
		marked, err := c.markNKEClusterPoolForRecycle(ctx, cluster, pool, int(replicas), budget)
		if err != nil {
			return reconcile.Result{}, err
		}
		if marked {
			requeue = true
		}
	}
	if requeue {
		return reconcile.Result{RequeueAfter: machineDeploymentAutoModeRequeueInterval}, nil
	}
	return reconcile.Result{}, nil
}

func (c *Controller) markNKEClusterPoolForRecycle(
	ctx context.Context,
	cluster *enginev1.NKECluster,
	pool enginev1.NodePool,
	desired int,
	budget int,
) (bool, error) {
	logger := c.logger.With(
		"nkeCluster", cluster.Name,
		"nodePool", pool.Name,
		"rolloutBudget", budget,
	)
	machines := &clusterv1.MachineList{}
	if err := c.k8sClient.List(ctx, machines,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{
			labelNKEClusterName: cluster.Name,
			labelNKEClusterPool: string(pool.Name),
		},
	); err != nil {
		return false, fmt.Errorf("listing Machines for NKECluster %s node pool %s: %w", cluster.Name, pool.Name, err)
	}

	capiCluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: constants.NamespaceNKESystem,
		Name:      cluster.Name,
	}, capiCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting CAPI Cluster %s: %w", cluster.Name, err)
	}
	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, capiCluster)
	if err != nil {
		return false, fmt.Errorf("getting tenant client for cluster %s: %w", cluster.Name, err)
	}

	classification := classifyMachinesForAutoMode(ctx, tenantClient, machines.Items)
	if recycleAllowance(desired, classification.availableCount, budget) == 0 || len(classification.stale) == 0 {
		return false, nil
	}

	machine := &classification.stale[0]
	if err := nodeconditions.ApplyNeedsRecycle(ctx, tenantClient, machine.Status.NodeRef.Name, "NKECluster rollout budget recycling deprecated node"); err != nil {
		return false, fmt.Errorf("publishing recycle signal for Machine %s: %w", machine.Name, err)
	}
	logger.Info("Marked NKECluster worker for recycle", "machine", machine.Name, "node", machine.Status.NodeRef.Name)
	return true, nil
}

func recycleAllowance(desired, available, budget int) int {
	return max(budget-max(desired-available, 0), 0)
}

// disruptionBudget is the smallest continuous rollout budget. Scheduled budgets
// are not evaluated. Percentages round up to whole nodes.
func disruptionBudget(rollout *enginev1.Rollout, replicas int32) (int, error) {
	if rollout == nil {
		return 0, nil
	}
	found := false
	limit := 0
	for _, budget := range rollout.Strategy.Disruption.Budgets {
		if budget.Schedule != "" {
			continue
		}
		nodes, err := disruptionBudgetNodes(budget.Nodes, replicas)
		if err != nil {
			return 0, err
		}
		if !found || nodes < limit {
			limit = nodes
			found = true
		}
	}
	if !found {
		return 0, nil
	}
	return limit, nil
}

func disruptionBudgetNodes(raw string, replicas int32) (int, error) {
	value := strings.TrimSpace(raw)
	if strings.HasSuffix(value, "%") {
		percent, err := strconv.Atoi(strings.TrimSuffix(value, "%"))
		if err != nil {
			return 0, fmt.Errorf("parsing disruption budget %q: %w", raw, err)
		}
		return int((int64(replicas)*int64(percent) + 99) / 100), nil
	}
	nodes, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parsing disruption budget %q: %w", raw, err)
	}
	return nodes, nil
}
