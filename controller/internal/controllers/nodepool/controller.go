// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodepool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	dbfilters "github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/filters"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

// Controller is responsible for reconciling node pools.
type Controller struct {
	db                *dbwrapper.DB
	legacy            *legacyReconciler
	machineDeployment *machineDeploymentReconciler
	logger            *slog.Logger
}

func NewController(
	cfg config.Config,
	logger *slog.Logger,
	dbInstance *dbwrapper.DB,
	k8sClient client.Client,
	trustDomain string,
) *Controller {
	controller := &Controller{
		db: dbInstance,
		legacy: &legacyReconciler{
			cfg:    cfg,
			db:     dbInstance,
			logger: logger,
		},
		logger: logger,
		machineDeployment: &machineDeploymentReconciler{
			cfg:         cfg,
			db:          dbInstance,
			k8sClient:   k8sClient,
			trustDomain: trustDomain,
			logger:      logger,
		},
	}
	return controller
}

func (a *Controller) SetupWithManager(mgr manager.Manager) error {
	return dbreconcile.AddController(mgr, dbreconcile.Controller{
		Name:       "node-pool-controller",
		Reconciler: a,
		Sources: []dbreconcile.Source{
			dbwrapper.NewNodePoolSource(a.db, dbfilters.NodePool{}),
			dbwrapper.NewNodeSource(a.db, dbfilters.Node{}, nodePoolKeysForNode),
		},
		Watches: []dbreconcile.KubeWatch{{
			Object:    &clusterv1.MachineDeployment{},
			Namespace: constants.NamespaceNKESystem,
			LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      constants.LabelKeyNodePoolID,
				Operator: metav1.LabelSelectorOpExists,
			}}},
			Map: func(_ context.Context, object client.Object) []dbreconcile.Key {
				nodePoolID, err := uuid.Parse(object.GetLabels()[constants.LabelKeyNodePoolID])
				if err != nil {
					return nil
				}
				return []dbreconcile.Key{dbreconcile.NodePoolKey(nodePoolID)}
			},
		}},
	})
}

func nodePoolKeysForNode(_ context.Context, node db.Node) ([]dbreconcile.Key, error) {
	if node.NodePoolID == uuid.Nil {
		return nil, nil
	}
	return []dbreconcile.Key{dbreconcile.NodePoolKey(node.NodePoolID)}, nil
}

func (a *Controller) Reconcile(ctx context.Context, key dbreconcile.Key) (reconcile.Result, error) {
	nodePool, err := a.db.GetNodePool(ctx, key.ID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			a.logger.Debug("Node pool not found, skipping reconciliation", "nodePoolID", key.ID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting node pool %s: %w", key.ID, err)
	}

	switch nodePool.LifecycleMode {
	case db.NodePoolLifecycleModeLegacy:
		return a.legacy.Reconcile(ctx, key)
	case db.NodePoolLifecycleModeMachineDeployment:
		return a.machineDeployment.Reconcile(ctx, key)
	default:
		return reconcile.Result{}, fmt.Errorf("node pool %s has unsupported lifecycle mode %q", nodePool.ID, nodePool.LifecycleMode)
	}
}
