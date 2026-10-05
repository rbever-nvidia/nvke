// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package automode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
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
	"github.com/dsx-ai-factory/nke/controller/internal/kube"
)

// Controller is responsible for auto-mode operations.
type Controller struct {
	cfg                  config.Config
	db                   *dbwrapper.DB
	k8sClient            client.Client
	tenantClientProvider kube.TenantClientProvider
	logger               *slog.Logger
}

// NewController creates a new auto-mode controller.
func NewController(
	cfg config.Config,
	logger *slog.Logger,
	dbInstance *dbwrapper.DB,
	k8sClient client.Client,
	tenantClientProvider kube.TenantClientProvider,
) *Controller {
	return &Controller{
		cfg:                  cfg,
		db:                   dbInstance,
		k8sClient:            k8sClient,
		tenantClientProvider: tenantClientProvider,
		logger:               logger,
	}
}

// SetupWithManager wires the controller into controller-runtime with DB and CAPI sources.
func (c *Controller) SetupWithManager(mgr manager.Manager) error {
	return dbreconcile.AddController(mgr, dbreconcile.Controller{
		Name:       "auto-mode-controller",
		Reconciler: c,
		Sources: []dbreconcile.Source{
			dbwrapper.NewNodePoolSource(c.db, dbfilters.NodePool{ControlPlane: ptr.To(false)}),
			dbwrapper.NewNodeSetSource(c.db, dbfilters.NodeSet{}, nodePoolKeysForNodeSet),
			dbwrapper.NewNodeSource(c.db, dbfilters.Node{ControlPlane: ptr.To(false)}, nodePoolKeysForNode),
		},
		Watches: []dbreconcile.KubeWatch{{
			Object: &clusterv1.Machine{},
			LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      constants.LabelKeyNodePoolID,
				Operator: metav1.LabelSelectorOpExists,
			}}},
			Map: nodePoolKeyForMachine,
		}},
	})
}

func nodePoolKeysForNodeSet(_ context.Context, nodeSet db.NodeSet) ([]dbreconcile.Key, error) {
	if nodeSet.NodePoolID == uuid.Nil {
		return nil, nil
	}
	return []dbreconcile.Key{dbreconcile.NodePoolKey(nodeSet.NodePoolID)}, nil
}

func nodePoolKeysForNode(_ context.Context, node db.Node) ([]dbreconcile.Key, error) {
	if node.NodePoolID == uuid.Nil {
		return nil, nil
	}
	return []dbreconcile.Key{dbreconcile.NodePoolKey(node.NodePoolID)}, nil
}

func nodePoolKeyForMachine(_ context.Context, object client.Object) []dbreconcile.Key {
	nodePoolID, err := uuid.Parse(object.GetLabels()[constants.LabelKeyNodePoolID])
	if err != nil {
		return nil
	}
	return []dbreconcile.Key{dbreconcile.NodePoolKey(nodePoolID)}
}

// Reconcile applies auto-mode for a single NodePool key.
func (c *Controller) Reconcile(ctx context.Context, key dbreconcile.Key) (reconcile.Result, error) {
	nodePool, err := c.db.GetNodePool(ctx, key.ID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.logger.Debug("Node pool not found, skipping auto-mode reconciliation", "nodePoolID", key.ID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting node pool %s: %w", key.ID, err)
	}

	switch nodePool.LifecycleMode {
	case db.NodePoolLifecycleModeLegacy:
		return c.reconcileLegacy(ctx, key)
	case db.NodePoolLifecycleModeMachineDeployment:
		return c.reconcileMachineDeployment(ctx, nodePool)
	default:
		return reconcile.Result{}, fmt.Errorf("node pool %s has unsupported lifecycle mode %q", nodePool.ID, nodePool.LifecycleMode)
	}
}
