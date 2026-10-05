// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodeprovisioningmonitor

import (
	"context"
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
)

// Controller watches nodes that are in provisioning
// and recycles them once they exceed the configured timeout.
type Controller struct {
	cfg       config.Config
	db        *dbwrapper.DB
	k8sClient client.Client
	logger    *slog.Logger
}

func NewController(
	cfg config.Config,
	logger *slog.Logger,
	dbInstance *dbwrapper.DB,
	k8sClient client.Client,
) *Controller {
	return &Controller{
		cfg:       cfg,
		db:        dbInstance,
		k8sClient: k8sClient,
		logger:    logger,
	}
}

func (c *Controller) SetupWithManager(mgr manager.Manager) error {
	return dbreconcile.AddController(mgr, dbreconcile.Controller{
		Name:       "node-provisioning-monitor",
		Reconciler: c,
		Sources: []dbreconcile.Source{
			dbwrapper.NewNodeSource(c.db, dbfilters.Node{
				Provisioning: ptr.To(true),
				Deleting:     ptr.To(false),
			}),
			dbwrapper.NewNodePoolSource(c.db, dbfilters.NodePool{ControlPlane: ptr.To(false)}),
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

func nodePoolKeyForMachine(_ context.Context, object client.Object) []dbreconcile.Key {
	nodePoolID, err := uuid.Parse(object.GetLabels()[constants.LabelKeyNodePoolID])
	if err != nil {
		return nil
	}
	return []dbreconcile.Key{dbreconcile.NodePoolKey(nodePoolID)}
}

func (c *Controller) Reconcile(ctx context.Context, key dbreconcile.Key) (reconcile.Result, error) {
	if c.cfg.NodeProvisioningTimeout <= 0 {
		c.logger.Debug("Node provisioning monitor disabled, skipping reconciliation", "key", key)
		return reconcile.Result{}, nil
	}

	// Legacy provisioning is tracked per database Node, while CAPI provisioning
	// is tracked for all Machines in a NodePool. The MachineDeployment path
	// validates the NodePool lifecycle mode before acting.
	switch key.Kind {
	case dbreconcile.KindNode:
		return c.reconcileLegacy(ctx, key)
	case dbreconcile.KindNodePool:
		return c.reconcileMachineDeployment(ctx, key.ID)
	default:
		return reconcile.Result{}, fmt.Errorf("unsupported provisioning monitor key %s", key)
	}
}
