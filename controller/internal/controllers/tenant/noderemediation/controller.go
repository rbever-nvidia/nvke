// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package noderemediation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	dbfilters "github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/filters"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

// Reconciler starts node remediation workflows from tenant Kubernetes Node labels.
type Reconciler struct {
	clusterID  uuid.UUID
	cfg        config.Config
	db         *dbwrapper.DB
	client     ctrlclient.Client
	k8sClient  ctrlclient.Client
	logger     *slog.Logger
	remediator *nodeRemediator
}

// NewRegistrar returns a tenant-manager registrar for Kubernetes Node remediation.
func NewRegistrar(cfg config.Config, database *dbwrapper.DB, k8sClient ctrlclient.Client, cweTemporalClient workflowStarter, logger *slog.Logger) tenant.Registrar {
	if logger == nil {
		logger = slog.Default()
	}
	return func(clusterID uuid.UUID, manager ctrl.Manager) error {
		controllerName, controllerOptions := controllerRegistration(clusterID)
		reconciler := &Reconciler{
			clusterID:  clusterID,
			cfg:        cfg,
			db:         database,
			client:     manager.GetClient(),
			k8sClient:  k8sClient,
			logger:     logger.With("controller", "tenant-node-remediation", "clusterID", clusterID),
			remediator: newNodeRemediator(cfg, logger.With("controller", "tenant-node-remediation", "clusterID", clusterID), cweTemporalClient),
		}

		return ctrl.NewControllerManagedBy(manager).
			Named(controllerName).
			WithOptions(controllerOptions).
			For(&corev1.Node{}, builder.WithPredicates(remediationNodePredicate())).
			WatchesRawSource(dbwrapper.NewMappedNodeSource(
				database,
				dbfilters.Node{
					ClusterID: &clusterID,
					Deleting:  ptr.To(false),
				},
				nodeRequestsForDBNode,
			)).
			Complete(reconciler)
	}
}

func remediationNodePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return nodeHasRemediationRequest(e.Object) },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		// Reconcile only when the requested remediation changes; workflow status
		// labels are written back to the Node and should not requeue this controller.
		UpdateFunc: func(e event.UpdateEvent) bool {
			return nodeRemediationRequestChanged(e.ObjectOld, e.ObjectNew) && nodeHasRemediationRequest(e.ObjectNew)
		},
		GenericFunc: func(e event.GenericEvent) bool { return nodeHasRemediationRequest(e.Object) },
	}
}

func nodeHasRemediationRequest(object ctrlclient.Object) bool {
	node, isNode := object.(*corev1.Node)
	if !isNode {
		return false
	}
	labels := node.GetLabels()
	if _, hasTerminateRequest := labels[constants.LabelKeyNodeLabelTerminate]; hasTerminateRequest {
		return true
	}
	if _, hasRebootRequest := labels[constants.LabelKeyNodeLabelReboot]; hasRebootRequest {
		return true
	}
	if nodeNeedsRecycle(node) {
		return true
	}
	_, hasMachineHealthIssue := node.GetAnnotations()[constants.NodeAnnotationKeyMachineHealthIssue]
	return hasMachineHealthIssue
}

func nodeRemediationRequestChanged(oldObject, newObject ctrlclient.Object) bool {
	oldNode, ok := oldObject.(*corev1.Node)
	if !ok {
		return false
	}
	newNode, ok := newObject.(*corev1.Node)
	if !ok {
		return false
	}
	return oldNode.GetLabels()[constants.LabelKeyNodeLabelTerminate] != newNode.GetLabels()[constants.LabelKeyNodeLabelTerminate] ||
		oldNode.GetLabels()[constants.LabelKeyNodeLabelReboot] != newNode.GetLabels()[constants.LabelKeyNodeLabelReboot] ||
		nodeNeedsRecycle(oldNode) != nodeNeedsRecycle(newNode) ||
		oldNode.GetAnnotations()[constants.NodeAnnotationKeyMachineHealthIssue] != newNode.GetAnnotations()[constants.NodeAnnotationKeyMachineHealthIssue]
}

func nodeNeedsRecycle(node *corev1.Node) bool {
	needsRecycle, _ := hasNeedsRecycleNodeCondition(node)
	return needsRecycle
}

func controllerRegistration(clusterID uuid.UUID) (string, controller.Options) {
	return "tenant-node-remediation-" + clusterID.String(), controller.Options{
		SkipNameValidation: ptr.To(true),
	}
}

func nodeRequestsForDBNode(_ context.Context, node db.Node) ([]reconcile.Request, error) {
	if node.Name == "" {
		return nil, nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Name: node.Name,
		},
	}}, nil
}

func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	kubeNode := &corev1.Node{}
	err := r.client.Get(ctx, types.NamespacedName{Name: request.Name}, kubeNode)
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting tenant Kubernetes Node %s for cluster %s: %w", request.Name, r.clusterID, err)
	}
	if !nodeHasRemediationRequest(kubeNode) {
		return reconcile.Result{}, nil
	}

	machine, err := r.machineForTenantNode(ctx, request.Name)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting Machine for tenant Node %s: %w", request.Name, err)
	}

	// Direct NKECluster workers have no db.NodePool. They use the MachineDeployment
	// remediation path, which deletes the Machine when the tenant Node is authorized.
	if nkeClusterWorker(machine) {
		return r.remediateMachineDeploymentWorker(ctx, kubeNode, machine)
	}

	nodePool, err := r.nodePoolForMachine(ctx, machine)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting node pool for Machine %s/%s: %w", machine.Namespace, machine.Name, err)
	}

	switch nodePool.LifecycleMode {
	case db.NodePoolLifecycleModeMachineDeployment:
		return r.remediateMachineDeploymentWorker(ctx, kubeNode, machine)
	case db.NodePoolLifecycleModeLegacy:
		return r.remediateLegacyWorker(ctx, kubeNode)
	default:
		return reconcile.Result{}, fmt.Errorf("unsupported node pool lifecycle %s for Machine %s/%s", nodePool.LifecycleMode, machine.Namespace, machine.Name)
	}
}

func (r *Reconciler) machineForTenantNode(ctx context.Context, nodeName string) (*clusterv1.Machine, error) {
	cluster, err := r.db.GetCluster(ctx, r.clusterID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("cluster %s not found", r.clusterID)
		}
		return nil, fmt.Errorf("getting cluster %s: %w", r.clusterID, err)
	}

	machines := &clusterv1.MachineList{}
	if err := r.k8sClient.List(
		ctx,
		machines,
		ctrlclient.InNamespace(constants.NamespaceNKESystem),
		ctrlclient.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name},
	); err != nil {
		return nil, fmt.Errorf("listing CAPI Machines for cluster %s: %w", cluster.Name, err)
	}

	for i := range machines.Items {
		machine := &machines.Items[i]
		if machine.Status.NodeRef.Name == nodeName {
			return machine, nil
		}
	}

	return nil, fmt.Errorf("no Machine found for tenant Node %s in cluster %s", nodeName, cluster.Name)
}

// FIXME: POC: share the NKECluster label keys instead of hardcoding them here.
func nkeClusterWorker(machine *clusterv1.Machine) bool {
	labels := machine.GetLabels()
	return labels["internal.nke.nvidia.com/nkecluster-name"] != "" && labels["internal.nke.nvidia.com/node-pool-name"] != ""
}

func (r *Reconciler) nodePoolForMachine(ctx context.Context, machine *clusterv1.Machine) (db.NodePool, error) {
	nodePoolIDValue := machine.Labels[constants.LabelKeyNodePoolID]
	if nodePoolIDValue == "" {
		return db.NodePool{}, fmt.Errorf("Machine %s/%s has no %s label", machine.Namespace, machine.Name, constants.LabelKeyNodePoolID)
	}

	nodePoolID, err := uuid.Parse(nodePoolIDValue)
	if err != nil {
		return db.NodePool{}, fmt.Errorf("Machine %s/%s has invalid %s label: %w", machine.Namespace, machine.Name, constants.LabelKeyNodePoolID, err)
	}

	return r.db.GetNodePool(ctx, nodePoolID)
}
