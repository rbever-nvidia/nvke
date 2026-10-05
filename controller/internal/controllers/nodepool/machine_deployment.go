// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodepool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiconditions "sigs.k8s.io/cluster-api/util/conditions"
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
	if err := a.updateNodePoolConditions(ctx, &nodePool, machineDeployment); err != nil {
		return reconcile.Result{}, err
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

func (a *machineDeploymentReconciler) updateNodePoolConditions(
	ctx context.Context,
	nodePool *db.NodePool,
	machineDeployment *clusterv1.MachineDeployment,
) error {
	desired := nodePool.DesiredCount
	available := int(ptr.Deref(machineDeployment.Status.AvailableReplicas, 0))
	actual := int(ptr.Deref(machineDeployment.Status.Replicas, 0))
	availableCondition := capiconditions.Get(machineDeployment, clusterv1.MachineDeploymentAvailableCondition)
	converged := machineDeployment.Status.ObservedGeneration == machineDeployment.Generation &&
		actual == desired &&
		available >= desired &&
		availableCondition != nil &&
		availableCondition.Status == metav1.ConditionTrue

	message := machineDeploymentProgressMessage(nodePool, machineDeployment, actual, available, desired)
	provisioning := db.Condition{
		Status:  db.ConditionStatusFromBool(!converged),
		Message: message,
	}
	provisioned := db.Condition{
		Status:  db.ConditionStatusFromBool(converged),
		Message: message,
	}

	previousProvisioning := nodePool.Conditions.Provisioning
	previousProvisioned := nodePool.Conditions.Provisioned
	db.SetCondition(&nodePool.Conditions.Provisioning, provisioning)
	db.SetCondition(&nodePool.Conditions.Provisioned, provisioned)
	if previousProvisioning.Equal(nodePool.Conditions.Provisioning) &&
		previousProvisioned.Equal(nodePool.Conditions.Provisioned) {
		return nil
	}

	if _, err := a.db.UpdateNodePool(ctx, nodePool); err != nil {
		return fmt.Errorf("updating MachineDeployment status for node pool %s: %w", nodePool.ID, err)
	}
	return nil
}

func machineDeploymentProgressMessage(
	nodePool *db.NodePool,
	machineDeployment *clusterv1.MachineDeployment,
	actual, available, desired int,
) string {
	counts := fmt.Sprintf("%d of %d replicas available", available, desired)
	if nodePool.AutoProvisionDisabled {
		return fmt.Sprintf("Automatic provisioning is disabled; %s", counts)
	}
	if machineDeployment.Status.ObservedGeneration != machineDeployment.Generation {
		return fmt.Sprintf(
			"Waiting for MachineDeployment generation %d to be observed; %s",
			machineDeployment.Generation,
			counts,
		)
	}

	for _, conditionType := range []string{
		clusterv1.MachineDeploymentScalingUpCondition,
		clusterv1.MachineDeploymentScalingDownCondition,
		clusterv1.MachineDeploymentRollingOutCondition,
	} {
		condition := capiconditions.Get(machineDeployment, conditionType)
		if condition != nil && condition.Status == metav1.ConditionTrue {
			return fmt.Sprintf("%s; %s", conditionDetail(condition), counts)
		}
	}

	availableCondition := capiconditions.Get(machineDeployment, clusterv1.MachineDeploymentAvailableCondition)
	if availableCondition != nil && availableCondition.Status != metav1.ConditionTrue {
		return fmt.Sprintf("%s; %s", conditionDetail(availableCondition), counts)
	}
	if actual != desired {
		return fmt.Sprintf("Waiting for MachineDeployment replica count to converge; %s", counts)
	}
	return fmt.Sprintf("MachineDeployment is fully available; %s", counts)
}

func conditionDetail(condition *metav1.Condition) string {
	if condition.Message != "" {
		return fmt.Sprintf("%s: %s", condition.Reason, condition.Message)
	}
	return condition.Reason
}

func (a *machineDeploymentReconciler) deleteResources(ctx context.Context, nodePoolID uuid.UUID) error {
	options := []client.DeleteAllOfOption{
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePoolID.String()},
	}
	machineDeployments := &clusterv1.MachineDeploymentList{}
	if err := a.k8sClient.List(
		ctx,
		machineDeployments,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePoolID.String()},
	); err != nil {
		return fmt.Errorf("listing MachineDeployments for node pool %s: %w", nodePoolID, err)
	}
	for i := range machineDeployments.Items {
		machineDeployment := &machineDeployments.Items[i]
		if _, paused := machineDeployment.Annotations[clusterv1.PausedAnnotation]; !paused {
			continue
		}
		base := machineDeployment.DeepCopy()
		delete(machineDeployment.Annotations, clusterv1.PausedAnnotation)
		if err := a.k8sClient.Patch(ctx, machineDeployment, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("unpausing MachineDeployment %s before deletion: %w", machineDeployment.Name, err)
		}
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
