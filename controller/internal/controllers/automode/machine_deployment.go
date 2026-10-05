// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package automode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

const machineDeploymentAutoModeRequeueInterval = 2 * time.Minute

// reconcileMachineDeployment:
//
//   - Counts a Machine as available before recycle only when it has joined, its
//     CAPI NodeReady condition and tenant Node Ready condition are true, the Node
//     is schedulable, it does not need repair, and deletion is either not started
//     or still held at NKE's pre-drain authorization hook.
//   - Excludes Machines whose tenant Node already has NeedsRecycle=True from the
//     available count.
//   - Applies the same available-deficit calculation as legacy auto-mode to
//     determine how much of RecycleBudget remains for this iteration.
//   - Prioritizes Machines already selected by CAPI for scale-down, then selects
//     the oldest Machine with UpToDate=False for OnDelete rollout.
//   - Deletes at most one stale Machine per pass and publishes NeedsRecycle so
//     nv-sentinel can authorize removal. Without node-signal authorization,
//     deletion proceeds through normal CAPI drain and in-progress deletions count
//     against the recycle budget.
func (c *Controller) reconcileMachineDeployment(ctx context.Context, nodePool db.NodePool) (reconcile.Result, error) {
	result := reconcile.Result{RequeueAfter: machineDeploymentAutoModeRequeueInterval}
	logger := c.logger.With(
		"nodePoolID", nodePool.ID,
		"nodePoolName", nodePool.Name,
		"nodePoolRecycleBudget", nodePool.RecycleBudget,
	)

	if nodePool.ControlPlane ||
		nodePool.Conditions.Deleting.Status == db.ConditionStatusTrue ||
		nodePool.AutoModeDisabled ||
		nodePool.RecycleBudget <= 0 {
		return reconcile.Result{}, nil
	}

	cluster, err := c.db.GetCluster(ctx, nodePool.ClusterID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return result, fmt.Errorf("getting cluster %s: %w", nodePool.ClusterID, err)
	}

	machines := &clusterv1.MachineList{}
	if err := c.k8sClient.List(
		ctx,
		machines,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePool.ID.String()},
	); err != nil {
		return result, fmt.Errorf("listing Machines for node pool %s: %w", nodePool.ID, err)
	}

	capiCluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: constants.NamespaceNKESystem,
		Name:      cluster.Name,
	}, capiCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return result, nil
		}
		return result, fmt.Errorf("getting CAPI Cluster %s: %w", cluster.Name, err)
	}
	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, capiCluster)
	if err != nil {
		return result, fmt.Errorf("getting tenant client for cluster %s: %w", cluster.Name, err)
	}

	classification := classifyMachinesForAutoMode(ctx, tenantClient, machines.Items)

	// Example 1:
	//  nodePool.DesiredCount = 250
	//  availableCount = 248
	//  nodePool.RecycleBudget = 10
	//  availableDeficit = 250 - 248 = 2
	//  iterationRecycleCount = 10 - 2 = 8
	//
	// Example 2 (insufficient available capacity):
	//  nodePool.DesiredCount = 250
	//  availableCount = 230
	//  nodePool.RecycleBudget = 10
	//  availableDeficit = 250 - 230 = 20
	//  iterationRecycleCount = 10 - 20 = -10 -> 0
	//
	// Example 3 (surplus available capacity):
	//  nodePool.DesiredCount = 250
	//  availableCount = 260
	//  nodePool.RecycleBudget = 10
	//  availableDeficit = 250 - 260 = -10 -> 0
	//  iterationRecycleCount = 10 - 0 = 10
	//
	// Unlike the legacy path, this reconciler starts at most one recycle per
	// iteration. A positive iterationRecycleCount only authorizes that recycle;
	// subsequent reconciliations observe its effect before starting another.
	availableDeficit := max(nodePool.DesiredCount-classification.availableCount, 0)
	iterationRecycleCount := max(nodePool.RecycleBudget-availableDeficit, 0)
	if iterationRecycleCount == 0 {
		return result, nil
	}

	candidates := classification.stale
	if c.cfg.NodeSignalDisruptionAuthorizationEnabled {
		candidates = append(classification.scaleDown, candidates...)
	}
	if len(candidates) == 0 {
		return result, nil
	}

	machine := &candidates[0]
	if machine.DeletionTimestamp.IsZero() {
		if err := c.k8sClient.Delete(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
			return result, fmt.Errorf("deleting stale Machine %s: %w", machine.Name, err)
		}
	}

	if c.cfg.NodeSignalDisruptionAuthorizationEnabled {
		if err := applyNeedsRecycleCondition(ctx, tenantClient, machine.Status.NodeRef.Name); err != nil {
			return result, fmt.Errorf("publishing recycle signal for Machine %s: %w", machine.Name, err)
		}
	}

	logger.Info(
		"Started MachineDeployment worker recycle",
		"machine", machine.Name,
		"node", machine.Status.NodeRef.Name,
		"nodeSignalAuthorizationEnabled", c.cfg.NodeSignalDisruptionAuthorizationEnabled,
	)
	return result, nil
}

type autoModeClassification struct {
	availableCount int
	scaleDown      []clusterv1.Machine
	stale          []clusterv1.Machine
}

// FIXME: Verify this logic in detail for acutal implementation.
func classifyMachinesForAutoMode(ctx context.Context, tenantClient client.Client, machines []clusterv1.Machine) autoModeClassification {
	classification := autoModeClassification{}
	for i := range machines {
		machine := &machines[i]
		if !machine.Status.NodeRef.IsDefined() {
			continue
		}
		node := &corev1.Node{}
		if err := tenantClient.Get(ctx, client.ObjectKey{Name: machine.Status.NodeRef.Name}, node); err != nil {
			continue
		}
		observation := observeMachineForAutoMode(machine, node)
		if !observation.operational || observation.needsRecycle {
			continue
		}
		classification.availableCount++

		if !machine.DeletionTimestamp.IsZero() {
			classification.scaleDown = append(classification.scaleDown, *machine.DeepCopy())
			continue
		}
		upToDate, hasUpToDate := machineConditionStatus(machine, clusterv1.MachineUpToDateCondition)
		if hasUpToDate && upToDate == metav1.ConditionFalse {
			classification.stale = append(classification.stale, *machine.DeepCopy())
		}
	}

	sort.SliceStable(classification.scaleDown, func(i, j int) bool {
		if classification.scaleDown[i].DeletionTimestamp.Equal(classification.scaleDown[j].DeletionTimestamp) {
			return classification.scaleDown[i].Name < classification.scaleDown[j].Name
		}
		return classification.scaleDown[i].DeletionTimestamp.Time.Before(classification.scaleDown[j].DeletionTimestamp.Time)
	})
	sort.SliceStable(classification.stale, func(i, j int) bool {
		if classification.stale[i].CreationTimestamp.Equal(&classification.stale[j].CreationTimestamp) {
			return classification.stale[i].Name < classification.stale[j].Name
		}
		return classification.stale[i].CreationTimestamp.Before(&classification.stale[j].CreationTimestamp)
	})
	return classification
}

type machineAutoModeObservation struct {
	operational  bool
	needsRecycle bool
}

func observeMachineForAutoMode(machine *clusterv1.Machine, node *corev1.Node) machineAutoModeObservation {
	nodeReady, hasNodeReady := machineConditionStatus(machine, clusterv1.MachineNodeReadyCondition)
	tenantNodeReady := nodeConditionStatus(node, string(corev1.NodeReady))
	needsRecycle := nodeConditionStatus(node, constants.NodeConditionTypeNeedsRecycle)
	needsRepair := nodeConditionStatus(node, constants.NodeConditionTypeNeedsRepair)
	deletionHeldBeforeDrain := machine.DeletionTimestamp.IsZero()
	if !machine.DeletionTimestamp.IsZero() {
		_, deletionHeldBeforeDrain = machine.Annotations[constants.MachineAnnotationKeyDisruptionAuthorizationHook]
	}

	return machineAutoModeObservation{
		operational: hasNodeReady &&
			machine.Status.NodeRef.IsDefined() &&
			deletionHeldBeforeDrain &&
			nodeReady == metav1.ConditionTrue &&
			tenantNodeReady == corev1.ConditionTrue &&
			!node.Spec.Unschedulable &&
			needsRepair != corev1.ConditionTrue,
		needsRecycle: needsRecycle == corev1.ConditionTrue,
	}
}

func machineConditionStatus(machine *clusterv1.Machine, conditionType string) (metav1.ConditionStatus, bool) {
	for _, condition := range machine.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status, true
		}
	}
	return "", false
}

func nodeConditionStatus(node *corev1.Node, conditionType string) corev1.ConditionStatus {
	for _, condition := range node.Status.Conditions {
		if string(condition.Type) == conditionType {
			return condition.Status
		}
	}
	if conditionType == constants.NodeConditionTypeNeedsRecycle ||
		conditionType == constants.NodeConditionTypeNeedsRepair {
		return corev1.ConditionFalse
	}
	return corev1.ConditionUnknown
}

func applyNeedsRecycleCondition(ctx context.Context, tenantClient client.Client, nodeName string) error {
	node := &corev1.Node{}
	if err := tenantClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("getting tenant Node %s: %w", nodeName, err)
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle) &&
			condition.Status == corev1.ConditionTrue {
			return nil
		}
	}

	conditionPatch := struct {
		Status struct {
			Conditions []corev1.NodeCondition `json:"conditions"`
		} `json:"status"`
	}{}
	conditionPatch.Status.Conditions = []corev1.NodeCondition{{
		Type:               corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle),
		Status:             corev1.ConditionTrue,
		Reason:             "Deprecated",
		Message:            "NKE auto-mode recycling deprecated node",
		LastTransitionTime: metav1.Now(),
	}}
	patch, err := json.Marshal(conditionPatch)
	if err != nil {
		return fmt.Errorf("marshaling Node condition patch: %w", err)
	}

	patchNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
	if err := tenantClient.Status().Patch(
		ctx,
		patchNode,
		client.RawPatch(types.StrategicMergePatchType, patch),
	); err != nil {
		return fmt.Errorf("applying condition %s to Kubernetes Node %s: %w", constants.NodeConditionTypeNeedsRecycle, nodeName, err)
	}
	return nil
}
