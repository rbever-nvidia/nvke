// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/dsx-ai-factory/nke/controller/internal/agent"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/kube"
)

const (
	machineHealthRequeueInterval = 2 * time.Minute
	machineHealthMaxConcurrent   = 20
)

const (
	machineHealthReasonNodeAgentReachable     = "NodeAgentReachable"
	machineHealthReasonNodeAgentUnreachable   = "NodeAgentUnreachable"
	machineHealthReasonNodeAgentTargetUnknown = "NodeAgentTargetUnknown"
	machineHealthReasonNodeSchedulable        = "NodeSchedulable"
	machineHealthReasonNodeUnschedulable      = "NodeUnschedulable"
	machineHealthReasonMarkedOnNode           = "MarkedOnKubernetesNode"
	machineHealthReasonNotMarkedOnNode        = "NotMarkedOnKubernetesNode"
	machineHealthReasonTenantNodeUnavailable  = "TenantNodeUnavailable"
)

// MachineHealthController reports NKE-specific health for joined
// MachineDeployment workers:
//   - NodeAgentReady reports whether CWE can reach the node agent.
//   - KubernetesUnschedulable reflects the tenant Node's cordon state.
//   - NeedsRecycle and NeedsRepair mirror the corresponding tenant Node
//     conditions written by nv-sentinel.
//
// CAPI already reports Kubernetes readiness through MachineNodeReadyCondition.
// This controller does not replace unhealthy joined workers; disruption remains
// an auto-mode and nv-sentinel policy decision.
type MachineHealthController struct {
	cfg                  config.Config
	k8sClient            client.Client
	tenantClientProvider kube.TenantClientProvider
	nodeAgentClient      agent.Client
	logger               *slog.Logger
}

func NewMachineHealthController(
	cfg config.Config,
	k8sClient client.Client,
	tenantClientProvider kube.TenantClientProvider,
	nodeAgentClient agent.Client,
	logger *slog.Logger,
) *MachineHealthController {
	if logger == nil {
		logger = slog.Default()
	}
	return &MachineHealthController{
		cfg:                  cfg,
		k8sClient:            k8sClient,
		tenantClientProvider: tenantClientProvider,
		nodeAgentClient:      nodeAgentClient,
		logger:               logger,
	}
}

func (c *MachineHealthController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1.Machine{}).
		Named("machine-health").
		WithOptions(controller.Options{MaxConcurrentReconciles: machineHealthMaxConcurrent}).
		Complete(c)
}

func (c *MachineHealthController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	machine := &clusterv1.Machine{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, machine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !machine.DeletionTimestamp.IsZero() ||
		!machineSetOwnedWorker(machine) ||
		!machine.Status.NodeRef.IsDefined() {
		return ctrl.Result{}, nil
	}

	logger := c.logger.With("machineNamespace", machine.Namespace, "machineName", machine.Name)
	node, nodeErr := c.tenantNode(ctx, machine)
	if nodeErr != nil {
		logger.Debug("Tenant Node unavailable, reporting unknown node health", "reason", nodeErr)
	}

	probe := c.probeNodeAgent(ctx, machine)
	if probe.targetErr != nil {
		logger.Debug("Machine cannot be addressed by the node agent client yet", "reason", probe.targetErr)
	}

	patchHelper, err := patch.NewHelper(machine, c.k8sClient)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("building patch helper for Machine %s: %w", machine.Name, err)
	}
	for _, condition := range machineHealthConditions(probe, node, nodeErr) {
		conditions.Set(machine, condition)
	}
	if err := patchHelper.Patch(ctx, machine, patch.WithOwnedConditions{
		Conditions: []string{
			constants.MachineConditionTypeNodeAgentReady,
			constants.MachineConditionTypeKubernetesUnschedulable,
			constants.NodeConditionTypeNeedsRecycle,
			constants.NodeConditionTypeNeedsRepair,
		},
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("patching health conditions on Machine %s: %w", machine.Name, err)
	}

	return ctrl.Result{RequeueAfter: machineHealthRequeueInterval}, nil
}

func (c *MachineHealthController) tenantNode(ctx context.Context, machine *clusterv1.Machine) (*corev1.Node, error) {
	cluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: machine.Namespace,
		Name:      machine.Spec.ClusterName,
	}, cluster); err != nil {
		return nil, fmt.Errorf("getting CAPI Cluster %s: %w", machine.Spec.ClusterName, err)
	}
	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("getting tenant client for cluster %s: %w", cluster.Name, err)
	}

	node := &corev1.Node{}
	if err := tenantClient.Get(ctx, client.ObjectKey{Name: machine.Status.NodeRef.Name}, node); err != nil {
		return nil, fmt.Errorf("getting tenant Node %s: %w", machine.Status.NodeRef.Name, err)
	}
	return node, nil
}

type machineNodeAgentProbe struct {
	targetErr error
	probeErr  error
}

func (c *MachineHealthController) probeNodeAgent(ctx context.Context, machine *clusterv1.Machine) machineNodeAgentProbe {
	target, err := machineNodeAgentTarget(machine, c.cfg.SpiffeEnabled)
	if err != nil {
		return machineNodeAgentProbe{targetErr: err}
	}

	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.NodeHealthCheckTimeout)
	defer cancel()
	if _, err := c.nodeAgentClient.GetAgentVersion(probeCtx, target); err != nil {
		return machineNodeAgentProbe{probeErr: err}
	}
	return machineNodeAgentProbe{}
}

func machineNodeAgentTarget(machine *clusterv1.Machine, spiffeEnabled bool) (db.Node, error) {
	ip := ""
	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineInternalIP {
			ip = address.Address
			break
		}
	}
	if ip == "" {
		return db.Node{}, fmt.Errorf("Machine %s has no internal address", machine.Name)
	}

	hostID, err := uuid.Parse(hostIDForMachine(machine))
	if err != nil {
		return db.Node{}, fmt.Errorf("Machine %s has an invalid host ID: %w", machine.Name, err)
	}
	return db.Node{
		ID:            hostID,
		Name:          machine.Name,
		IP:            ip,
		SpiffeEnabled: spiffeEnabled,
	}, nil
}

func machineHealthConditions(probe machineNodeAgentProbe, node *corev1.Node, nodeErr error) []metav1.Condition {
	return []metav1.Condition{
		machineNodeAgentReadyCondition(probe),
		machineKubernetesUnschedulableCondition(node, nodeErr),
		mirroredTenantNodeCondition(constants.NodeConditionTypeNeedsRecycle, node, nodeErr),
		mirroredTenantNodeCondition(constants.NodeConditionTypeNeedsRepair, node, nodeErr),
	}
}

func machineNodeAgentReadyCondition(probe machineNodeAgentProbe) metav1.Condition {
	switch {
	case probe.targetErr != nil:
		return machineHealthCondition(
			constants.MachineConditionTypeNodeAgentReady,
			metav1.ConditionUnknown,
			machineHealthReasonNodeAgentTargetUnknown,
			probe.targetErr.Error(),
		)
	case probe.probeErr != nil:
		return machineHealthCondition(
			constants.MachineConditionTypeNodeAgentReady,
			metav1.ConditionFalse,
			machineHealthReasonNodeAgentUnreachable,
			probe.probeErr.Error(),
		)
	default:
		return machineHealthCondition(
			constants.MachineConditionTypeNodeAgentReady,
			metav1.ConditionTrue,
			machineHealthReasonNodeAgentReachable,
			"Node agent is reachable",
		)
	}
}

func machineKubernetesUnschedulableCondition(node *corev1.Node, nodeErr error) metav1.Condition {
	if node == nil {
		return unavailableTenantNodeCondition(constants.MachineConditionTypeKubernetesUnschedulable, nodeErr)
	}
	if node.Spec.Unschedulable {
		return machineHealthCondition(
			constants.MachineConditionTypeKubernetesUnschedulable,
			metav1.ConditionTrue,
			machineHealthReasonNodeUnschedulable,
			"Node is unschedulable",
		)
	}
	return machineHealthCondition(
		constants.MachineConditionTypeKubernetesUnschedulable,
		metav1.ConditionFalse,
		machineHealthReasonNodeSchedulable,
		"Node is schedulable",
	)
}

func mirroredTenantNodeCondition(conditionType string, node *corev1.Node, nodeErr error) metav1.Condition {
	if node == nil {
		return unavailableTenantNodeCondition(conditionType, nodeErr)
	}
	for _, condition := range node.Status.Conditions {
		if string(condition.Type) != conditionType {
			continue
		}
		reason := machineHealthReasonMarkedOnNode
		if condition.Status == corev1.ConditionFalse {
			reason = machineHealthReasonNotMarkedOnNode
		} else if condition.Status == corev1.ConditionUnknown {
			reason = machineHealthReasonTenantNodeUnavailable
		}
		return machineHealthCondition(
			conditionType,
			metav1.ConditionStatus(condition.Status),
			reason,
			condition.Message,
		)
	}
	return machineHealthCondition(
		conditionType,
		metav1.ConditionFalse,
		machineHealthReasonNotMarkedOnNode,
		fmt.Sprintf("Node has no %s condition", conditionType),
	)
}

func unavailableTenantNodeCondition(conditionType string, nodeErr error) metav1.Condition {
	message := "Tenant Node data is unavailable"
	if nodeErr != nil {
		message = nodeErr.Error()
	}
	return machineHealthCondition(
		conditionType,
		metav1.ConditionUnknown,
		machineHealthReasonTenantNodeUnavailable,
		message,
	)
}

func machineHealthCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}
