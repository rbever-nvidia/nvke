// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/api"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/temporal/workflows"
)

func (s *nodeService) listMachineDeploymentWorkers(ctx context.Context, params api.ListNodesParams) ([]clusterv1.Machine, error) {
	if s.k8sClient == nil || params.ControlPlane != nil && *params.ControlPlane {
		return nil, nil
	}

	clusterName, err := s.clusterNameForNodeList(ctx, params)
	if err != nil {
		return nil, err
	}
	if params.ClusterID != nil && clusterName == "" {
		return nil, nil
	}

	machines := &clusterv1.MachineList{}
	if err := s.k8sClient.List(ctx, machines, machineDeploymentListOptions(params.NodePoolID, clusterName)...); err != nil {
		return nil, fmt.Errorf("listing MachineDeployment workers: %w", err)
	}
	if params.Name == nil || *params.Name == "" {
		return machines.Items, nil
	}

	filtered := make([]clusterv1.Machine, 0, len(machines.Items))
	for i := range machines.Items {
		if machines.Items[i].Name == *params.Name {
			filtered = append(filtered, machines.Items[i])
		}
	}
	return filtered, nil
}

func (s *nodeService) clusterNameForNodeList(ctx context.Context, params api.ListNodesParams) (string, error) {
	if params.ClusterID == nil {
		if params.ClusterName != nil {
			return *params.ClusterName, nil
		}
		return "", nil
	}
	cluster, err := s.db.GetCluster(ctx, *params.ClusterID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("getting cluster %s: %w", *params.ClusterID, err)
	}
	if params.ClusterName != nil && *params.ClusterName != "" && *params.ClusterName != cluster.Name {
		return "", nil
	}
	return cluster.Name, nil
}

func (s *nodeService) getMachineDeploymentWorker(ctx context.Context, nodeID uuid.UUID) (*clusterv1.Machine, bool, error) {
	if s.k8sClient == nil {
		return nil, false, nil
	}

	machines := &clusterv1.MachineList{}
	if err := s.k8sClient.List(ctx, machines, machineDeploymentListOptions(nil, "")...); err != nil {
		return nil, false, fmt.Errorf("listing MachineDeployment workers: %w", err)
	}
	for i := range machines.Items {
		if string(machines.Items[i].UID) == nodeID.String() {
			return &machines.Items[i], true, nil
		}
	}
	return nil, false, nil
}

func apiNodeFromMachine(machine *clusterv1.Machine) *api.Node {
	if machine == nil {
		return nil
	}
	id, err := uuid.Parse(string(machine.UID))
	if err != nil {
		return nil
	}
	nodePoolID, err := uuid.Parse(machine.Labels[constants.LabelKeyNodePoolID])
	if err != nil {
		return nil
	}

	nodeSetID := uuid.Nil
	for _, owner := range machine.OwnerReferences {
		if owner.Kind != "MachineSet" {
			continue
		}
		if id, err := uuid.Parse(string(owner.UID)); err == nil {
			nodeSetID = id
		}
		break
	}
	if nodeSetID == uuid.Nil {
		return nil
	}

	return &api.Node{
		ID:           id,
		NodePoolID:   nodePoolID,
		NodeSetID:    nodeSetID,
		Name:         machine.Name,
		IP:           machineInternalIP(machine),
		Nico:         nicoProviderFromMachine(machine),
		ControlPlane: false,
		Conditions:   apiConditionsFromMachine(machine),
	}
}

func machineInternalIP(machine *clusterv1.Machine) string {
	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineInternalIP && address.Address != "" {
			return address.Address
		}
	}
	return ""
}

func nicoProviderFromMachine(machine *clusterv1.Machine) *api.NicoNodeProvider {
	if !strings.HasPrefix(machine.Spec.ProviderID, "nico://") {
		return nil
	}
	return &api.NicoNodeProvider{InstanceID: strings.TrimPrefix(machine.Spec.ProviderID, "nico://")}
}

// apiConditionsFromMachine projects CAPI and NKE-owned Machine conditions into
// the existing NodeConditions contract for workers that have no db.Node row.
//
// Pending, provisioning, join, provisioned, and deletion state come from CAPI
// Machine fields and conditions. KubernetesReady is CAPI's NodeReady condition.
// The Machine health controller supplies KubernetesUnschedulable,
// NodeAgentReady, NeedsRecycle, and NeedsRepair.
func apiConditionsFromMachine(machine *clusterv1.Machine) api.NodeConditions {
	fallbackTime := machine.CreationTimestamp.Time
	if fallbackTime.IsZero() {
		fallbackTime = time.Now()
	}

	nodeReady := machineConditionFromMachine(machine, clusterv1.MachineNodeReadyCondition, fallbackTime)
	bootstrapReady := machineConditionFromMachine(machine, clusterv1.MachineBootstrapConfigReadyCondition, fallbackTime)
	deleting := !machine.DeletionTimestamp.IsZero() ||
		machineConditionStatus(machine, clusterv1.MachineDeletingCondition) == db.ConditionStatusTrue
	joined := machine.Status.NodeRef.IsDefined()
	provisioned := joined && nodeReady.Status == db.ConditionStatusTrue
	pending := !deleting && !joined && machine.Spec.ProviderID == ""
	provisioning := !deleting && !pending && !provisioned

	return api.NodeConditions{
		Pending:             apiConditionFromDB(boolCondition(pending, fallbackTime)),
		ProvisioningClaimed: apiConditionFromDB(boolCondition(!pending, fallbackTime)),
		Provisioning:        apiConditionFromDB(boolCondition(provisioning, fallbackTime)),
		ReadyToJoin:         apiConditionFromDB(bootstrapReady),
		Joined:              apiConditionFromDB(boolCondition(joined, fallbackTime)),
		Provisioned:         apiConditionFromDB(boolCondition(provisioned, fallbackTime)),
		DeleteRequested:     apiConditionFromDB(boolCondition(deleting, fallbackTime)),
		Deleting:            apiConditionFromDB(boolCondition(deleting, fallbackTime)),
		KubernetesReady:     apiConditionFromDB(nodeReady),
		KubernetesUnschedulable: apiConditionFromDB(machineConditionFromMachine(
			machine,
			constants.MachineConditionTypeKubernetesUnschedulable,
			fallbackTime,
		)),
		NodeAgentReady: apiConditionFromDB(machineConditionFromMachine(
			machine,
			constants.MachineConditionTypeNodeAgentReady,
			fallbackTime,
		)),
		Rebooting: apiConditionFromDB(db.Condition{
			Status:             db.ConditionStatusUnknown,
			LastTransitionTime: fallbackTime,
		}),
		MarkedForRecycling: apiConditionFromDB(machineConditionFromMachine(
			machine,
			constants.NodeConditionTypeNeedsRecycle,
			fallbackTime,
		)),
		MarkedForRepair: apiConditionFromDB(machineConditionFromMachine(
			machine,
			constants.NodeConditionTypeNeedsRepair,
			fallbackTime,
		)),
	}
}

func boolCondition(value bool, at time.Time) db.Condition {
	status := db.ConditionStatusFalse
	if value {
		status = db.ConditionStatusTrue
	}
	return db.Condition{Status: status, LastTransitionTime: at}
}

func machineConditionStatus(machine *clusterv1.Machine, conditionType string) db.ConditionStatus {
	for _, condition := range machine.Status.Conditions {
		if condition.Type != conditionType {
			continue
		}
		switch condition.Status {
		case metav1.ConditionTrue:
			return db.ConditionStatusTrue
		case metav1.ConditionFalse:
			return db.ConditionStatusFalse
		default:
			return db.ConditionStatusUnknown
		}
	}
	return db.ConditionStatusUnknown
}

func machineConditionFromMachine(machine *clusterv1.Machine, conditionType string, fallbackTime time.Time) db.Condition {
	for _, condition := range machine.Status.Conditions {
		if condition.Type == conditionType {
			return db.Condition{
				Status:             machineConditionStatus(machine, conditionType),
				Message:            condition.Message,
				LastTransitionTime: condition.LastTransitionTime.Time,
			}
		}
	}
	return db.Condition{
		Status:             db.ConditionStatusUnknown,
		LastTransitionTime: fallbackTime,
	}
}

func (s *nodeService) deleteMachineDeploymentWorker(
	ctx context.Context,
	nodeID uuid.UUID,
	machineHealthIssue *workflows.MachineHealthIssue,
) (bool, error) {
	machine, found, err := s.getMachineDeploymentWorker(ctx, nodeID)
	if err != nil || !found {
		return false, err
	}

	base := machine.DeepCopy()
	annotations := machine.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[constants.MachineAnnotationKeyDisruptionAuthorized] = "true"
	if machineHealthIssue != nil {
		data, err := json.Marshal(machineHealthIssue)
		if err != nil {
			return false, fmt.Errorf("marshaling health issue for Machine %s: %w", machine.Name, err)
		}
		annotations[constants.MachineAnnotationKeyMachineHealthIssue] = string(data)
	}
	machine.SetAnnotations(annotations)
	if err := s.k8sClient.Patch(ctx, machine, ctrlclient.MergeFrom(base)); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("authorizing disruption of Machine %s: %w", machine.Name, err)
	}

	if err := s.k8sClient.Delete(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("deleting Machine %s: %w", machine.Name, err)
	}
	return true, nil
}
