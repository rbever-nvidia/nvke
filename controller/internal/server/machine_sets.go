// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/api"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

func machineDeploymentListOptions(nodePoolID *uuid.UUID, clusterName string) []ctrlclient.ListOption {
	options := []ctrlclient.ListOption{
		ctrlclient.InNamespace(constants.NamespaceNKESystem),
		ctrlclient.HasLabels{clusterv1.MachineDeploymentNameLabel},
	}
	if nodePoolID != nil {
		options = append(options, ctrlclient.MatchingLabels{constants.LabelKeyNodePoolID: nodePoolID.String()})
	}
	if clusterName != "" {
		options = append(options, ctrlclient.MatchingLabels{clusterv1.ClusterNameLabel: clusterName})
	}
	return options
}

func listMachineDeploymentMachineSets(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	nodePoolID *uuid.UUID,
) ([]clusterv1.MachineSet, error) {
	if k8sClient == nil {
		return nil, nil
	}

	machineSets := &clusterv1.MachineSetList{}
	if err := k8sClient.List(ctx, machineSets, machineDeploymentListOptions(nodePoolID, "")...); err != nil {
		return nil, fmt.Errorf("listing MachineDeployment MachineSets: %w", err)
	}
	return machineSets.Items, nil
}

// apiNodeSetFromMachineSet projects the MachineSet identity, desired replicas,
// and Kubernetes version. Legacy NodeSet profile and bootstrap snapshots do not
// exist for MachineDeployment-owned pools and remain empty.
func apiNodeSetFromMachineSet(machineSet *clusterv1.MachineSet) *api.NodeSet {
	if machineSet == nil {
		return nil
	}
	id, err := uuid.Parse(string(machineSet.UID))
	if err != nil {
		return nil
	}
	nodePoolID, err := uuid.Parse(machineSet.Labels[constants.LabelKeyNodePoolID])
	if err != nil {
		return nil
	}

	desiredCount := 0
	if machineSet.Spec.Replicas != nil {
		desiredCount = int(*machineSet.Spec.Replicas)
	}
	return &api.NodeSet{
		ID:                   id,
		NodePoolID:           nodePoolID,
		DesiredCount:         desiredCount,
		SystemReservedMemory: "",
		KubernetesVersion:    machineSet.Spec.Template.Spec.Version,
	}
}

func currentMachineSetID(machineSets []clusterv1.MachineSet) *uuid.UUID {
	var current *clusterv1.MachineSet
	currentRevision := int64(-1)
	for i := range machineSets {
		revision, err := strconv.ParseInt(machineSets[i].Annotations[clusterv1.RevisionAnnotation], 10, 64)
		if err != nil {
			revision = 0
		}
		if current == nil ||
			revision > currentRevision ||
			(revision == currentRevision && machineSets[i].CreationTimestamp.After(current.CreationTimestamp.Time)) {
			current = &machineSets[i]
			currentRevision = revision
		}
	}
	if current == nil {
		return nil
	}
	id, err := uuid.Parse(string(current.UID))
	if err != nil {
		return nil
	}
	return &id
}

func (s *nodePoolService) projectCurrentMachineSetIDs(
	ctx context.Context,
	dbNodePools []db.NodePool,
	apiNodePools []api.NodePool,
) error {
	if s.k8sClient == nil {
		return nil
	}

	machineSets, err := listMachineDeploymentMachineSets(ctx, s.k8sClient, nil)
	if err != nil {
		return err
	}
	byNodePool := make(map[uuid.UUID][]clusterv1.MachineSet)
	for i := range machineSets {
		nodePoolID, err := uuid.Parse(machineSets[i].Labels[constants.LabelKeyNodePoolID])
		if err != nil {
			continue
		}
		byNodePool[nodePoolID] = append(byNodePool[nodePoolID], machineSets[i])
	}

	for i := range dbNodePools {
		if !dbNodePools[i].UsesMachineDeployment() {
			continue
		}
		apiNodePools[i].CurrentNodeSetID = currentMachineSetID(byNodePool[dbNodePools[i].ID])
	}
	return nil
}
