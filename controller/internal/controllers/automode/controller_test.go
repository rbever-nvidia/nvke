// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package automode

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/temporal/workflows"
	"github.com/dsx-ai-factory/nke/controller/internal/testutil"
)

func TestUnit_ReconcileSkipsNodePools(t *testing.T) {
	ctx := context.Background()
	nodePoolID := uuid.New()
	currentNodeSetID := uuid.New()

	type parameters struct {
		nodePool db.NodePool
	}

	tests := map[string]parameters{
		"missing node pool": {
			nodePool: db.NodePool{},
		},
		"auto mode disabled": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.AutoModeDisabled = true
			}),
		},
		"MachineDeployment lifecycle": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.LifecycleMode = db.NodePoolLifecycleModeMachineDeployment
			}),
		},
		"control plane": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.ControlPlane = true
			}),
		},
		"deleting": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.Conditions.Deleting = db.NewCondition(true, "Deleting")
			}),
		},
		"no recycle budget": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.RecycleBudget = 0
			}),
		},
		"no current node set": {
			nodePool: baseNodePool(nodePoolID, currentNodeSetID, func(nodePool *db.NodePool) {
				nodePool.CurrentNodeSetID = nil
			}),
		},
	}

	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			mockDB := &testutil.MockDB{}
			mockTx := &testutil.MockTx{}
			mockDB.MockTx = mockTx
			if name == "missing node pool" {
				mockDB.On("GetNodePool", mock.Anything, nodePoolID).Return(db.NodePool{}, db.ErrNotFound)
			} else {
				mockDB.On("GetNodePool", mock.Anything, nodePoolID).Return(params.nodePool, nil)
				if params.nodePool.LifecycleMode != db.NodePoolLifecycleModeMachineDeployment {
					mockTx.On("GetNodePoolForUpdate", mock.Anything, nodePoolID).Return(params.nodePool, nil)
				}
			}
			if params.nodePool.LifecycleMode == db.NodePoolLifecycleModeMachineDeployment {
				mockDB.On("GetCluster", mock.Anything, params.nodePool.ClusterID).Return(db.Cluster{}, db.ErrNotFound)
			} else if name != "missing node pool" {
				mockDB.On("RunInTx", mock.Anything, mock.Anything).Return(nil)
			}

			controller := NewController(config.Config{}, slog.Default(), dbwrapper.New(mockDB, slog.Default()), nil, nil)

			result, err := controller.Reconcile(ctx, dbreconcile.NodePoolKey(nodePoolID))

			require.NoError(t, err)
			assert.Equal(t, reconcile.Result{}, result)
			mockDB.AssertExpectations(t)
			mockTx.AssertExpectations(t)
			mockTx.AssertNotCalled(t, "ListNodes", mock.Anything, mock.Anything)
			mockTx.AssertNotCalled(t, "ListNodesEligibleForRecycling", mock.Anything, mock.Anything, mock.Anything)
			mockTx.AssertNotCalled(t, "UpdateNode", mock.Anything, mock.Anything)
			mockTx.AssertNotCalled(t, "CreateWorkflowOutboxEntry", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestUnit_ReconcileMarksDeprecatedNodesWithinBudget(t *testing.T) {
	ctx := context.Background()
	nodePoolID := uuid.New()
	currentNodeSetID := uuid.New()
	nodePool := baseNodePool(nodePoolID, currentNodeSetID)
	nodePool.DesiredCount = 3
	nodePool.RecycleBudget = 2
	availableNodes := []db.Node{
		{ID: uuid.New(), NodePoolID: nodePoolID, NodeSetID: currentNodeSetID},
		{ID: uuid.New(), NodePoolID: nodePoolID, NodeSetID: currentNodeSetID},
	}
	recyclingNode := db.Node{
		ID:         uuid.New(),
		NodePoolID: nodePoolID,
		NodeSetID:  uuid.New(),
		Name:       "worker-old-0",
	}

	mockDB := &testutil.MockDB{}
	mockTx := &testutil.MockTx{}
	mockDB.MockTx = mockTx
	mockDB.On("GetNodePool", mock.Anything, nodePoolID).Return(nodePool, nil)
	mockDB.On("RunInTx", mock.Anything, mock.Anything).Return(nil)
	mockTx.On("GetNodePoolForUpdate", mock.Anything, nodePoolID).Return(nodePool, nil)
	mockTx.On("ListNodes", mock.Anything, availableNodeFilters(nodePoolID)).Return(availableNodes, nil)
	mockTx.On("ListNodesEligibleForRecycling", mock.Anything, nodePoolID, 1).Return([]db.Node{recyclingNode}, nil)
	mockTx.On("GetNodeForUpdate", mock.Anything, recyclingNode.ID).Return(recyclingNode, nil)
	mockTx.On("GetNode", mock.Anything, recyclingNode.ID).Return(recyclingNode, nil)
	mockTx.On("UpdateNode", mock.Anything, mock.MatchedBy(func(node *db.Node) bool {
		return node.ID == recyclingNode.ID &&
			node.Conditions.MarkedForRecycling.Status == db.ConditionStatusTrue &&
			node.Conditions.MarkedForRecycling.Message == "Marked for auto-mode recycling"
	})).Return(recyclingNode, nil)
	mockTx.On(
		"CreateWorkflowOutboxEntry",
		mock.Anything,
		workflows.ApplyK8sNodeConditionWorkflowName,
		constants.ApplyK8sNodeConditionWorkflowPrefix+recyclingNode.ID.String(),
		mock.MatchedBy(func(input workflows.ApplyK8sNodeConditionInput) bool {
			return input.NodeID == recyclingNode.ID &&
				input.NodeCondition.Type == constants.NodeConditionTypeNeedsRecycle &&
				input.NodeCondition.Status == corev1.ConditionTrue &&
				input.NodeCondition.Reason == "Deprecated" &&
				input.NodeCondition.Message == "NKE auto-mode recycling deprecated node"
		}),
	).Return(db.WorkflowOutbox{}, nil)

	controller := NewController(config.Config{}, slog.Default(), dbwrapper.New(mockDB, slog.Default()), nil, nil)

	result, err := controller.Reconcile(ctx, dbreconcile.NodePoolKey(nodePoolID))

	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
	mockDB.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestUnit_ReconcileSelfEnqueueIsIdempotent(t *testing.T) {
	ctx := context.Background()
	nodePoolID := uuid.New()
	currentNodeSetID := uuid.New()
	nodePool := baseNodePool(nodePoolID, currentNodeSetID)
	nodePool.DesiredCount = 1
	nodePool.RecycleBudget = 1
	recyclingNode := db.Node{
		ID:         uuid.New(),
		NodePoolID: nodePoolID,
		NodeSetID:  uuid.New(),
		Name:       "worker-old-0",
	}

	mockDB := &testutil.MockDB{}
	mockTx := &testutil.MockTx{}
	mockDB.MockTx = mockTx
	mockDB.On("GetNodePool", mock.Anything, nodePoolID).Return(nodePool, nil).Twice()
	mockDB.On("RunInTx", mock.Anything, mock.Anything).Return(nil).Twice()
	mockTx.On("GetNodePoolForUpdate", mock.Anything, nodePoolID).Return(nodePool, nil).Twice()
	mockTx.On("ListNodes", mock.Anything, availableNodeFilters(nodePoolID)).Return([]db.Node{{ID: uuid.New(), NodePoolID: nodePoolID}}, nil).Twice()
	mockTx.On("ListNodesEligibleForRecycling", mock.Anything, nodePoolID, 1).Return([]db.Node{recyclingNode}, nil).Once()
	mockTx.On("ListNodesEligibleForRecycling", mock.Anything, nodePoolID, 1).Return([]db.Node{}, nil).Once()
	mockTx.On("GetNodeForUpdate", mock.Anything, recyclingNode.ID).Return(recyclingNode, nil).Once()
	mockTx.On("GetNode", mock.Anything, recyclingNode.ID).Return(recyclingNode, nil).Once()
	mockTx.On("UpdateNode", mock.Anything, mock.MatchedBy(func(node *db.Node) bool {
		return node.ID == recyclingNode.ID &&
			node.Conditions.MarkedForRecycling.Status == db.ConditionStatusTrue
	})).Return(recyclingNode, nil).Once()
	mockTx.On("CreateWorkflowOutboxEntry", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(db.WorkflowOutbox{}, nil).
		Once()

	controller := NewController(config.Config{}, slog.Default(), dbwrapper.New(mockDB, slog.Default()), nil, nil)

	_, err := controller.Reconcile(ctx, dbreconcile.NodePoolKey(nodePoolID))
	require.NoError(t, err)
	_, err = controller.Reconcile(ctx, dbreconcile.NodePoolKey(nodePoolID))
	require.NoError(t, err)

	mockDB.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestUnit_ClassifyMachineDeploymentMachinesExcludesNeedsRecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	tenantClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-recycling"},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle), Status: corev1.ConditionTrue},
			}},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-available"},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			}},
		},
	).Build()
	machines := []clusterv1.Machine{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "machine-recycling"},
			Status: clusterv1.MachineStatus{
				NodeRef: clusterv1.MachineNodeReference{Name: "worker-recycling"},
				Conditions: []metav1.Condition{
					{Type: clusterv1.MachineNodeReadyCondition, Status: metav1.ConditionTrue},
					{Type: clusterv1.MachineUpToDateCondition, Status: metav1.ConditionFalse},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "machine-available"},
			Status: clusterv1.MachineStatus{
				NodeRef: clusterv1.MachineNodeReference{Name: "worker-available"},
				Conditions: []metav1.Condition{
					{Type: clusterv1.MachineNodeReadyCondition, Status: metav1.ConditionTrue},
				},
			},
		},
	}

	classification := classifyMachinesForAutoMode(context.Background(), tenantClient, machines)

	require.Equal(t, 1, classification.availableCount)
	require.Empty(t, classification.stale)
}

func TestUnit_NodePoolKeysForNodeSet(t *testing.T) {
	nodePoolID := uuid.New()

	keys, err := nodePoolKeysForNodeSet(context.Background(), db.NodeSet{NodePoolID: nodePoolID})
	require.NoError(t, err)
	assert.Equal(t,
		[]dbreconcile.Key{dbreconcile.NodePoolKey(nodePoolID)},
		keys,
	)
	keys, err = nodePoolKeysForNodeSet(context.Background(), db.NodeSet{})
	require.NoError(t, err)
	assert.Nil(t, keys)
}

func TestUnit_NodePoolKeysForNode(t *testing.T) {
	nodePoolID := uuid.New()

	keys, err := nodePoolKeysForNode(context.Background(), db.Node{NodePoolID: nodePoolID})
	require.NoError(t, err)
	assert.Equal(t,
		[]dbreconcile.Key{dbreconcile.NodePoolKey(nodePoolID)},
		keys,
	)
	keys, err = nodePoolKeysForNode(context.Background(), db.Node{})
	require.NoError(t, err)
	assert.Nil(t, keys)
}

func baseNodePool(id, currentNodeSetID uuid.UUID, opts ...func(*db.NodePool)) db.NodePool {
	nodePool := db.NodePool{
		ID:               id,
		ClusterID:        uuid.New(),
		Name:             "workers",
		DesiredCount:     1,
		RecycleBudget:    1,
		LifecycleMode:    db.NodePoolLifecycleModeLegacy,
		CurrentNodeSetID: &currentNodeSetID,
		Conditions:       db.NodePoolConditions{},
	}
	for _, opt := range opts {
		opt(&nodePool)
	}
	return nodePool
}

func availableNodeFilters(nodePoolID uuid.UUID) db.NodeFilters {
	return db.NodeFilters{
		NodePoolID:              &nodePoolID,
		KubernetesReady:         ptrTo(true),
		KubernetesUnschedulable: ptrTo(false),
		Deleting:                ptrTo(false),
		MarkedForRecycling:      ptrTo(false),
		MarkedForRepair:         ptrTo(false),
	}
}

func ptrTo[T any](v T) *T {
	return &v
}
