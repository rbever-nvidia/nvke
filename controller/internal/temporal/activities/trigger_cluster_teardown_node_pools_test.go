// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package activities

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/testutil"
)

func TestTriggerClusterTeardownNodePoolsRoutesByLifecycleMode(t *testing.T) {
	clusterID := uuid.New()
	legacyPool := db.NodePool{ID: uuid.New(), ClusterID: clusterID}
	machineDeploymentPool := db.NodePool{
		ID:            uuid.New(),
		ClusterID:     clusterID,
		LifecycleMode: db.NodePoolLifecycleModeMachineDeployment,
	}
	mockDB := &testutil.MockDB{}
	mockTx := &testutil.MockTx{}
	mockDB.MockTx = mockTx
	mockDB.On("RunInTx", mock.Anything, mock.Anything).Return(nil)
	mockTx.On("ListNodePools", mock.Anything, db.NodePoolFilters{ClusterID: &clusterID}).
		Return([]db.NodePool{legacyPool, machineDeploymentPool}, nil)
	mockTx.On("CreateWorkflowOutboxEntry",
		mock.Anything,
		"TeardownNodePool",
		constants.TeardownNodePoolWorkflowPrefix+legacyPool.ID.String(),
		mock.Anything,
	).Return(db.WorkflowOutbox{}, nil).Once()
	mockTx.On("UpdateNodePool", mock.Anything, mock.MatchedBy(func(pool *db.NodePool) bool {
		return pool.ID == machineDeploymentPool.ID && pool.Conditions.Deleting.Status == db.ConditionStatusTrue
	})).Return(machineDeploymentPool, nil).Once()

	activitySuite := &testsuite.WorkflowTestSuite{}
	env := activitySuite.NewTestActivityEnvironment()
	activities := &Activities{db: mockDB}
	env.RegisterActivity(activities.triggerClusterTeardownNodePools)
	value, err := env.ExecuteActivity(
		activities.triggerClusterTeardownNodePools,
		TriggerClusterTeardownNodePoolsInput{ClusterID: clusterID},
	)

	require.NoError(t, err)
	var result TriggerClusterTeardownNodePoolsResult
	require.NoError(t, value.Get(&result))
	require.ElementsMatch(t, []uuid.UUID{legacyPool.ID, machineDeploymentPool.ID}, result.NodePoolIDs)
	mockTx.AssertExpectations(t)
}
