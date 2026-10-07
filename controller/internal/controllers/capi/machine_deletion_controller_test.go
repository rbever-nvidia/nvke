// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/testutil"
)

func TestUnit_MachineDeletionPublishesNeedsRecycleWhileWaitingForTerminate(t *testing.T) {
	ctx := context.Background()
	nodePoolID := uuid.New()
	clusterName := "nico-vms"
	machineName := "nico-vms-md-abcde"
	nodeName := "worker-1"
	deletionTime := metav1.NewTime(time.Now())

	mgmtScheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(mgmtScheme))
	mgmtClient := fake.NewClientBuilder().WithScheme(mgmtScheme).WithRuntimeObjects(
		&clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: constants.NamespaceNKESystem}},
		&clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:              machineName,
				Namespace:         constants.NamespaceNKESystem,
				DeletionTimestamp: &deletionTime,
				Finalizers:        []string{"example.com/finalizer"},
				Annotations: map[string]string{
					constants.MachineAnnotationKeyDisruptionAuthorizationHook: "",
				},
				Labels: map[string]string{
					constants.LabelKeyNodePoolID: nodePoolID.String(),
				},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "MachineSet", Name: "nico-vms-md-abcde"}},
			},
			Spec: clusterv1.MachineSpec{ClusterName: clusterName},
			Status: clusterv1.MachineStatus{
				NodeRef: clusterv1.MachineNodeReference{Name: nodeName},
			},
		},
	).Build()

	tenantScheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(tenantScheme))
	tenantClient := fake.NewClientBuilder().WithScheme(tenantScheme).WithStatusSubresource(&corev1.Node{}).WithRuntimeObjects(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}},
	).Build()

	database := &testutil.MockDB{}
	database.On("GetNodePool", mock.Anything, nodePoolID).Return(db.NodePool{
		ID:            nodePoolID,
		LifecycleMode: db.NodePoolLifecycleModeMachineDeployment,
	}, nil)

	controller := &MachineDeletionController{
		cfg:                  config.Config{NodeSignalDisruptionAuthorizationEnabled: true},
		k8sClient:            mgmtClient,
		db:                   &dbwrapper.DB{DB: database},
		tenantClientProvider: staticTenantClientProvider{client: tenantClient},
		logger:               slog.Default(),
	}

	result, err := controller.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: constants.NamespaceNKESystem, Name: machineName}})
	require.NoError(t, err)
	require.Equal(t, machineDeletionAuthorizationRequeue, result.RequeueAfter)

	node := &corev1.Node{}
	require.NoError(t, tenantClient.Get(ctx, client.ObjectKey{Name: nodeName}, node))
	require.Condition(t, func() bool {
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle) && condition.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	})
}

type staticTenantClientProvider struct {
	client client.Client
}

func (p staticTenantClientProvider) ClientForCluster(context.Context, *clusterv1.Cluster) (client.Client, error) {
	return p.client, nil
}
