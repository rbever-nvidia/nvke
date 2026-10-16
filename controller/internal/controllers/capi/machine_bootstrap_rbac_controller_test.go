// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	"github.com/dsx-ai-factory/nvke/controller/internal/config"
)

func TestMachineBootstrapRBACControllerAddsSpireSubject(t *testing.T) {
	ctx := context.Background()
	cluster := newBootstrapRBACCluster()
	machine := newBootstrapRBACMachine()
	tenantClient := newBootstrapRBACTenantClient(t, bootstrapRBACObjects(nil)...)
	controller := newMachineBootstrapRBACControllerForTest(t,
		config.Config{WorkerBootstrapIdentitySource: config.WorkerBootstrapIdentitySourceSPIRE},
		[]client.Object{cluster, machine},
		tenantClient,
	)

	_, err := controller.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(machine)})
	require.NoError(t, err)

	requireBootstrapSubject(t, ctx, tenantClient, "spiffe://test.example/kubeadm/join/machine-uid", true)
}

func TestMachineBootstrapRBACControllerRemovesSpireSubjectAfterJoin(t *testing.T) {
	ctx := context.Background()
	cluster := newBootstrapRBACCluster()
	machine := newBootstrapRBACMachine()
	machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: "node-a"}
	existing := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "spiffe://test.example/kubeadm/join/machine-uid"}
	other := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "spiffe://test.example/kubeadm/join/other-machine"}
	tenantClient := newBootstrapRBACTenantClient(t, bootstrapRBACObjects([]rbacv1.Subject{existing, other})...)
	controller := newMachineBootstrapRBACControllerForTest(t,
		config.Config{WorkerBootstrapIdentitySource: config.WorkerBootstrapIdentitySourceSPIRE},
		[]client.Object{cluster, machine},
		tenantClient,
	)

	_, err := controller.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(machine)})
	require.NoError(t, err)

	requireBootstrapSubject(t, ctx, tenantClient, existing.Name, false)
	requireBootstrapSubject(t, ctx, tenantClient, other.Name, true)
}

func TestMachineBootstrapRBACControllerAddsIMDSSubject(t *testing.T) {
	ctx := context.Background()
	cluster := newBootstrapRBACCluster()
	machine := newBootstrapRBACMachine()
	machine.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{
		APIGroup: nicov1.GroupVersion.Group,
		Kind:     "NicoMachine",
		Name:     "machine-a-infra",
	}
	nicoMachine := &nicov1.NicoMachine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoMachine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "machine-a-infra",
			Namespace: cluster.Namespace,
		},
		Status: nicov1.NicoMachineStatus{
			PrimaryDPUMachineID: "host-abc123",
		},
	}
	tenantClient := newBootstrapRBACTenantClient(t, bootstrapRBACObjects(nil)...)
	controller := newMachineBootstrapRBACControllerForTest(t,
		config.Config{
			WorkerBootstrapIdentitySource: config.WorkerBootstrapIdentitySourceIMDS,
			NodeAgent: config.NodeAgentConfig{
				DSXImds: config.DSXImdsConfig{
					OperatorTenant: "operator-1",
					IssuerURL:      "https://ds9.dsx.nvid.id",
				},
			},
		},
		[]client.Object{cluster, machine, nicoMachine},
		tenantClient,
	)

	_, err := controller.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(machine)})
	require.NoError(t, err)

	requireBootstrapSubject(t, ctx, tenantClient, "spiffe://operator-1.ds9.dsx.nvid.id/bm/host-abc123", true)
}

func newMachineBootstrapRBACControllerForTest(
	t *testing.T,
	cfg config.Config,
	managementObjects []client.Object,
	tenantClient client.Client,
) *MachineBootstrapRBACController {
	t.Helper()

	managementClient := fake.NewClientBuilder().
		WithScheme(newMachineBootstrapRBACTestScheme(t)).
		WithObjects(managementObjects...).
		Build()
	return &MachineBootstrapRBACController{
		cfg:                  cfg,
		k8sClient:            managementClient,
		tenantClientProvider: &fakeTenantClusterClientProvider{tenantClient: tenantClient},
		logger:               slog.New(slog.DiscardHandler),
		trustDomain:          "test.example",
	}
}

func newMachineBootstrapRBACTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, nicov1.AddToScheme(scheme))
	return scheme
}

func newBootstrapRBACTenantClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, rbacv1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func newBootstrapRBACCluster() *clusterv1.Cluster {
	return &clusterv1.Cluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Cluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "cluster-test",
		},
	}
}

func newBootstrapRBACMachine() *clusterv1.Machine {
	return &clusterv1.Machine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Machine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "machine-a",
			Namespace: "cluster-test",
			UID:       types.UID("machine-uid"),
		},
		Spec: clusterv1.MachineSpec{
			ClusterName: "test-cluster",
		},
	}
}

func bootstrapRBACObjects(subjects []rbacv1.Subject) []client.Object {
	return []client.Object{
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: bootstrapClusterRoleBindingName},
			Subjects:   append([]rbacv1.Subject(nil), subjects...),
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: bootstrapGetNodesClusterRoleBindingName},
			Subjects:   append([]rbacv1.Subject(nil), subjects...),
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: bootstrapKubeletConfigRoleBindingName, Namespace: metav1.NamespaceSystem},
			Subjects:   append([]rbacv1.Subject(nil), subjects...),
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: bootstrapNodesKubeadmConfigRoleBindingName, Namespace: metav1.NamespaceSystem},
			Subjects:   append([]rbacv1.Subject(nil), subjects...),
		},
	}
}

func requireBootstrapSubject(t *testing.T, ctx context.Context, tenantClient client.Client, subject string, present bool) {
	t.Helper()

	for _, binding := range bootstrapBindingRefs {
		var subjects []rbacv1.Subject
		if binding.cluster {
			clusterRoleBinding := &rbacv1.ClusterRoleBinding{}
			require.NoError(t, tenantClient.Get(ctx, client.ObjectKey{Name: binding.name}, clusterRoleBinding))
			subjects = clusterRoleBinding.Subjects
		} else {
			roleBinding := &rbacv1.RoleBinding{}
			require.NoError(t, tenantClient.Get(ctx, client.ObjectKey{Namespace: binding.namespace, Name: binding.name}, roleBinding))
			subjects = roleBinding.Subjects
		}

		found := false
		for _, existing := range subjects {
			if existing.Kind == rbacv1.UserKind && existing.APIGroup == rbacv1.GroupName && existing.Name == subject {
				found = true
				break
			}
		}
		require.Equal(t, present, found, "binding %s/%s", binding.namespace, binding.name)
	}
}
