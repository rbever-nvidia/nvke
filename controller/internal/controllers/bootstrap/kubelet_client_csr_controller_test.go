// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/dsx-ai-factory/nvke/controller/internal/keys"
)

const (
	testKubeletClientClusterName = "tenant-a"
	testKubeletClientNamespace   = "nke-system"
	testKubeletClientIdentity    = "spiffe://example.org/bootstrap/worker-a"
	testKubeletClientIdentityB   = "spiffe://example.org/bootstrap/worker-b"
	testKubeletClientNodeName    = "worker-a"
)

func TestUnit_ValidateKubeletClientCSR(t *testing.T) {
	t.Parallel()

	validRequest := kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup}, nil, nil, nil, nil)

	tests := map[string]struct {
		usages     []certificatesv1.KeyUsage
		request    []byte
		wantReason string
	}{
		"digital signature and client auth": {
			usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageClientAuth,
				certificatesv1.UsageDigitalSignature,
			},
			request: validRequest,
		},
		"duplicate usages still match the signer set": {
			usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageClientAuth,
			},
			request: validRequest,
		},
		"server auth": {
			usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageKeyEncipherment,
				certificatesv1.UsageServerAuth,
			},
			request:    validRequest,
			wantReason: "spec.usages must be digital signature and client auth, with key encipherment optional",
		},
		"extra usage": {
			usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageKeyEncipherment,
				certificatesv1.UsageClientAuth,
				certificatesv1.UsageCertSign,
			},
			request:    validRequest,
			wantReason: "spec.usages must be digital signature and client auth, with key encipherment optional",
		},
		"malformed request": {
			request:    []byte("not pem"),
			wantReason: "spec.request is not a single PEM-encoded certificate request",
		},
		"trailing data": {
			request:    append(append([]byte{}, validRequest...), []byte("trailing")...),
			wantReason: "spec.request is not a single PEM-encoded certificate request",
		},
		"invalid signature": {
			request:    corruptCertificateRequest(t, validRequest),
			wantReason: "certificate request signature is invalid",
		},
		"wrong organization": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a", []string{"system:masters"}, nil, nil, nil, nil),
			wantReason: "subject organization is not system:nodes",
		},
		"extra organization": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup, "system:masters"}, nil, nil, nil, nil),
			wantReason: "subject organization is not system:nodes",
		},
		"common name prefix is not enough": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a-extra", []string{kubeletNodeGroup}, nil, nil, nil, nil),
			wantReason: "subject common name is not system:node:worker-a",
		},
		"dns san": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup}, []string{"worker-a"}, nil, nil, nil),
			wantReason: "DNS subjectAltNames are not allowed",
		},
		"ip san": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup}, nil, []net.IP{net.ParseIP("192.0.2.10")}, nil, nil),
			wantReason: "IP subjectAltNames are not allowed",
		},
		"email san": {
			request:    kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup}, nil, nil, []string{"kubelet@example.org"}, nil),
			wantReason: "Email subjectAltNames are not allowed",
		},
		"uri san": {
			request: kubeletClientCSRRequest(t, "system:node:worker-a", []string{kubeletNodeGroup}, nil, nil, nil, []*url.URL{
				{Scheme: "spiffe", Host: "example.org", Path: "/node/worker-a"},
			}),
			wantReason: "URI subjectAltNames are not allowed",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			csr := &certificatesv1.CertificateSigningRequest{
				Spec: certificatesv1.CertificateSigningRequestSpec{
					Request: test.request,
					Usages:  test.usages,
				},
			}
			if len(csr.Spec.Usages) == 0 {
				csr.Spec.Usages = defaultKubeletClientUsages()
			}
			require.Equal(t, test.wantReason, validateKubeletClientCSR(csr, "worker-a"))
		})
	}
}

func TestUnit_KubeletClientCSRControllerApprovesBootstrapRequestor(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	csr.Spec.Groups = []string{"oidc:bootstrap"}
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, true)
	approved := &certificatesv1.CertificateSigningRequest{}
	require.NoError(t, tenantClient.Get(t.Context(), client.ObjectKey{Name: csr.Name}, approved))
	require.Equal(t, KubeletClientCSRApprovalReason, approved.Status.Conditions[0].Reason)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRWaitingForSigningReason)
}

func TestUnit_KubeletClientCSRControllerApprovesWithoutKeyEncipherment(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	csr.Spec.Usages = []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageClientAuth}
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	_, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	requireCSRApproved(t, tenantClient, csr.Name, true)
}

func TestUnit_KubeletClientCSRControllerUsesExpectedNodeName(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	machine.Annotations[keys.MachineExpectedNodeName.String()] = "host-1"
	csr := newKubeletClientCSR(t, "host-1-client", testKubeletClientIdentity, "host-1")
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	_, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	requireCSRApproved(t, tenantClient, csr.Name, true)
}

func TestUnit_KubeletClientCSRControllerRejectsMismatchedCommonName(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, "worker-a-extra")
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRInvalidReason)
}

func TestUnit_KubeletClientCSRControllerIgnoresOtherRequestorsAndSigners(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	otherUser := newKubeletClientCSR(t, "other-user", "spiffe://example.org/bootstrap/other", testKubeletClientNodeName)
	renewal := newKubeletClientCSR(t, "renewal", kubeletNodeNamePrefix+testKubeletClientNodeName, testKubeletClientNodeName)
	serving := newKubeletClientCSR(t, "serving", testKubeletClientIdentity, testKubeletClientNodeName)
	serving.Spec.SignerName = certificatesv1.KubeletServingSignerName
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, otherUser, renewal, serving)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	for _, csr := range []*certificatesv1.CertificateSigningRequest{otherUser, renewal, serving} {
		requireCSRApproved(t, tenantClient, csr.Name, false)
	}
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRWaitingReason)
}

func TestUnit_KubeletClientCSRControllerApprovesOnlyTheValidRequest(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	valid := newKubeletClientCSR(t, "valid", testKubeletClientIdentity, testKubeletClientNodeName)
	invalid := newKubeletClientCSR(t, "invalid", testKubeletClientIdentity, "worker-a-extra")
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, valid, invalid)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, valid.Name, true)
	requireCSRApproved(t, tenantClient, invalid.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRWaitingForSigningReason)
}

func TestUnit_KubeletClientCSRControllerReportsIssuedCertificate(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	csr.Status.Certificate = []byte("issued")
	csr.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{
		Type:   certificatesv1.CertificateApproved,
		Status: corev1.ConditionTrue,
		Reason: KubeletClientCSRApprovalReason,
	}}
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, true)
	approved := &certificatesv1.CertificateSigningRequest{}
	require.NoError(t, tenantClient.Get(t.Context(), client.ObjectKey{Name: csr.Name}, approved))
	require.Len(t, approved.Status.Conditions, 1)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionTrue, KubeletClientCertificateIssuedReason)
}

func TestUnit_KubeletClientCSRControllerDoesNotApproveAfterJoin(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: testKubeletClientNodeName}
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionTrue, KubeletClientCertificateIssuedReason)
}

func TestUnit_KubeletClientCSRControllerObservesIssuedCertificateAfterIdentityCleanup(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", "")
	machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: testKubeletClientNodeName}
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	csr.Status.Certificate = []byte("issued")
	csr.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{
		Type:   certificatesv1.CertificateApproved,
		Status: corev1.ConditionTrue,
		Reason: KubeletClientCSRApprovalReason,
	}}
	controller, _ := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionTrue, KubeletClientCertificateIssuedReason)
}

func TestUnit_KubeletClientCSRControllerWaitsForIdentity(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", "")
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRWaitingForBootstrapIdentityReason)
}

func TestUnit_KubeletClientCSRControllerRejectsDuplicateIdentity(t *testing.T) {
	t.Parallel()

	first := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	second := newMachineForKubeletClientCSRTest("worker-b", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), first, second, csr)

	for _, machine := range []*clusterv1.Machine{first, second} {
		result, err := controller.Reconcile(t.Context(), machineRequest(machine))
		require.NoError(t, err)
		require.Equal(t, ctrl.Result{}, result)
	}
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, first, metav1.ConditionFalse, KubeletClientCSRInvalidReason)
	assertKubeletClientCSRCondition(t, controller.k8sClient, second, metav1.ConditionFalse, KubeletClientCSRInvalidReason)
}

func TestUnit_KubeletClientCSRControllerRejectsDuplicateNodeName(t *testing.T) {
	t.Parallel()

	pending := newMachineForKubeletClientCSRTest("worker-b", testKubeletClientIdentity)
	pending.Annotations[keys.MachineExpectedNodeName.String()] = testKubeletClientNodeName
	joined := newMachineForKubeletClientCSRTest("worker-joined", testKubeletClientIdentityB)
	joined.Status.NodeRef = clusterv1.MachineNodeReference{Name: testKubeletClientNodeName}
	csr := newKubeletClientCSR(t, "worker-b-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), pending, joined, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(pending))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, pending, metav1.ConditionFalse, KubeletClientCSRInvalidReason)
}

func TestUnit_KubeletClientCSRControllerReportsDeniedCSR(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	csr.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{
		Type:   certificatesv1.CertificateDenied,
		Status: corev1.ConditionTrue,
		Reason: "Denied",
	}}
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRInvalidReason)
}

func TestUnit_KubeletClientCSRControllerRetriesApprovalFailure(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)
	controller.clusterCache = staticKubeletClientClusterCache{client: interceptor.NewClient(tenantClient.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return errors.New("approval rejected")
		},
	})}

	_, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.Error(t, err)
	assertKubeletClientCSRCondition(t, controller.k8sClient, machine, metav1.ConditionFalse, KubeletClientCSRFailedReason)
}

func TestUnit_KubeletClientCSRControllerIgnoresUnselectedMachines(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	delete(machine.Labels, keys.ApproveKubeletClientCSR.String())
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertNoKubeletClientCSRCondition(t, controller.k8sClient, machine)
}

func TestUnit_KubeletClientCSRControllerReturnsMissingCluster(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, nil, machine, csr)

	_, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.Error(t, err)
	requireCSRApproved(t, tenantClient, csr.Name, false)
}

func TestUnit_KubeletClientCSRControllerReturnsClusterReadErrors(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, _ := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)
	controller.k8sClient = interceptor.NewClient(controller.k8sClient.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*clusterv1.Cluster); ok {
				return errors.New("cluster down")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	_, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.Error(t, err)
}

func TestUnit_KubeletClientCSRControllerWaitsForClusterConnection(t *testing.T) {
	t.Parallel()

	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	controller, tenantClient := newKubeletClientCSRControllerForTest(t, newClusterForKubeletClientCSRTest(), machine, csr)
	controller.clusterCache = staticKubeletClientClusterCache{err: clustercache.ErrClusterNotConnected}

	result, err := controller.Reconcile(t.Context(), machineRequest(machine))
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	requireCSRApproved(t, tenantClient, csr.Name, false)
	assertNoKubeletClientCSRCondition(t, controller.k8sClient, machine)
}

func TestUnit_KubeletClientCSRPredicate(t *testing.T) {
	t.Parallel()

	csrPredicate := kubeletClientCSRPredicate()
	csr := newKubeletClientCSR(t, "worker-a-client", testKubeletClientIdentity, testKubeletClientNodeName)
	require.True(t, csrPredicate.Create(event.CreateEvent{Object: csr}))
	require.True(t, csrPredicate.Update(event.UpdateEvent{ObjectNew: csr}))
	require.False(t, csrPredicate.Delete(event.DeleteEvent{Object: csr}))

	renewal := csr.DeepCopy()
	renewal.Spec.Username = kubeletNodeNamePrefix + testKubeletClientNodeName
	require.False(t, csrPredicate.Create(event.CreateEvent{Object: renewal}))

	serving := csr.DeepCopy()
	serving.Spec.SignerName = certificatesv1.KubeletServingSignerName
	require.False(t, csrPredicate.Create(event.CreateEvent{Object: serving}))
}

func TestUnit_KubeletClientCSRMachinePredicate(t *testing.T) {
	t.Parallel()

	machinePredicate := kubeletClientCSRMachinePredicate()
	machine := newMachineForKubeletClientCSRTest("worker-a", testKubeletClientIdentity)
	require.True(t, machinePredicate.Create(event.CreateEvent{Object: machine}))

	updated := machine.DeepCopy()
	updated.Annotations[keys.MachineExpectedNodeName.String()] = "host-1"
	require.True(t, machinePredicate.Update(event.UpdateEvent{ObjectOld: machine, ObjectNew: updated}))

	conditionOnly := machine.DeepCopy()
	conditionOnly.Status.Conditions = []metav1.Condition{{
		Type:   KubeletClientCertificateIssuedCondition,
		Status: metav1.ConditionFalse,
		Reason: KubeletClientCSRWaitingReason,
	}}
	require.False(t, machinePredicate.Update(event.UpdateEvent{ObjectOld: machine, ObjectNew: conditionOnly}))
}

type staticKubeletClientClusterCache struct {
	client client.Client
	err    error
}

var _ clustercache.ClusterCache = staticKubeletClientClusterCache{}

func (s staticKubeletClientClusterCache) GetClient(context.Context, client.ObjectKey) (client.Client, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.client, nil
}

func (s staticKubeletClientClusterCache) GetReader(ctx context.Context, cluster client.ObjectKey) (client.Reader, error) {
	return s.GetClient(ctx, cluster)
}

func (s staticKubeletClientClusterCache) GetUncachedClient(ctx context.Context, cluster client.ObjectKey) (client.Client, error) {
	return s.GetClient(ctx, cluster)
}

func (s staticKubeletClientClusterCache) GetRESTConfig(context.Context, client.ObjectKey) (*rest.Config, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &rest.Config{}, nil
}

func (s staticKubeletClientClusterCache) GetHealthCheckingState(context.Context, client.ObjectKey) clustercache.HealthCheckingState {
	return clustercache.HealthCheckingState{}
}

func (s staticKubeletClientClusterCache) Watch(context.Context, client.ObjectKey, clustercache.Watcher) error {
	return s.err
}

func (s staticKubeletClientClusterCache) GetClusterSource(string, func(context.Context, client.Object) []ctrl.Request, ...clustercache.GetClusterSourceOption) source.Source {
	return nil
}

func newClusterForKubeletClientCSRTest() *clusterv1.Cluster {
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testKubeletClientClusterName,
			Namespace: testKubeletClientNamespace,
		},
	}
}

func newKubeletClientCSRControllerForTest(t *testing.T, cluster *clusterv1.Cluster, objects ...client.Object) (*KubeletClientCSRController, client.Client) {
	t.Helper()

	var machines []client.Object
	var csrs []client.Object
	for _, object := range objects {
		switch object.(type) {
		case *clusterv1.Machine:
			machines = append(machines, object)
		case *certificatesv1.CertificateSigningRequest:
			csrs = append(csrs, object)
		default:
			t.Fatalf("unexpected test object %T", object)
		}
	}
	if cluster != nil {
		machines = append(machines, cluster)
	}

	managementScheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(managementScheme))
	managementClient := fake.NewClientBuilder().
		WithScheme(managementScheme).
		WithStatusSubresource(&clusterv1.Machine{}).
		WithIndex(&clusterv1.Machine{}, machineBootstrapIdentityField, extractMachineBootstrapIdentity).
		WithIndex(&clusterv1.Machine{}, machineKubeletNodeNameField, extractMachineKubeletNodeName).
		WithObjects(machines...).
		Build()

	tenantScheme := runtime.NewScheme()
	require.NoError(t, certificatesv1.AddToScheme(tenantScheme))
	tenantClient := fake.NewClientBuilder().
		WithScheme(tenantScheme).
		WithIndex(&certificatesv1.CertificateSigningRequest{}, kubeletClientCSRUsernameField, extractKubeletClientCSRUsername).
		WithObjects(csrs...).
		Build()

	return NewKubeletClientCSRController(
		managementClient,
		staticKubeletClientClusterCache{client: tenantClient},
		slog.New(slog.DiscardHandler),
	), tenantClient
}

func newMachineForKubeletClientCSRTest(name, identity string) *clusterv1.Machine {
	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testKubeletClientNamespace,
			Labels: map[string]string{
				clusterv1.ClusterNameLabel:            testKubeletClientClusterName,
				keys.ApproveKubeletClientCSR.String(): "true",
			},
			Annotations: map[string]string{},
		},
		Spec: clusterv1.MachineSpec{ClusterName: testKubeletClientClusterName},
	}
	if identity != "" {
		machine.Annotations[keys.MachineBootstrapIdentity.String()] = identity
	}
	return machine
}

func newKubeletClientCSR(t *testing.T, name, username, nodeName string) *certificatesv1.CertificateSigningRequest {
	t.Helper()
	return &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    kubeletClientCSRRequest(t, kubeletNodeNamePrefix+nodeName, []string{kubeletNodeGroup}, nil, nil, nil, nil),
			SignerName: certificatesv1.KubeAPIServerClientKubeletSignerName,
			Usages:     defaultKubeletClientUsages(),
			Username:   username,
			Groups:     []string{"oidc:bootstrap"},
		},
	}
}

func defaultKubeletClientUsages() []certificatesv1.KeyUsage {
	return []certificatesv1.KeyUsage{
		certificatesv1.UsageDigitalSignature,
		certificatesv1.UsageKeyEncipherment,
		certificatesv1.UsageClientAuth,
	}
}

func kubeletClientCSRRequest(
	t *testing.T,
	commonName string,
	organizations []string,
	dnsNames []string,
	ips []net.IP,
	emails []string,
	uris []*url.URL,
) []byte {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:        pkix.Name{CommonName: commonName, Organization: organizations},
		DNSNames:       dnsNames,
		IPAddresses:    ips,
		EmailAddresses: emails,
		URIs:           uris,
	}, privateKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: certificateRequestPEMType, Bytes: request})
}

func corruptCertificateRequest(t *testing.T, pemBytes []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	require.NotNil(t, block)
	der := append([]byte(nil), block.Bytes...)
	der[len(der)-1] ^= 0xff
	return pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: der})
}

func machineRequest(machine *clusterv1.Machine) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(machine)}
}

func requireCSRApproved(t *testing.T, tenantClient client.Client, name string, approved bool) {
	t.Helper()
	got := &certificatesv1.CertificateSigningRequest{}
	require.NoError(t, tenantClient.Get(t.Context(), client.ObjectKey{Name: name}, got))
	require.Equal(t, approved, clientCSRApproved(got))
}

func assertKubeletClientCSRCondition(
	t *testing.T,
	k8sClient client.Client,
	machine *clusterv1.Machine,
	status metav1.ConditionStatus,
	reason string,
) {
	t.Helper()
	got := &clusterv1.Machine{}
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(machine), got))
	condition := meta.FindStatusCondition(got.Status.Conditions, KubeletClientCertificateIssuedCondition)
	require.NotNil(t, condition)
	require.Equal(t, status, condition.Status)
	require.Equal(t, reason, condition.Reason)
}

func assertNoKubeletClientCSRCondition(t *testing.T, k8sClient client.Client, machine *clusterv1.Machine) {
	t.Helper()
	got := &clusterv1.Machine{}
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(machine), got))
	require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, KubeletClientCertificateIssuedCondition))
}
