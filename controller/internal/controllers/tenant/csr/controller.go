// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package csr

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"

	"github.com/google/uuid"
	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

const (
	decisionApprove = "approve"
	decisionSkip    = "skip"

	nodeUsernamePrefix = "system:node:"
	nodeGroup          = "system:nodes"

	nodeBootstrapperGroup = "nke:node-bootstrapper"
	joinSPIFFEPath        = "/kubeadm/join/"

	approvalReason = "NKEValidatedNodeIdentity"

	unsupportedSANReason = "subject alternative names contain an unsupported identity"
)

type database interface {
	GetCluster(ctx context.Context, id uuid.UUID) (db.Cluster, error)
	ListNodes(ctx context.Context, filters db.NodeFilters) ([]db.Node, error)
}

// Reconciler approves pending kubelet-serving CSRs that match the node identity
// recorded in CWE.
type Reconciler struct {
	clusterID  uuid.UUID
	database   database
	mgmtClient client.Client
	client     client.Client
	logger     *slog.Logger
}

type csrDecision struct {
	CSR      *certificatesv1.CertificateSigningRequest
	NodeName string
	Action   string
	Reason   string
}

// NewRegistrar returns a tenant-manager registrar. Each tenant manager
// gets its own CSR informer and work queue; clusterID is therefore implicit in
// every queued CSR name and cannot be confused with another tenant cluster.
func NewRegistrar(database database, mgmtClient client.Client, logger *slog.Logger) tenant.Registrar {
	if logger == nil {
		logger = slog.Default()
	}
	return func(clusterID uuid.UUID, manager ctrl.Manager) error {
		controllerName, controllerOptions := tenantCSRControllerRegistration(clusterID)
		reconciler := &Reconciler{
			clusterID:  clusterID,
			database:   database,
			mgmtClient: mgmtClient,
			client:     manager.GetClient(),
			logger:     logger,
		}
		return ctrl.NewControllerManagedBy(manager).
			Named(controllerName).
			WithOptions(controllerOptions).
			For(
				&certificatesv1.CertificateSigningRequest{},
				builder.WithPredicates(kubeletCSRPredicate()),
			).
			Complete(reconciler)
	}
}

func tenantCSRControllerRegistration(clusterID uuid.UUID) (string, controller.Options) {
	// controller-runtime retains controller names for the lifetime of the
	// process, even after their manager stops. ManagerSet serializes manager
	// generations for a cluster, so the same name is never active concurrently;
	// skipping validation permits kubeconfig rotation and runtime recovery while
	// the cluster-scoped name keeps metrics and logs attributable.
	return "tenant-csr-" + clusterID.String(), controller.Options{
		SkipNameValidation: ptr.To(true),
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	csr := &certificatesv1.CertificateSigningRequest{}
	if err := r.client.Get(ctx, request.NamespacedName, csr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting tenant CSR %s for cluster %s: %w", request.Name, r.clusterID, err)
	}
	if !isPendingKubeletServingCSR(csr) && !isPendingKubeletClientCSR(csr) {
		return ctrl.Result{}, nil
	}
	cluster, err := r.database.GetCluster(ctx, r.clusterID)
	if errors.Is(err, db.ErrNotFound) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting CWE cluster %s: %w", r.clusterID, err)
	}
	if cluster.Conditions.Deleting.Status == db.ConditionStatusTrue {
		return ctrl.Result{}, nil
	}

	nodes, err := r.database.ListNodes(ctx, db.NodeFilters{ClusterID: &r.clusterID})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing CWE nodes for cluster %s: %w", r.clusterID, err)
	}

	machines := &clusterv1.MachineList{}
	if err := r.mgmtClient.List(ctx, machines,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing NKE Machines for cluster %s: %w", cluster.Name, err)
	}

	var decision csrDecision
	if isPendingKubeletClientCSR(csr) {
		decision = planClientDecision(csr, machines)
	} else {
		decision = planServingDecision(csr, nodes, machines)
	}

	if decision.Action == decisionSkip {
		r.logDecision(decision)
		return ctrl.Result{}, nil
	}

	if err := r.applyDecision(ctx, decision); err != nil {
		return ctrl.Result{}, err
	}

	r.logDecision(decision)
	return ctrl.Result{}, nil
}

func (r *Reconciler) applyDecision(ctx context.Context, decision csrDecision) error {
	now := metav1.Now()
	decision.CSR.Status.Conditions = append(decision.CSR.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:               certificatesv1.CertificateApproved,
		Status:             corev1.ConditionTrue,
		Reason:             approvalReason,
		Message:            "kubelet CSR matches the node identity recorded by NKE",
		LastUpdateTime:     now,
		LastTransitionTime: now,
	})
	if err := r.client.SubResource("approval").Update(ctx, decision.CSR); err != nil {
		return fmt.Errorf(
			"applying %s decision to tenant CSR %s for cluster %s: %w",
			decision.Action,
			decision.CSR.Name,
			r.clusterID,
			err,
		)
	}
	return nil
}

func (r *Reconciler) logDecision(decision csrDecision) {
	log := r.logger.Info
	if decision.Action == decisionSkip {
		log = r.logger.Debug
	}
	log(
		"Reconciled tenant kubelet CSR",
		"clusterID", r.clusterID,
		"csrName", decision.CSR.Name,
		"nodeName", decision.NodeName,
		"decision", decision.Action,
		"reason", decision.Reason,
	)
}

func kubeletCSRPredicate() predicate.Predicate {
	matches := func(object client.Object) bool {
		csr, ok := object.(*certificatesv1.CertificateSigningRequest)
		return ok && (isPendingKubeletServingCSR(csr) || isPendingKubeletClientCSR(csr))
	}

	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return matches(e.Object) },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		UpdateFunc:  func(e event.UpdateEvent) bool { return matches(e.ObjectOld) || matches(e.ObjectNew) },
		GenericFunc: func(e event.GenericEvent) bool { return matches(e.Object) },
	}
}

func isPendingKubeletClientCSR(csr *certificatesv1.CertificateSigningRequest) bool {
	if csr.Spec.SignerName != certificatesv1.KubeAPIServerClientKubeletSignerName {
		return false
	}
	for _, condition := range csr.Status.Conditions {
		switch condition.Type {
		case certificatesv1.CertificateApproved, certificatesv1.CertificateDenied, certificatesv1.CertificateFailed:
			return false
		}
	}
	return true
}

func isPendingKubeletServingCSR(csr *certificatesv1.CertificateSigningRequest) bool {
	if csr.Spec.SignerName != certificatesv1.KubeletServingSignerName {
		return false
	}
	for _, condition := range csr.Status.Conditions {
		switch condition.Type {
		case certificatesv1.CertificateApproved, certificatesv1.CertificateDenied, certificatesv1.CertificateFailed:
			return false
		}
	}
	return true
}

func planServingDecision(csr *certificatesv1.CertificateSigningRequest, nodes []db.Node, machines *clusterv1.MachineList) csrDecision {
	nodeName, ok := strings.CutPrefix(csr.Spec.Username, nodeUsernamePrefix)
	if !ok || nodeName == "" {
		return newDecision(csr, "", decisionSkip, "requestor is not a system:node identity")
	}

	expectedIP, known := knownNodeIP(nodes, machines, nodeName)
	if !known {
		return newDecision(csr, nodeName, decisionSkip, "node is not present in CWE desired state")
	}
	if reason := validateServingCSR(csr, nodeName, expectedIP); reason != "" {
		return newDecision(csr, nodeName, decisionSkip, reason)
	}
	return newDecision(csr, nodeName, decisionApprove, "matches CWE node identity")
}

// planClientDecision handles the kubelet client CSR created during kubeadm
// join. Approving that CSR gives the host a client certificate for
// system:node:<node name>, which is what lets it authenticate as that
// Kubernetes Node. The pending CSR is not node credentials until an approver signs
// it.
//
// NKE approves the request only when it:
// - uses a /kubeadm/join/<machine-uid> SPIFFE identity;
// - matches exactly one CAPI Machine in the tenant cluster;
// - targets a worker Machine that is not deleting and has not already joined;
// - targets a Machine with an attested SPIRE agent identity; and
// - requests the exact system:node:<machine name> kubelet client identity.
func planClientDecision(csr *certificatesv1.CertificateSigningRequest, machines *clusterv1.MachineList) csrDecision {
	machineUID, ok := joinMachineUID(csr.Spec.Username)
	if !ok {
		return newDecision(csr, "", decisionSkip, "requestor is not a kubeadm join SPIFFE identity")
	}
	if !slices.Contains(csr.Spec.Groups, nodeBootstrapperGroup) {
		return newDecision(csr, "", decisionSkip, "spec.groups does not include nke:node-bootstrapper")
	}

	var matched *clusterv1.Machine
	for _, machine := range machines.Items {
		if string(machine.UID) != machineUID {
			continue
		}
		if matched != nil {
			return newDecision(csr, machine.Name, decisionSkip, "join identity matched multiple Machines")
		}
		matched = &machine
	}
	if matched == nil {
		return newDecision(csr, "", decisionSkip, "join identity does not match a CAPI Machine")
	}
	if !matched.DeletionTimestamp.IsZero() {
		return newDecision(csr, matched.Name, decisionSkip, "Machine is deleting")
	}
	if _, ok := matched.Labels[clusterv1.MachineControlPlaneLabel]; ok {
		return newDecision(csr, matched.Name, decisionSkip, "Machine is labeled as a control-plane Machine")
	}
	if matched.Status.NodeRef.Name != "" {
		return newDecision(csr, matched.Name, decisionSkip, "Machine already has nodeRef")
	}
	if matched.GetAnnotations()[constants.MachineAnnotationKeySpireAgentSpiffeIDPath] == "" {
		return newDecision(csr, matched.Name, decisionSkip, "Machine has no attested SPIRE agent identity path")
	}
	if reason := validateClientCSR(csr, matched.Name); reason != "" {
		return newDecision(csr, matched.Name, decisionSkip, reason)
	}
	return newDecision(csr, matched.Name, decisionApprove, "matches CAPI Machine join identity")
}

func joinMachineUID(username string) (string, bool) {
	i := strings.LastIndex(username, joinSPIFFEPath)
	if i < 0 {
		return "", false
	}
	machineUID := username[i+len(joinSPIFFEPath):]
	return machineUID, machineUID != ""
}

func newDecision(csr *certificatesv1.CertificateSigningRequest, nodeName, action, reason string) csrDecision {
	return csrDecision{
		CSR:      csr.DeepCopy(),
		NodeName: nodeName,
		Action:   action,
		Reason:   reason,
	}
}

func knownNodeIP(nodes []db.Node, machines *clusterv1.MachineList, nodeName string) (string, bool) {
	// Try to find the node in the list of database nodes first for legacy or control-plane Machines.
	for _, node := range nodes {
		if node.Name == nodeName {
			return node.IP, true
		}
	}

	// Else try to find the node in the list of CAPI Machines.
	for _, machine := range machines.Items {
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}
		if machine.Status.NodeRef.Name == nodeName {
			for _, address := range machine.Status.Addresses {
				if address.Type == clusterv1.MachineInternalIP && address.Address != "" {
					return address.Address, true
				}
			}
		}
	}

	// Else not a known node/IP
	return "", false
}

// validateServingCSR returns an empty string when a CSR matches the identity
// recorded in CWE. A non-empty return value explains why it must not be
// approved.
func validateServingCSR(csr *certificatesv1.CertificateSigningRequest, nodeName, expectedIP string) string {
	// spec.groups describes the authenticated requester that submitted the CSR.
	if !slices.Contains(csr.Spec.Groups, nodeGroup) {
		return "spec.groups does not include system:nodes"
	}
	if reason := validateKubeletCSRUsages(csr, certificatesv1.UsageServerAuth); reason != "" {
		return reason
	}
	parsed, reason := parseCertificateRequest(csr)
	if reason != "" {
		return reason
	}
	if reason := validateKubeletSubject(parsed, nodeName); reason != "" {
		return reason
	}
	if reason := validateSubjectAlternativeNames(parsed, nodeName, expectedIP); reason != "" {
		return reason
	}
	return ""
}

func validateClientCSR(csr *certificatesv1.CertificateSigningRequest, nodeName string) string {
	if reason := validateKubeletCSRUsages(csr, certificatesv1.UsageClientAuth); reason != "" {
		return reason
	}
	parsed, reason := parseCertificateRequest(csr)
	if reason != "" {
		return reason
	}
	if reason := validateKubeletSubject(parsed, nodeName); reason != "" {
		return reason
	}
	if reason := validateNoSubjectAlternativeNames(parsed); reason != "" {
		return reason
	}
	return ""
}

func validateKubeletCSRUsages(csr *certificatesv1.CertificateSigningRequest, required certificatesv1.KeyUsage) string {
	hasRequired := false
	for _, usage := range csr.Spec.Usages {
		if usage == required {
			hasRequired = true
			continue
		}
		if usage == certificatesv1.UsageDigitalSignature || usage == certificatesv1.UsageKeyEncipherment {
			// These usages are allowed for kubelet client or serving CSRs.
			continue
		}
		return fmt.Sprintf("spec.usages contains disallowed usage %q", usage)
	}

	if !hasRequired {
		return fmt.Sprintf("spec.usages does not include %s", required)
	}
	return ""
}

func parseCertificateRequest(csr *certificatesv1.CertificateSigningRequest) (*x509.CertificateRequest, string) {
	pemBlock, trailing := pem.Decode(csr.Spec.Request)
	if pemBlock == nil || strings.TrimSpace(string(trailing)) != "" {
		return nil, "spec.request is not a single PEM-encoded certificate request"
	}
	parsed, err := x509.ParseCertificateRequest(pemBlock.Bytes)
	if err != nil {
		return nil, "spec.request cannot be parsed as a certificate request"
	}
	if err := parsed.CheckSignature(); err != nil {
		return nil, "certificate request signature is invalid"
	}
	return parsed, ""
}

func validateKubeletSubject(csr *x509.CertificateRequest, nodeName string) string {
	// Subject.Organization describes the identity requested in the certificate.
	if !slices.Contains(csr.Subject.Organization, nodeGroup) {
		return "subject organization is not system:nodes"
	}
	expectedCommonName := nodeUsernamePrefix + nodeName
	if csr.Subject.CommonName != expectedCommonName {
		return fmt.Sprintf("subject common name is not %s", expectedCommonName)
	}
	return ""
}

func validateNoSubjectAlternativeNames(csr *x509.CertificateRequest) string {
	if len(csr.DNSNames) > 0 || len(csr.IPAddresses) > 0 || len(csr.EmailAddresses) > 0 || len(csr.URIs) > 0 {
		return "subject alternative names are not allowed for kubelet client CSRs"
	}
	return ""
}

func validateSubjectAlternativeNames(csr *x509.CertificateRequest, nodeName, expectedIP string) string {
	if len(csr.EmailAddresses) > 0 || len(csr.URIs) > 0 {
		return unsupportedSANReason
	}
	if len(csr.DNSNames) == 0 && len(csr.IPAddresses) == 0 {
		return "certificate request has no subject alternative names"
	}

	for _, dnsName := range csr.DNSNames {
		if dnsName != nodeName {
			return fmt.Sprintf("DNS SAN %q does not match CWE node name", dnsName)
		}
	}

	if len(csr.IPAddresses) == 0 {
		return ""
	}
	expected := net.ParseIP(expectedIP)
	for _, ipAddress := range csr.IPAddresses {
		if expected == nil || !ipAddress.Equal(expected) {
			return fmt.Sprintf("IP SAN %q does not match CWE node IP", ipAddress)
		}
	}
	return ""
}
