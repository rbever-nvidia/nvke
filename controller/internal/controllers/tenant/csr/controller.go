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
	"time"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"
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

	"github.com/dsx-ai-factory/nvke/controller/internal/constants"
	"github.com/dsx-ai-factory/nvke/controller/internal/controllers/tenant"
	"github.com/dsx-ai-factory/nvke/controller/internal/db"
)

const (
	decisionApprove = "approve"
	decisionRequeue = "requeue"
	decisionSkip    = "skip"

	nodeUsernamePrefix = "system:node:"
	nodeGroup          = "system:nodes"

	nodeBootstrapperGroup = "nvke:node-bootstrapper"
	dsxIMDSBMPath         = "/bm/"
	spireJoinPath         = "/kubeadm/join/"

	approvalReason = "NVKEValidatedNodeIdentity"

	unsupportedSANReason = "subject alternative names contain an unsupported identity"

	unknownNodeReason = "node is not present in the controller's desired state"

	// The kubelet requests a serving certificate as it joins, before Cluster API
	// writes the Machine nodeRef that records the accepted node.
	unknownNodeRetryInterval = 10 * time.Second
)

type database interface {
	GetCluster(ctx context.Context, id uuid.UUID) (db.Cluster, error)
	ListNodes(ctx context.Context, filters db.NodeFilters) ([]db.Node, error)
}

// Reconciler approves pending kubelet CSRs that match the node identity recorded
// in the controller. Serving CSRs are checked against the controller database;
// client CSRs in this POC are checked against the bootstrap identity source and
// the corresponding Machine in the management cluster.
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
		return ctrl.Result{}, fmt.Errorf("getting controller cluster %s: %w", r.clusterID, err)
	}
	if cluster.Conditions.Deleting.Status == db.ConditionStatusTrue {
		return ctrl.Result{}, nil
	}

	nodes, err := r.database.ListNodes(ctx, db.NodeFilters{ClusterID: &r.clusterID})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing controller nodes for cluster %s: %w", r.clusterID, err)
	}

	machines := &clusterv1.MachineList{}
	if err := r.mgmtClient.List(ctx, machines,
		client.InNamespace(constants.NamespaceNVKESystem),
		client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing NVKE Machines for cluster %s: %w", cluster.Name, err)
	}
	nicoMachines := &nicov1.NicoMachineList{}
	if err := r.mgmtClient.List(ctx, nicoMachines, client.InNamespace(constants.NamespaceNVKESystem)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing NVKE NicoMachines for cluster %s: %w", cluster.Name, err)
	}

	var decision csrDecision
	if isPendingKubeletClientCSR(csr) {
		decision = planClientDecision(csr, machines, nicoMachines)
	} else {
		decision = planServingDecision(csr, nodes, machines)
	}
	switch decision.Action {
	case decisionSkip:
		r.logDecision(decision)
		return ctrl.Result{}, nil
	case decisionRequeue:
		r.logDecision(decision)
		return ctrl.Result{RequeueAfter: unknownNodeRetryInterval}, nil
	case decisionApprove:
		if err := r.applyDecision(ctx, decision); err != nil {
			return ctrl.Result{}, err
		}
		r.logDecision(decision)
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("unsupported CSR decision action %q", decision.Action)
	}
}

func (r *Reconciler) applyDecision(ctx context.Context, decision csrDecision) error {
	now := metav1.Now()
	decision.CSR.Status.Conditions = append(decision.CSR.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:               certificatesv1.CertificateApproved,
		Status:             corev1.ConditionTrue,
		Reason:             approvalReason,
		Message:            "kubelet CSR matches the node identity recorded by NVKE",
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
	if decision.Action == decisionSkip || decision.Action == decisionRequeue {
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
		return newDecision(csr, nodeName, decisionRequeue, unknownNodeReason)
	}
	if reason := validateServingCSR(csr, nodeName, expectedIP); reason != "" {
		return newDecision(csr, nodeName, decisionSkip, reason)
	}
	return newDecision(csr, nodeName, decisionApprove, "matches controller node identity")
}

func planClientDecision(csr *certificatesv1.CertificateSigningRequest, machines *clusterv1.MachineList, nicoMachines *nicov1.NicoMachineList) csrDecision {
	if !slices.Contains(csr.Spec.Groups, nodeBootstrapperGroup) {
		return newDecision(csr, "", decisionSkip, "spec.groups does not include nvke:node-bootstrapper")
	}

	matched, ok, reason := matchClientCSRMachine(csr.Spec.Username, machines, nicoMachines)
	if !ok {
		return newDecision(csr, "", decisionSkip, reason)
	}
	if !matched.DeletionTimestamp.IsZero() {
		return newDecision(csr, matched.Name, decisionSkip, "Machine is deleting")
	}
	if _, ok := matched.Labels[clusterv1.MachineControlPlaneLabel]; ok {
		return newDecision(csr, matched.Name, decisionSkip, "Machine is labeled as a control-plane Machine")
	}
	if matched.Status.NodeRef.IsDefined() {
		return newDecision(csr, matched.Name, decisionSkip, "Machine already has nodeRef")
	}
	if reason := validateClientCSR(csr, matched.Name); reason != "" {
		return newDecision(csr, matched.Name, decisionSkip, reason)
	}
	return newDecision(csr, matched.Name, decisionApprove, reason)
}

func matchClientCSRMachine(username string, machines *clusterv1.MachineList, nicoMachines *nicov1.NicoMachineList) (*clusterv1.Machine, bool, string) {
	if resourceID, ok := dsxIMDSResourceID(username); ok {
		return matchDSXIMDSMachine(machines, nicoMachines, resourceID)
	}
	if machineUID, ok := spireJoinMachineUID(username); ok {
		return matchSpireJoinMachine(machines, machineUID)
	}
	return nil, false, "requestor is not a supported NVKE bootstrap identity"
}

func matchDSXIMDSMachine(machines *clusterv1.MachineList, nicoMachines *nicov1.NicoMachineList, resourceID string) (*clusterv1.Machine, bool, string) {
	var matched *clusterv1.Machine
	for i := range machines.Items {
		machine := &machines.Items[i]
		if !machineMatchesDSXIMDSResource(machine, nicoMachines, resourceID) {
			continue
		}
		if matched != nil {
			return nil, false, "DSX IMDS identity matched multiple Machines"
		}
		matched = machine
	}
	if matched == nil {
		return nil, false, "DSX IMDS identity does not match a CAPI Machine"
	}
	return matched, true, "matches CAPI Machine DSX IMDS identity"
}

func matchSpireJoinMachine(machines *clusterv1.MachineList, machineUID string) (*clusterv1.Machine, bool, string) {
	var matched *clusterv1.Machine
	for i := range machines.Items {
		machine := &machines.Items[i]
		if string(machine.UID) != machineUID {
			continue
		}
		if matched != nil {
			return nil, false, "SPIRE join identity matched multiple Machines"
		}
		matched = machine
	}
	if matched == nil {
		return nil, false, "SPIRE join identity does not match a CAPI Machine"
	}
	return matched, true, "matches CAPI Machine SPIRE join identity"
}

func dsxIMDSResourceID(username string) (string, bool) {
	i := strings.LastIndex(username, dsxIMDSBMPath)
	if i < 0 {
		return "", false
	}
	resourceID := username[i+len(dsxIMDSBMPath):]
	return resourceID, resourceID != "" && !strings.Contains(resourceID, "/")
}

func spireJoinMachineUID(username string) (string, bool) {
	i := strings.LastIndex(username, spireJoinPath)
	if i < 0 {
		return "", false
	}
	machineUID := username[i+len(spireJoinPath):]
	return machineUID, machineUID != "" && !strings.Contains(machineUID, "/")
}

func machineMatchesDSXIMDSResource(machine *clusterv1.Machine, nicoMachines *nicov1.NicoMachineList, resourceID string) bool {
	for i := range nicoMachines.Items {
		nicoMachine := &nicoMachines.Items[i]
		if nicoMachine.Status.PrimaryDPUMachineID != resourceID {
			continue
		}
		for _, owner := range nicoMachine.OwnerReferences {
			if owner.Kind == "Machine" && owner.UID == machine.UID {
				return true
			}
		}
	}
	return false
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
	for _, node := range nodes {
		if node.Name == nodeName {
			return node.IP, true
		}
	}
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
	return "", false
}

// validateServingCSR returns an empty string when a CSR matches the identity
// recorded in the controller. A non-empty return value explains why it must not be
// approved.
func validateServingCSR(csr *certificatesv1.CertificateSigningRequest, nodeName, expectedIP string) string {
	if !slices.Contains(csr.Spec.Groups, nodeGroup) {
		return "spec.groups does not include system:nodes"
	}

	hasServerAuth := false
	for _, usage := range csr.Spec.Usages {
		if usage == certificatesv1.UsageServerAuth {
			hasServerAuth = true
		}
		if usage != certificatesv1.UsageServerAuth &&
			usage != certificatesv1.UsageDigitalSignature &&
			usage != certificatesv1.UsageKeyEncipherment {
			return fmt.Sprintf("spec.usages contains disallowed usage %q", usage)
		}
	}
	if !hasServerAuth {
		return "spec.usages does not include server auth"
	}

	pemBlock, trailing := pem.Decode(csr.Spec.Request)
	if pemBlock == nil || strings.TrimSpace(string(trailing)) != "" {
		return "spec.request is not a single PEM-encoded certificate request"
	}
	parsed, err := x509.ParseCertificateRequest(pemBlock.Bytes)
	if err != nil {
		return "spec.request cannot be parsed as a certificate request"
	}
	if err := parsed.CheckSignature(); err != nil {
		return "certificate request signature is invalid"
	}
	if !slices.Contains(parsed.Subject.Organization, nodeGroup) {
		return "subject organization is not system:nodes"
	}
	expectedCommonName := nodeUsernamePrefix + nodeName
	if parsed.Subject.CommonName != expectedCommonName {
		return fmt.Sprintf("subject common name is not %s", expectedCommonName)
	}
	if reason := validateSubjectAlternativeNames(parsed, nodeName, expectedIP); reason != "" {
		return reason
	}
	return ""
}

func validateClientCSR(csr *certificatesv1.CertificateSigningRequest, nodeName string) string {
	hasClientAuth := false
	for _, usage := range csr.Spec.Usages {
		if usage == certificatesv1.UsageClientAuth {
			hasClientAuth = true
			continue
		}
		if usage == certificatesv1.UsageDigitalSignature || usage == certificatesv1.UsageKeyEncipherment {
			continue
		}
		return fmt.Sprintf("spec.usages contains disallowed usage %q", usage)
	}
	if !hasClientAuth {
		return "spec.usages does not include client auth"
	}

	pemBlock, trailing := pem.Decode(csr.Spec.Request)
	if pemBlock == nil || strings.TrimSpace(string(trailing)) != "" {
		return "spec.request is not a single PEM-encoded certificate request"
	}
	parsed, err := x509.ParseCertificateRequest(pemBlock.Bytes)
	if err != nil {
		return "spec.request cannot be parsed as a certificate request"
	}
	if err := parsed.CheckSignature(); err != nil {
		return "certificate request signature is invalid"
	}
	if !slices.Contains(parsed.Subject.Organization, nodeGroup) {
		return "subject organization is not system:nodes"
	}
	expectedCommonName := nodeUsernamePrefix + nodeName
	if parsed.Subject.CommonName != expectedCommonName {
		return fmt.Sprintf("subject common name is not %s", expectedCommonName)
	}
	if len(parsed.DNSNames) > 0 || len(parsed.IPAddresses) > 0 || len(parsed.EmailAddresses) > 0 || len(parsed.URIs) > 0 {
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
			return fmt.Sprintf("DNS SAN %q does not match controller node name", dnsName)
		}
	}

	if len(csr.IPAddresses) == 0 {
		return ""
	}
	expected := net.ParseIP(expectedIP)
	for _, ipAddress := range csr.IPAddresses {
		if expected == nil || !ipAddress.Equal(expected) {
			return fmt.Sprintf("IP SAN %q does not match controller node IP", ipAddress)
		}
	}
	return ""
}
