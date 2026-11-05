// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	kubeletNodeNamePrefix     = "system:node:"
	kubeletNodeGroup          = "system:nodes"
	certificateRequestPEMType = "CERTIFICATE REQUEST"
)

type kubeletClientCSRDecisions struct {
	pending       []*certificatesv1.CertificateSigningRequest
	approved      []*certificatesv1.CertificateSigningRequest
	issued        []*certificatesv1.CertificateSigningRequest
	denied        []*certificatesv1.CertificateSigningRequest
	invalid       []*certificatesv1.CertificateSigningRequest
	invalidReason string
}

func decideKubeletClientCSRs(machine *clusterv1.Machine, csrs []certificatesv1.CertificateSigningRequest) kubeletClientCSRDecisions {
	nodeName := expectedKubeletNodeName(machine)
	var csrDecisions kubeletClientCSRDecisions
	var invalidAt time.Time
	for i := range csrs {
		csr := &csrs[i]
		if reason := validateKubeletClientCSR(csr, nodeName); reason != "" {
			csrDecisions.invalid = append(csrDecisions.invalid, csr)
			if at := csr.CreationTimestamp.Time; csrDecisions.invalidReason == "" || at.After(invalidAt) {
				csrDecisions.invalidReason = csr.Name + ": " + reason
				invalidAt = at
			}
			continue
		}
		switch {
		case clientCSRDenied(csr):
			csrDecisions.denied = append(csrDecisions.denied, csr)
		case len(csr.Status.Certificate) > 0:
			csrDecisions.issued = append(csrDecisions.issued, csr)
		case clientCSRApproved(csr):
			csrDecisions.approved = append(csrDecisions.approved, csr)
		default:
			csrDecisions.pending = append(csrDecisions.pending, csr)
		}
	}
	return csrDecisions
}

// validateKubeletClientCSR reports why csr must not be approved for nodeName.
// The subject, subjectAltName, and usage checks match the kubernetes.io/kube-apiserver-client-kubelet
// signer contract enforced by kube-controller-manager's CSR approver. spec.groups is the
// requestor's groups, not the certificate groups, so system:nodes is required on the
// certificate subject and not on the CSR object. The serving-certificate checklist
// (node requestor, server auth, and node SANs) does not apply to this signer.
func validateKubeletClientCSR(csr *certificatesv1.CertificateSigningRequest, nodeName string) string {
	if !kubeletClientUsagesAllowed(csr.Spec.Usages) {
		return "spec.usages must be digital signature and client auth, with key encipherment optional"
	}
	parsed, reason := parseSingleCertificateRequest(csr.Spec.Request)
	if reason != "" {
		return reason
	}
	if err := parsed.CheckSignature(); err != nil {
		return "certificate request signature is invalid"
	}
	return validateKubeletClientSubject(parsed, nodeName)
}

func validateKubeletClientSubject(parsed *x509.CertificateRequest, nodeName string) string {
	if !slices.Equal(parsed.Subject.Organization, []string{kubeletNodeGroup}) {
		return "subject organization is not system:nodes"
	}
	if len(parsed.DNSNames) > 0 {
		return "DNS subjectAltNames are not allowed"
	}
	if len(parsed.EmailAddresses) > 0 {
		return "Email subjectAltNames are not allowed"
	}
	if len(parsed.IPAddresses) > 0 {
		return "IP subjectAltNames are not allowed"
	}
	if len(parsed.URIs) > 0 {
		return "URI subjectAltNames are not allowed"
	}
	expectedCommonName := kubeletNodeNamePrefix + nodeName
	if parsed.Subject.CommonName != expectedCommonName {
		return "subject common name is not " + expectedCommonName
	}
	return ""
}

func kubeletClientUsagesAllowed(usages []certificatesv1.KeyUsage) bool {
	got := make(map[certificatesv1.KeyUsage]struct{}, len(usages))
	for _, usage := range usages {
		got[usage] = struct{}{}
	}
	return sameKubeletUsageSet(got,
		certificatesv1.UsageDigitalSignature,
		certificatesv1.UsageKeyEncipherment,
		certificatesv1.UsageClientAuth,
	) || sameKubeletUsageSet(got,
		certificatesv1.UsageDigitalSignature,
		certificatesv1.UsageClientAuth,
	)
}

func sameKubeletUsageSet(got map[certificatesv1.KeyUsage]struct{}, want ...certificatesv1.KeyUsage) bool {
	if len(got) != len(want) {
		return false
	}
	for _, usage := range want {
		if _, ok := got[usage]; !ok {
			return false
		}
	}
	return true
}

func parseSingleCertificateRequest(request []byte) (*x509.CertificateRequest, string) {
	block, trailing := pem.Decode(request)
	if block == nil || block.Type != certificateRequestPEMType || strings.TrimSpace(string(trailing)) != "" {
		return nil, "spec.request is not a single PEM-encoded certificate request"
	}
	parsed, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, "spec.request cannot be parsed as a certificate request"
	}
	return parsed, ""
}

func clientCSRApproved(csr *certificatesv1.CertificateSigningRequest) bool {
	return clientCSRConditionTrue(csr, certificatesv1.CertificateApproved)
}

func clientCSRDenied(csr *certificatesv1.CertificateSigningRequest) bool {
	return clientCSRConditionTrue(csr, certificatesv1.CertificateDenied) || clientCSRConditionTrue(csr, certificatesv1.CertificateFailed)
}

func clientCSRConditionTrue(csr *certificatesv1.CertificateSigningRequest, conditionType certificatesv1.RequestConditionType) bool {
	for _, condition := range csr.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func approveKubeletClientCSRs(ctx context.Context, tenantClient client.Client, machine *clusterv1.Machine, csrs []*certificatesv1.CertificateSigningRequest, logger *slog.Logger) error {
	slices.SortFunc(csrs, func(left, right *certificatesv1.CertificateSigningRequest) int {
		return strings.Compare(left.Name, right.Name)
	})
	for _, csr := range csrs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := approveKubeletClientCSR(ctx, tenantClient, csr); err != nil {
			return err
		}
		logger.Info("Approved kubelet client CSR", "machineName", machine.Name, "csrName", csr.Name, "nodeName", expectedKubeletNodeName(machine))
	}
	return nil
}

func approveKubeletClientCSR(ctx context.Context, tenantClient client.Client, csr *certificatesv1.CertificateSigningRequest) error {
	updated := csr.DeepCopy()
	now := metav1.Now()
	updated.Status.Conditions = append(updated.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:               certificatesv1.CertificateApproved,
		Status:             corev1.ConditionTrue,
		Reason:             KubeletClientCSRApprovalReason,
		Message:            "kubelet client CSR matches the Machine bootstrap identity and node name",
		LastUpdateTime:     now,
		LastTransitionTime: now,
	})
	if err := tenantClient.SubResource("approval").Update(ctx, updated); err != nil {
		return fmt.Errorf("approving kubelet client CSR %s: %w", csr.Name, err)
	}
	return nil
}
