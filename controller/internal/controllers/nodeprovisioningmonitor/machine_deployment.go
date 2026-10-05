// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodeprovisioningmonitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
	certificatesv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiconditions "sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/metrics"
)

func (c *Controller) reconcileMachineDeployment(ctx context.Context, nodePoolID uuid.UUID) (reconcile.Result, error) {
	nodePool, err := c.db.GetNodePool(ctx, nodePoolID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting node pool %s: %w", nodePoolID, err)
	}
	if !nodePool.UsesMachineDeployment() ||
		nodePool.ControlPlane ||
		nodePool.Conditions.Deleting.Status == db.ConditionStatusTrue ||
		nodePool.ProvisioningMonitorDisabled {
		return reconcile.Result{}, nil
	}

	machines := &clusterv1.MachineList{}
	if err := c.k8sClient.List(
		ctx,
		machines,
		client.InNamespace(constants.NamespaceNKESystem),
		client.MatchingLabels{constants.LabelKeyNodePoolID: nodePool.ID.String()},
	); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing Machines for node pool %s: %w", nodePool.ID, err)
	}

	now := time.Now()
	var nextCheck time.Duration
	for _, machine := range machines.Items {
		if !machineSetOwnedWorker(machine) {
			continue
		}

		condition, inspectionErr := c.machineProvisioningCondition(ctx, &machine)
		if inspectionErr != nil {
			c.logger.Warn(
				"Unable to inspect worker Machine provisioning checkpoint",
				"machine", machine.Name,
				"error", inspectionErr,
			)
		}
		if err := c.patchMachineProvisioningCondition(ctx, &machine, condition); err != nil {
			return reconcile.Result{}, err
		}

		// Timeout replacement applies only while the Machine is not deleting and
		// has not created a tenant Node.
		if !machine.DeletionTimestamp.IsZero() || machine.Status.NodeRef.IsDefined() {
			continue
		}

		provisioningAge := now.Sub(machine.CreationTimestamp.Time)
		if provisioningAge < c.cfg.NodeProvisioningTimeout {
			// Re-calculate the next check time to the shortest remaining timeout across all unjoined Machines.
			untilTimeout := c.cfg.NodeProvisioningTimeout - provisioningAge
			if nextCheck == 0 || untilTimeout < nextCheck {
				nextCheck = untilTimeout
			}
			continue
		}

		cluster, err := c.db.GetCluster(ctx, nodePool.ClusterID)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("getting cluster for node pool %s: %w", nodePool.ID, err)
		}

		c.logger.Warn(
			"Deleting worker Machine that never joined within the provisioning timeout",
			"clusterID", nodePool.ClusterID,
			"nodePoolID", nodePool.ID,
			"nodePoolName", nodePool.Name,
			"machine", machine.Name,
			"nodeProvisioningTimeout", c.cfg.NodeProvisioningTimeout.String(),
			"provisioningAge", provisioningAge.String(),
		)
		if err := c.k8sClient.Delete(ctx, &machine); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("deleting Machine %s stuck provisioning: %w", machine.Name, err)
		}

		metrics.RecordNodeProvisioningGiveup(
			cluster.NicoSiteID().String(),
			nodePool.ClusterID.String(),
			nodePool.ID.String(),
			cluster.Name,
			nodePool.Name,
		)
	}

	return reconcile.Result{RequeueAfter: nextCheck}, nil
}

func (c *Controller) machineProvisioningCondition(ctx context.Context, machine *clusterv1.Machine) (metav1.Condition, error) {
	if !machine.DeletionTimestamp.IsZero() {
		return provisioningCondition(machine, metav1.ConditionFalse, "MachineDeleting", "Machine deletion is in progress"), nil
	}

	bootstrapReady := capiconditions.Get(machine, clusterv1.MachineBootstrapConfigReadyCondition)
	if bootstrapReady == nil || bootstrapReady.Status != metav1.ConditionTrue {
		return provisioningCondition(
			machine,
			metav1.ConditionTrue,
			"WaitingForBootstrapData",
			conditionMessage("Waiting for bootstrap data", bootstrapReady),
		), nil
	}

	infrastructureReady := capiconditions.Get(machine, clusterv1.MachineInfrastructureReadyCondition)
	if infrastructureReady == nil || infrastructureReady.Status != metav1.ConditionTrue {
		return provisioningCondition(
			machine,
			metav1.ConditionTrue,
			"ProvisioningInfrastructure",
			conditionMessage("Waiting for infrastructure provider", infrastructureReady),
		), nil
	}

	if machine.Status.NodeRef.IsDefined() {
		nodeReady := capiconditions.Get(machine, clusterv1.MachineNodeReadyCondition)
		if nodeReady != nil && nodeReady.Status == metav1.ConditionTrue {
			return provisioningCondition(machine, metav1.ConditionFalse, "ProvisioningComplete", "Kubernetes Node is ready"), nil
		}
		return provisioningCondition(
			machine,
			metav1.ConditionTrue,
			"WaitingForKubernetesNodeReady",
			conditionMessage("Waiting for Kubernetes Node to become ready", nodeReady),
		), nil
	}

	if machine.Annotations[constants.MachineAnnotationKeySpireAgentSpiffeIDPath] == "" {
		return provisioningCondition(
			machine,
			metav1.ConditionTrue,
			"WaitingForSPIRERegistration",
			"Infrastructure is ready; waiting for the host SPIRE identity",
		), nil
	}

	joinEntry := &spirev1alpha1.ClusterStaticEntry{}
	joinEntryName := "join-" + string(machine.UID)
	if err := c.k8sClient.Get(ctx, client.ObjectKey{Name: joinEntryName}, joinEntry); err != nil {
		if apierrors.IsNotFound(err) {
			return provisioningCondition(
				machine,
				metav1.ConditionTrue,
				"WaitingForSPIRERegistration",
				"Infrastructure and host SPIRE identity are ready; waiting for the kubeadm join identity",
			), nil
		}
		return provisioningInspectionFailed(machine, fmt.Errorf("getting SPIRE join entry %s: %w", joinEntryName, err))
	}

	csr, err := c.kubeletClientCSR(ctx, machine)
	if err != nil {
		return provisioningInspectionFailed(machine, err)
	}
	if csr == nil {
		return provisioningCondition(
			machine,
			metav1.ConditionTrue,
			"WaitingForKubeadmAuthentication",
			"Infrastructure and SPIRE identities are ready; waiting for kubeadm to request a kubelet client certificate",
		), nil
	}

	for _, csrCondition := range csr.Status.Conditions {
		switch csrCondition.Type {
		case certificatesv1.CertificateDenied:
			return provisioningCondition(
				machine,
				metav1.ConditionTrue,
				"KubeletCSRDenied",
				fmt.Sprintf("Kubelet client CSR %s was denied: %s", csr.Name, csrCondition.Message),
			), nil
		case certificatesv1.CertificateApproved:
			if len(csr.Status.Certificate) == 0 {
				return provisioningCondition(
					machine,
					metav1.ConditionTrue,
					"WaitingForKubeletCertificate",
					fmt.Sprintf("Kubelet client CSR %s is approved; waiting for certificate issuance", csr.Name),
				), nil
			}
			return provisioningCondition(
				machine,
				metav1.ConditionTrue,
				"WaitingForTenantNode",
				fmt.Sprintf("Kubelet client CSR %s is issued; waiting for the tenant Node", csr.Name),
			), nil
		}
	}

	return provisioningCondition(
		machine,
		metav1.ConditionTrue,
		"WaitingForKubeletCSRApproval",
		fmt.Sprintf("Waiting for approval of kubelet client CSR %s", csr.Name),
	), nil
}

func (c *Controller) kubeletClientCSR(ctx context.Context, machine *clusterv1.Machine) (*certificatesv1.CertificateSigningRequest, error) {
	if c.tenantClientProvider == nil {
		return nil, fmt.Errorf("tenant client provider is not configured")
	}

	cluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: machine.Namespace,
		Name:      machine.Spec.ClusterName,
	}, cluster); err != nil {
		return nil, fmt.Errorf("getting CAPI Cluster %s: %w", machine.Spec.ClusterName, err)
	}
	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("getting tenant client for cluster %s: %w", cluster.Name, err)
	}

	csrs := &certificatesv1.CertificateSigningRequestList{}
	if err := tenantClient.List(ctx, csrs); err != nil {
		return nil, fmt.Errorf("listing tenant CSRs for Machine %s: %w", machine.Name, err)
	}
	expectedUsernameSuffix := "/kubeadm/join/" + string(machine.UID)
	for i := range csrs.Items {
		csr := &csrs.Items[i]
		if csr.Spec.SignerName == certificatesv1.KubeAPIServerClientKubeletSignerName &&
			strings.HasSuffix(csr.Spec.Username, expectedUsernameSuffix) {
			return csr, nil
		}
	}
	return nil, nil
}

func (c *Controller) patchMachineProvisioningCondition(ctx context.Context, machine *clusterv1.Machine, condition metav1.Condition) error {
	patchHelper, err := patch.NewHelper(machine, c.k8sClient)
	if err != nil {
		return fmt.Errorf("building patch helper for Machine %s: %w", machine.Name, err)
	}
	capiconditions.Set(machine, condition)
	if err := patchHelper.Patch(ctx, machine, patch.WithOwnedConditions{
		Conditions: []string{constants.MachineConditionTypeProvisioning},
	}); err != nil {
		return fmt.Errorf("patching provisioning condition on Machine %s: %w", machine.Name, err)
	}
	return nil
}

func provisioningCondition(machine *clusterv1.Machine, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               constants.MachineConditionTypeProvisioning,
		Status:             status,
		ObservedGeneration: machine.Generation,
		Reason:             reason,
		Message:            message,
	}
}

func provisioningInspectionFailed(machine *clusterv1.Machine, err error) (metav1.Condition, error) {
	return provisioningCondition(machine, metav1.ConditionUnknown, "InspectionFailed", err.Error()), err
}

func conditionMessage(fallback string, condition *metav1.Condition) string {
	if condition == nil {
		return fallback
	}
	switch {
	case condition.Message != "":
		return fmt.Sprintf("%s: %s: %s", fallback, condition.Reason, condition.Message)
	case condition.Reason != "":
		return fmt.Sprintf("%s: %s", fallback, condition.Reason)
	default:
		return fallback
	}
}

func machineSetOwnedWorker(machine clusterv1.Machine) bool {
	if _, controlPlane := machine.Labels[clusterv1.MachineControlPlaneLabel]; controlPlane {
		return false
	}
	for _, owner := range machine.OwnerReferences {
		if owner.Kind == "MachineSet" {
			return true
		}
	}
	return false
}
