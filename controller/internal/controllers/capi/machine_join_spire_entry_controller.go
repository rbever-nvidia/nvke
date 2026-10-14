// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"fmt"
	"log/slog"

	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/config"
	"github.com/dsx-ai-factory/nvke/controller/internal/constants"
)

const (
	kubeadmJoinSpirePathPrefix = "/kubeadm/join/"
	kubeadmJoinSpireHint       = "kubeadm-join"
	kubeadmJoinExecSelector    = "unix:path:/opt/spiffe/bin/k8s-spiffe-workload-jwt-exec-auth"
)

type MachineJoinSpireEntryController struct {
	cfg         config.Config
	k8sClient   client.Client
	logger      *slog.Logger
	trustDomain string
}

func NewMachineJoinSpireEntryController(
	cfg config.Config,
	k8sClient client.Client,
	logger *slog.Logger,
	trustDomain string,
) *MachineJoinSpireEntryController {
	return &MachineJoinSpireEntryController{
		cfg:         cfg,
		k8sClient:   k8sClient,
		logger:      logger,
		trustDomain: trustDomain,
	}
}

func (c *MachineJoinSpireEntryController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1.Machine{}).
		Named("machine-join-spire-entry").
		Complete(c)
}

func (c *MachineJoinSpireEntryController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := c.logger.With("machineNamespace", req.Namespace, "machineName", req.Name)

	machine := &clusterv1.Machine{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, machine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, c.deleteClusterStaticEntry(ctx, machineJoinSpireEntryName(req.Namespace, req.Name))
		}
		return ctrl.Result{}, fmt.Errorf("failed to get machine: %w", err)
	}

	if !c.shouldHaveJoinEntry(machine) {
		return ctrl.Result{}, c.deleteClusterStaticEntry(ctx, machineJoinSpireEntryName(machine.Namespace, machine.Name))
	}

	spireAgentIDPath := machine.GetAnnotations()[constants.MachineAnnotationKeySpireAgentSpiffeIDPath]
	if spireAgentIDPath == "" {
		logger.Info("Waiting for Machine SPIRE agent ID path before creating kubeadm join SPIRE entry")
		return ctrl.Result{}, nil
	}

	parentID := spiffeIDFromPath(c.trustDomain, spireAgentIDPath)
	joinID := spiffeIDFromPath(c.trustDomain, kubeadmJoinSpirePathPrefix+string(machine.UID))
	name := machineJoinSpireEntryName(machine.Namespace, machine.Name)
	logger.Info("Applying kubeadm join ClusterStaticEntry", "entry", name, "parentID", parentID, "spiffeID", joinID)
	if err := c.applyClusterStaticEntry(ctx, name, joinID, parentID); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (c *MachineJoinSpireEntryController) shouldHaveJoinEntry(machine *clusterv1.Machine) bool {
	if !machine.DeletionTimestamp.IsZero() {
		// A deleting Machine should not keep a join-only identity alive.
		return false
	}
	if machine.Status.NodeRef.IsDefined() {
		// nodeRef means kubeadm join has completed, so the temporary discovery identity can be removed.
		return false
	}
	if _, ok := machine.Labels[clusterv1.MachineControlPlaneLabel]; ok {
		// This controller only grants CABPK worker join identities; control-plane bootstrap is handled separately.
		return false
	}
	return true
}

func (c *MachineJoinSpireEntryController) applyClusterStaticEntry(ctx context.Context, name, spiffeID, parentID string) error {
	cse := &spirev1alpha1.ClusterStaticEntry{
		TypeMeta: metav1.TypeMeta{
			APIVersion: spirev1alpha1.SchemeGroupVersion.String(),
			Kind:       "ClusterStaticEntry",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				constants.LabelKeyNvkeManagedBy: constants.FieldManagerMachineJoinSpire,
			},
		},
		Spec: spirev1alpha1.ClusterStaticEntrySpec{
			ClassName: c.cfg.SpireClassName,
			ParentID:  parentID,
			Selectors: []string{kubeadmJoinExecSelector},
			SPIFFEID:  spiffeID,
			Hint:      kubeadmJoinSpireHint,
		},
	}
	if err := c.k8sClient.Patch(
		ctx,
		cse,
		client.Apply,
		client.FieldOwner(constants.FieldManagerMachineJoinSpire),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to apply kubeadm join cluster static entry %s: %w", name, err)
	}
	return nil
}

func (c *MachineJoinSpireEntryController) deleteClusterStaticEntry(ctx context.Context, name string) error {
	cse := &spirev1alpha1.ClusterStaticEntry{}
	cse.SetName(name)
	if err := c.k8sClient.Delete(ctx, cse); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete kubeadm join cluster static entry %s: %w", name, err)
	}
	return nil
}

func machineJoinSpireEntryName(namespace, name string) string {
	return fmt.Sprintf("kubeadm-join-%s-%s", namespace, name)
}
