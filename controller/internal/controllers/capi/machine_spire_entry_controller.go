// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

type machineSpireEntryNodeReader interface {
	GetNode(ctx context.Context, id uuid.UUID) (db.Node, error)
}

type MachineSpireEntryController struct {
	cfg         config.Config
	k8sClient   client.Client
	nodes       machineSpireEntryNodeReader
	logger      *slog.Logger
	trustDomain string
}

func NewMachineSpireEntryController(
	cfg config.Config,
	k8sClient client.Client,
	nodes machineSpireEntryNodeReader,
	logger *slog.Logger,
	trustDomain string,
) *MachineSpireEntryController {
	return &MachineSpireEntryController{
		cfg:         cfg,
		k8sClient:   k8sClient,
		nodes:       nodes,
		logger:      logger,
		trustDomain: trustDomain,
	}
}

func (c *MachineSpireEntryController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1.Machine{}).
		Named("machine-spire-entry").
		Complete(c)
}

func (c *MachineSpireEntryController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := c.logger.With("machineNamespace", req.Namespace, "machineName", req.Name)

	machine := &clusterv1.Machine{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, machine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get machine: %w", err)
	}

	hostID := hostIDForMachine(machine)
	if !machine.DeletionTimestamp.IsZero() {
		if hostID == "" {
			return ctrl.Result{}, nil
		}
		if err := c.deleteClusterStaticEntry(ctx, machineSpireEntryName(hostID)); err != nil {
			return ctrl.Result{}, err
		}
		if err := c.deleteClusterStaticEntry(ctx, machineJoinSpireEntryName(hostID)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if hostID == "" {
		logger.Debug("Machine has no host-id annotation, skipping ClusterStaticEntry reconciliation")
		return ctrl.Result{}, nil
	}

	spireAgentIDPath := machine.GetAnnotations()[constants.MachineAnnotationKeySpireAgentSpiffeIDPath]
	if spireAgentIDPath == "" {
		logger.Info("Waiting for spire agent ID path to be set on Machine", "hostID", hostID)
		return ctrl.Result{}, nil
	}
	parentID := spiffeIDFromPath(c.trustDomain, spireAgentIDPath)
	logger.Info("Applying ClusterStaticEntry for machine", "hostID", hostID, "parentID", parentID)
	cseName := machineSpireEntryName(hostID)
	workloadSpiffeID, err := c.workloadSpiffeID(ctx, machine, hostID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := c.applyClusterStaticEntry(ctx, cseName, spirev1alpha1.ClusterStaticEntrySpec{
		ClassName: c.cfg.SpireClassName,
		SPIFFEID:  workloadSpiffeID,
		ParentID:  parentID,
		Selectors: []string{"unix:uid:0"},
	}); err != nil {
		return ctrl.Result{}, err
	}

	if machineSetOwnedWorker(machine) {
		logger.Info("Applying ClusterStaticEntry for machine join", "hostID", hostID, "parentID", parentID)

		joinCSEName := machineJoinSpireEntryName(hostID)
		if machine.Status.NodeRef.Name != "" {
			// Machine has a nodeRef, so it is already joined to the cluster, the join only ClusterStaticEntry should be deleted.
			logger.Info("Machine has a nodeRef, Deleting ClusterStaticEntry for machine join", "hostID", hostID)
			if err := c.deleteClusterStaticEntry(ctx, joinCSEName); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			joinSPIFFEID := fmt.Sprintf("spiffe://%s/kubeadm/join/%s", c.trustDomain, hostID)
			if err := c.applyClusterStaticEntry(ctx, joinCSEName, spirev1alpha1.ClusterStaticEntrySpec{
				ClassName: c.cfg.SpireClassName,
				SPIFFEID:  joinSPIFFEID,
				ParentID:  parentID,
				Selectors: []string{constants.CAPIWorkerJoinSpiffeSelector},
				Hint:      constants.CAPIWorkerJoinSpiffeHint,
			}); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

func (c *MachineSpireEntryController) workloadSpiffeID(ctx context.Context, machine *clusterv1.Machine, hostID string) (string, error) {
	if machineSetOwnedWorker(machine) {
		return fmt.Sprintf("spiffe://%s/host/%s", c.trustDomain, hostID), nil
	}

	nodeID, err := uuid.Parse(hostID)
	if err != nil {
		return "", fmt.Errorf("invalid host ID %q: %w", hostID, err)
	}
	node, err := c.nodes.GetNode(ctx, nodeID)
	if err != nil {
		return "", fmt.Errorf("get node %s: %w", nodeID, err)
	}
	return node.GetSpiffeID(
		c.trustDomain,
		c.cfg.NodeAgent.TLSJWT.SubjectTenant,
		c.cfg.NodeAgent.TLSJWT.SubjectService,
	), nil
}

func (c *MachineSpireEntryController) applyClusterStaticEntry(ctx context.Context, name string, spec spirev1alpha1.ClusterStaticEntrySpec) error {
	cse := &spirev1alpha1.ClusterStaticEntry{
		TypeMeta: metav1.TypeMeta{
			APIVersion: spirev1alpha1.SchemeGroupVersion.String(),
			Kind:       "ClusterStaticEntry",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				constants.LabelKeyNkeManagedBy: constants.FieldManagerMachineSpireEntry,
			},
		},
		Spec: spec,
	}

	if err := c.k8sClient.Patch(
		ctx,
		cse,
		client.Apply,
		client.FieldOwner(constants.FieldManagerMachineSpireEntry),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to apply cluster static entry %s: %w", name, err)
	}

	return nil
}

func (c *MachineSpireEntryController) deleteClusterStaticEntry(ctx context.Context, name string) error {
	cse := &spirev1alpha1.ClusterStaticEntry{}
	cse.SetName(name)
	if err := c.k8sClient.Delete(ctx, cse); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete cluster static entry %s: %w", cse.GetName(), err)
	}
	return nil
}

func machineSpireEntryName(hostID string) string {
	return fmt.Sprintf("host-%s", hostID)
}

func machineJoinSpireEntryName(hostID string) string {
	return fmt.Sprintf("join-%s", hostID)
}

func hostIDForMachine(machine *clusterv1.Machine) string {
	// Legacy workers and control-plane Machines are backed by db.Node rows, their host-id annotation is db.Node.ID.
	if hostID := machine.GetAnnotations()[constants.MachineAnnotationKeyHostID]; hostID != "" {
		return hostID
	}
	// Do not fall back to Machine.UID for every Machine. On legacy and
	// control-plane paths, a missing host-id annotation means it may just not be ready yet.
	// Creating a UID-based SVID would hide that bug and introduce a
	// second identity for an existing node. MachineSet-owned workers are the
	// only path here where no legacy host ID is expected.
	if machineSetOwnedWorker(machine) {
		return string(machine.UID)
	}
	return ""
}

func machineSetOwnedWorker(machine *clusterv1.Machine) bool {
	if _, ok := machine.Labels[clusterv1.MachineControlPlaneLabel]; ok {
		return false
	}
	for _, owner := range machine.OwnerReferences {
		if owner.Kind == "MachineSet" {
			return true
		}
	}
	return false
}

func spiffeIDFromPath(trustDomain, path string) string {
	if strings.HasPrefix(path, "/") {
		return fmt.Sprintf("spiffe://%s%s", trustDomain, path)
	}
	return fmt.Sprintf("spiffe://%s/%s", trustDomain, path)
}
