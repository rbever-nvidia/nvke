// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	"github.com/dsx-ai-factory/nvke/controller/internal/config"
	"github.com/dsx-ai-factory/nvke/controller/internal/kube"
)

const (
	bootstrapClusterRoleBindingName            = "nvke:node-bootstrapper"
	bootstrapGetNodesClusterRoleBindingName    = "nvke:node-bootstrapper:get-nodes"
	bootstrapKubeletConfigRoleBindingName      = "nvke:node-bootstrapper:kubelet-config"
	bootstrapNodesKubeadmConfigRoleBindingName = "nvke:node-bootstrapper:nodes-kubeadm-config"
)

type bootstrapBindingRef struct {
	name      string
	namespace string
	cluster   bool
}

type bootstrapSubjectPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

var bootstrapBindingRefs = []bootstrapBindingRef{
	{name: bootstrapClusterRoleBindingName, cluster: true},
	{name: bootstrapGetNodesClusterRoleBindingName, cluster: true},
	{name: bootstrapKubeletConfigRoleBindingName, namespace: metav1.NamespaceSystem},
	{name: bootstrapNodesKubeadmConfigRoleBindingName, namespace: metav1.NamespaceSystem},
}

// MachineBootstrapRBACController grants kubeadm bootstrap RBAC to the exact worker identities that are still joining.
type MachineBootstrapRBACController struct {
	cfg                  config.Config
	k8sClient            client.Client
	tenantClientProvider kube.TenantClientProvider
	logger               *slog.Logger
	trustDomain          string
}

func NewMachineBootstrapRBACController(
	cfg config.Config,
	k8sClient client.Client,
	tenantClientProvider kube.TenantClientProvider,
	logger *slog.Logger,
	trustDomain string,
) *MachineBootstrapRBACController {
	return &MachineBootstrapRBACController{
		cfg:                  cfg,
		k8sClient:            k8sClient,
		tenantClientProvider: tenantClientProvider,
		logger:               logger,
		trustDomain:          trustDomain,
	}
}

func (c *MachineBootstrapRBACController) SetupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).For(&clusterv1.Machine{})
	if c.cfg.WorkerBootstrapIdentitySource == config.WorkerBootstrapIdentitySourceIMDS {
		builder = builder.Watches(
			&nicov1.NicoMachine{},
			handler.EnqueueRequestsFromMapFunc(machineRequestsForNicoMachine),
		)
	}
	return builder.
		Named("machine-bootstrap-rbac").
		Complete(c)
}

func machineRequestsForNicoMachine(_ context.Context, obj client.Object) []reconcile.Request {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.APIVersion == clusterv1.GroupVersion.String() && owner.Kind == "Machine" && owner.Name != "" {
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: owner.Name}}}
		}
	}
	return nil
}

func (c *MachineBootstrapRBACController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := c.logger.With("namespace", req.Namespace, "machineName", req.Name)

	machine := &clusterv1.Machine{}
	if err := c.k8sClient.Get(ctx, req.NamespacedName, machine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get Machine: %w", err)
	}
	logger = logger.With("resourceVersion", machine.ResourceVersion)

	if machineIsControlPlane(machine) {
		logger.Debug("Control-plane Machine does not need bootstrap RBAC")
		return ctrl.Result{}, nil
	}

	subject, ok, err := c.bootstrapSubject(ctx, machine)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ok {
		logger.Info("Waiting for bootstrap subject identity")
		return ctrl.Result{}, nil
	}

	cluster, err := c.getMachineCluster(ctx, machine)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cluster == nil {
		logger.Info("CAPI Cluster no longer exists, skipping bootstrap RBAC")
		return ctrl.Result{}, nil
	}

	tenantClient, err := c.tenantClientProvider.ClientForCluster(ctx, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, kube.ErrTenantClientNotReady) {
			logger.Info("Waiting for CAPI kubeconfig secret", "clusterName", cluster.Name)
			return ctrl.Result{RequeueAfter: tenantClusterReadinessRetryAfter}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to create tenant cluster client: %w", err)
	}

	present := machineNeedsBootstrapRBAC(machine)
	if err := reconcileBootstrapRBACSubject(ctx, tenantClient, subject, present); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Reconciled bootstrap RBAC subject", "subject", subject, "present", present)
	return ctrl.Result{}, nil
}

func (c *MachineBootstrapRBACController) getMachineCluster(ctx context.Context, machine *clusterv1.Machine) (*clusterv1.Cluster, error) {
	if machine.Spec.ClusterName == "" {
		return nil, fmt.Errorf("machine %s/%s has empty spec.clusterName", machine.Namespace, machine.Name)
	}

	cluster := &clusterv1.Cluster{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.ClusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get CAPI cluster: %w", err)
	}
	return cluster, nil
}

func (c *MachineBootstrapRBACController) bootstrapSubject(ctx context.Context, machine *clusterv1.Machine) (string, bool, error) {
	switch c.cfg.WorkerBootstrapIdentitySource {
	case config.WorkerBootstrapIdentitySourceSPIRE:
		return spiffeIDFromPath(c.trustDomain, kubeadmJoinSpirePathPrefix+string(machine.UID)), true, nil
	case config.WorkerBootstrapIdentitySourceIMDS:
		return c.imdsBootstrapSubject(ctx, machine)
	default:
		return "", false, fmt.Errorf("unsupported worker bootstrap identity source %q", c.cfg.WorkerBootstrapIdentitySource)
	}
}

func (c *MachineBootstrapRBACController) imdsBootstrapSubject(ctx context.Context, machine *clusterv1.Machine) (string, bool, error) {
	if machine.Spec.InfrastructureRef.APIGroup != nicov1.GroupVersion.Group ||
		machine.Spec.InfrastructureRef.Kind != "NicoMachine" || machine.Spec.InfrastructureRef.Name == "" {
		return "", false, nil
	}

	nicoMachine := &nicov1.NicoMachine{}
	if err := c.k8sClient.Get(ctx, client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.InfrastructureRef.Name}, nicoMachine); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to get NicoMachine: %w", err)
	}
	if nicoMachine.Status.PrimaryDPUMachineID == "" {
		return "", false, nil
	}

	issuerURL, err := url.Parse(c.cfg.NodeAgent.DSXImds.IssuerURL)
	if err != nil {
		return "", false, fmt.Errorf("parse DSX IMDS issuer URL: %w", err)
	}
	if issuerURL.Host == "" {
		return "", false, fmt.Errorf("DSX IMDS issuer URL %q does not include a host", c.cfg.NodeAgent.DSXImds.IssuerURL)
	}
	if c.cfg.NodeAgent.DSXImds.OperatorTenant == "" {
		return "", false, errors.New("DSX IMDS operator tenant is required for exact bootstrap RBAC")
	}
	return fmt.Sprintf("spiffe://%s.%s/bm/%s", c.cfg.NodeAgent.DSXImds.OperatorTenant, issuerURL.Host, nicoMachine.Status.PrimaryDPUMachineID), true, nil
}

func machineNeedsBootstrapRBAC(machine *clusterv1.Machine) bool {
	if !machine.DeletionTimestamp.IsZero() || machine.Status.NodeRef.IsDefined() {
		return false
	}
	return true
}

func machineIsControlPlane(machine *clusterv1.Machine) bool {
	_, controlPlane := machine.Labels[clusterv1.MachineControlPlaneLabel]
	return controlPlane
}

func reconcileBootstrapRBACSubject(ctx context.Context, tenantClient client.Client, subject string, present bool) error {
	for _, binding := range bootstrapBindingRefs {
		if binding.cluster {
			if err := patchBootstrapClusterRoleBindingSubject(ctx, tenantClient, binding.name, subject, present); err != nil {
				return err
			}
			continue
		}

		if err := patchBootstrapRoleBindingSubject(ctx, tenantClient, binding.namespace, binding.name, subject, present); err != nil {
			return err
		}
	}
	return nil
}

func patchBootstrapClusterRoleBindingSubject(ctx context.Context, tenantClient client.Client, name, subject string, present bool) error {
	// The JSON patch below tests resourceVersion. If another writer changes the
	// binding after our read, the patch conflicts and this loop recomputes the
	// subject list from the newest object instead of overwriting that change.
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		clusterRoleBinding := &rbacv1.ClusterRoleBinding{}
		if err := tenantClient.Get(ctx, client.ObjectKey{Name: name}, clusterRoleBinding); err != nil {
			return fmt.Errorf("get bootstrap ClusterRoleBinding %s: %w", name, err)
		}
		if err := patchBootstrapSubjects(ctx, tenantClient, clusterRoleBinding, &clusterRoleBinding.Subjects, subject, present); err != nil {
			return fmt.Errorf("patch bootstrap ClusterRoleBinding %s: %w", name, err)
		}
		return nil
	})
}

func patchBootstrapRoleBindingSubject(ctx context.Context, tenantClient client.Client, namespace, name, subject string, present bool) error {
	// The JSON patch below tests resourceVersion. If another writer changes the
	// binding after our read, the patch conflicts and this loop recomputes the
	// subject list from the newest object instead of overwriting that change.
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		roleBinding := &rbacv1.RoleBinding{}
		if err := tenantClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, roleBinding); err != nil {
			return fmt.Errorf("get bootstrap RoleBinding %s/%s: %w", namespace, name, err)
		}
		if err := patchBootstrapSubjects(ctx, tenantClient, roleBinding, &roleBinding.Subjects, subject, present); err != nil {
			return fmt.Errorf("patch bootstrap RoleBinding %s/%s: %w", namespace, name, err)
		}
		return nil
	})
}

func patchBootstrapSubjects(
	ctx context.Context,
	tenantClient client.Client,
	binding client.Object,
	subjects *[]rbacv1.Subject,
	subject string,
	present bool,
) error {
	if !reconcileSubjectList(subjects, subject, present) {
		return nil
	}

	// SSA would own subjects as one list, not one entry per Machine.
	// Tenant-access owns the baseline binding without subjects; this controller
	// JSON-patches only subjects and tests resourceVersion so concurrent list
	// changes are rejected and retried instead of being overwritten.
	patch, err := json.Marshal([]bootstrapSubjectPatchOperation{
		{Op: "test", Path: "/metadata/resourceVersion", Value: binding.GetResourceVersion()},
		{Op: "add", Path: "/subjects", Value: *subjects},
	})
	if err != nil {
		return fmt.Errorf("build bootstrap subject patch: %w", err)
	}
	return tenantClient.Patch(ctx, binding, client.RawPatch(types.JSONPatchType, patch))
}

func reconcileSubjectList(subjects *[]rbacv1.Subject, subject string, present bool) bool {
	desired := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: subject}
	for i, existing := range *subjects {
		if existing == desired {
			if present {
				return false
			}
			*subjects = append((*subjects)[:i], (*subjects)[i+1:]...)
			return true
		}
	}
	if !present {
		return false
	}

	*subjects = append(*subjects, desired)
	sort.SliceStable(*subjects, func(i, j int) bool {
		left := strings.Join([]string{(*subjects)[i].Kind, (*subjects)[i].APIGroup, (*subjects)[i].Name}, "\x00")
		right := strings.Join([]string{(*subjects)[j].Kind, (*subjects)[j].APIGroup, (*subjects)[j].Name}, "\x00")
		return left < right
	})
	return true
}
