// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nkecluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"

	capibuild "github.com/dsx-ai-factory/nke/controller/internal/capi"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/kubeadmconfig"
	"github.com/dsx-ai-factory/nke/controller/internal/machinerelease"
	enginev1 "github.com/dsx-ai-factory/nke/management-engine/apis/v1"
)

const (
	controllerName     = "nkecluster-capi-sync"
	templateHashLength = 16

	finalizerNKECluster = "internal.nke.nvidia.com/nkecluster"

	labelNKECluster = "internal.nke.nvidia.com/nkecluster-name"
	labelNodePool   = "internal.nke.nvidia.com/node-pool-name"

	workerCRISocket = "unix:///var/run/containerd/containerd.sock"

	pocNicoOrgID  = "novxwn552ssc"
	pocNicoSiteID = "a1b2c3d4-0001-4000-8000-000000000001"

	// FIXME: POC: disk selection belongs in the resolved machine/placement class.
	pocDiskSelectionStrategy = string(db.DiskSelectionStrategySmallest)

	// FIXME: POC: worker releases need a versioned artifact/package contract for this binary.
	pocSpireExecAuthInstallCommand = `sh -c 'set -eu; p=/opt/spiffe/bin/k8s-spiffe-workload-jwt-exec-auth; if [ ! -x "$p" ]; then mkdir -p /opt/spiffe/bin; u=http://carbide-pxe.forge:18080/files/spiffe/k8s-spiffe-workload-jwt-exec-auth; curl -fsSL "$u" -o /tmp/k8s-spiffe-workload-jwt-exec-auth; install -m 0755 /tmp/k8s-spiffe-workload-jwt-exec-auth "$p"; echo "downloaded SPIFFE JWT exec plugin"; fi'`

	pocSpireAgentPathSelectorDropInPath    = "/etc/systemd/system/spire-agent.service.d/10-workload-path.conf"
	pocSpireAgentPathSelectorDropInContent = "[Service]\nAmbientCapabilities=CAP_SYS_PTRACE\n"
)

// Controller reconciles the tenant-facing NKECluster API directly into CAPI resources.
type Controller struct {
	cfg         config.Config
	db          db.DB
	k8s         client.Client
	logger      *slog.Logger
	trustDomain string
}

// NewController returns a tenant NKECluster controller.
func NewController(cfg config.Config, database db.DB, k8s client.Client, logger *slog.Logger, trustDomain string) *Controller {
	return &Controller{
		cfg:         cfg,
		db:          database,
		k8s:         k8s,
		logger:      logger,
		trustDomain: trustDomain,
	}
}

// SetupWithManager registers the controller with controller-runtime.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		For(&enginev1.NKECluster{}).
		Complete(c)
}

// Reconcile applies MachineDeployments for worker pools embedded in the tenant-facing NKECluster.
func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var cluster enginev1.NKECluster
	if err := c.k8s.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting NKECluster %s: %w", req.NamespacedName, err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return c.reconcileDelete(ctx, cluster)
	}
	if controllerutil.AddFinalizer(&cluster, finalizerNKECluster) {
		if err := c.k8s.Update(ctx, &cluster); err != nil {
			return reconcile.Result{}, fmt.Errorf("adding NKECluster finalizer: %w", err)
		}
		return reconcile.Result{}, nil
	}

	controlPlaneClass, found, err := c.getControlPlaneClass(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !found {
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}
	controlPlaneMachineClass, found, err := c.getControlPlaneMachineClass(ctx, cluster, controlPlaneClass)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !found {
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}
	clusterRelease, clusterMachineImage, found, err := c.resolveClusterRelease(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !found {
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}
	controlPlaneVersion, err := kubernetesVersionFromRelease(
		clusterRelease,
		cluster.Spec.Release.Channel.Version,
	)
	if err != nil {
		return reconcile.Result{}, err
	}
	if cluster.Spec.Network == nil {
		c.logger.Info("waiting for NKECluster network before applying control plane and node pools", "nkeCluster", req.NamespacedName)
		return reconcile.Result{}, nil
	}
	dbCluster, err := c.ensureDBCluster(ctx, cluster, controlPlaneClass, controlPlaneMachineClass, controlPlaneVersion, clusterRelease, clusterMachineImage)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := capibuild.ApplyClusterScaffold(ctx, c.k8s, dbCluster, false, c.cfg.FieldManagerName, c.cfg.ManagedByLabelValue); err != nil {
		return reconcile.Result{}, fmt.Errorf("applying compatibility CAPI cluster scaffold: %w", err)
	}
	if len(cluster.Spec.NodePools) == 0 {
		return reconcile.Result{}, nil
	}

	for _, pool := range cluster.Spec.NodePools {
		machineClass, found, err := c.getNodePoolMachineClass(ctx, cluster, pool)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !found {
			return reconcile.Result{RequeueAfter: time.Minute}, nil
		}
		nodePoolRelease, machineImage, found, err := c.resolveNodePoolRelease(ctx, cluster, pool)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !found {
			return reconcile.Result{RequeueAfter: time.Minute}, nil
		}
		workerVersion, err := kubernetesVersionFromRelease(
			nodePoolRelease,
			cluster.Spec.Release.Channel.Version,
		)
		if err != nil {
			return reconcile.Result{}, err
		}
		if err := c.applyNodePool(ctx, cluster, pool, machineClass, workerVersion, nodePoolRelease, machineImage); err != nil {
			return reconcile.Result{}, fmt.Errorf("applying node pool %s: %w", pool.Name, err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) reconcileDelete(ctx context.Context, cluster enginev1.NKECluster) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(&cluster, finalizerNKECluster) {
		return reconcile.Result{}, nil
	}
	directResourcesRemain, err := c.deleteDirectCAPIResources(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}
	dbClusterGone, err := c.ensureCompatibilityClusterDeleting(ctx, cluster.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	if directResourcesRemain || !dbClusterGone {
		c.logger.Info("waiting for NKECluster teardown", "nkeCluster", cluster.Name, "directResourcesRemain", directResourcesRemain, "dbClusterGone", dbClusterGone)
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}
	controllerutil.RemoveFinalizer(&cluster, finalizerNKECluster)
	if err := c.k8s.Update(ctx, &cluster); err != nil {
		return reconcile.Result{}, fmt.Errorf("removing NKECluster finalizer: %w", err)
	}
	return reconcile.Result{}, nil
}

func (c *Controller) deleteDirectCAPIResources(ctx context.Context, cluster enginev1.NKECluster) (bool, error) {
	matcher := client.MatchingLabels{labelNKECluster: cluster.Name}
	remaining := false

	machineDeployments := &clusterv1.MachineDeploymentList{}
	if err := c.k8s.List(ctx, machineDeployments, client.InNamespace(constants.NamespaceNKESystem), matcher); err != nil {
		return false, fmt.Errorf("listing NKECluster MachineDeployments: %w", err)
	}
	for i := range machineDeployments.Items {
		md := &machineDeployments.Items[i]
		remaining = true
		if md.DeletionTimestamp.IsZero() {
			c.logger.Info("deleting NKECluster MachineDeployment", "nkeCluster", cluster.Name, "machineDeployment", md.Name)
			if err := c.k8s.Delete(ctx, md); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("deleting MachineDeployment %s: %w", md.Name, err)
			}
		}
	}

	kubeadmConfigTemplates := &bootstrapv1.KubeadmConfigTemplateList{}
	if err := c.k8s.List(ctx, kubeadmConfigTemplates, client.InNamespace(constants.NamespaceNKESystem), matcher); err != nil {
		return false, fmt.Errorf("listing NKECluster KubeadmConfigTemplates: %w", err)
	}
	for i := range kubeadmConfigTemplates.Items {
		template := &kubeadmConfigTemplates.Items[i]
		remaining = true
		if template.DeletionTimestamp.IsZero() {
			c.logger.Info("deleting NKECluster KubeadmConfigTemplate", "nkeCluster", cluster.Name, "kubeadmConfigTemplate", template.Name)
			if err := c.k8s.Delete(ctx, template); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("deleting KubeadmConfigTemplate %s: %w", template.Name, err)
			}
		}
	}

	nicoMachineTemplates := &nicov1.NicoMachineTemplateList{}
	if err := c.k8s.List(ctx, nicoMachineTemplates, client.InNamespace(constants.NamespaceNKESystem), matcher); err != nil {
		return false, fmt.Errorf("listing NKECluster NicoMachineTemplates: %w", err)
	}
	for i := range nicoMachineTemplates.Items {
		template := &nicoMachineTemplates.Items[i]
		remaining = true
		if template.DeletionTimestamp.IsZero() {
			c.logger.Info("deleting NKECluster NicoMachineTemplate", "nkeCluster", cluster.Name, "nicoMachineTemplate", template.Name)
			if err := c.k8s.Delete(ctx, template); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("deleting NicoMachineTemplate %s: %w", template.Name, err)
			}
		}
	}

	return remaining, nil
}

func (c *Controller) ensureCompatibilityClusterDeleting(ctx context.Context, name string) (bool, error) {
	dbClusterGone := false
	if err := c.db.RunInTx(ctx, func(ctx context.Context, tx db.Tx) error {
		clusters, err := tx.ListClusters(ctx, db.ClusterFilters{Name: &name})
		if err != nil {
			return fmt.Errorf("listing compatibility clusters by name %s: %w", name, err)
		}
		if len(clusters) == 0 {
			dbClusterGone = true
			return nil
		}
		cluster := clusters[0]
		if cluster.Conditions.Deleting.Status == db.ConditionStatusTrue {
			return nil
		}
		cluster.Conditions.Deleting = db.NewCondition(true, "Cluster deletion requested by NKECluster")
		if _, err := tx.UpdateCluster(ctx, &cluster); err != nil {
			return fmt.Errorf("marking compatibility cluster %s deleting: %w", name, err)
		}
		workflowID := constants.TeardownClusterWorkflowPrefix + cluster.ID.String()
		if _, err := tx.CreateWorkflowOutboxEntry(ctx, "TeardownCluster", workflowID, map[string]interface{}{"clusterID": cluster.ID}); err != nil {
			return fmt.Errorf("creating teardown workflow outbox for compatibility cluster %s: %w", name, err)
		}
		return nil
	}); err != nil {
		return false, err
	}
	return dbClusterGone, nil
}

func (c *Controller) getControlPlaneClass(ctx context.Context, cluster enginev1.NKECluster) (enginev1.ControlPlaneClass, bool, error) {
	var controlPlaneClass enginev1.ControlPlaneClass
	key := types.NamespacedName{Name: string(cluster.Spec.ControlPlaneClass)}
	if err := c.k8s.Get(ctx, key, &controlPlaneClass); err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Info("waiting for NKECluster control plane class", "nkeCluster", cluster.Name, "controlPlaneClass", cluster.Spec.ControlPlaneClass)
			return enginev1.ControlPlaneClass{}, false, nil
		}
		return enginev1.ControlPlaneClass{}, false, fmt.Errorf("getting ControlPlaneClass %s: %w", cluster.Spec.ControlPlaneClass, err)
	}
	return controlPlaneClass, true, nil
}

func (c *Controller) getControlPlaneMachineClass(ctx context.Context, cluster enginev1.NKECluster, controlPlaneClass enginev1.ControlPlaneClass) (enginev1.MachineClass, bool, error) {
	if controlPlaneClass.Spec.Machine.MachineClass == "" {
		return enginev1.MachineClass{}, true, nil
	}
	machineClass, found, err := c.getMachineClass(ctx, string(controlPlaneClass.Spec.Machine.MachineClass), cluster.Name)
	if !found {
		c.logger.Info("waiting for control-plane machine class", "nkeCluster", cluster.Name, "controlPlaneClass", controlPlaneClass.Name, "machineClass", controlPlaneClass.Spec.Machine.MachineClass)
	}
	return machineClass, found, err
}

func (c *Controller) getNodePoolMachineClass(ctx context.Context, cluster enginev1.NKECluster, pool enginev1.NodePool) (enginev1.MachineClass, bool, error) {
	if pool.MachineClass == "" {
		return enginev1.MachineClass{}, true, nil
	}
	machineClass, found, err := c.getMachineClass(ctx, string(pool.MachineClass), cluster.Name)
	if !found {
		c.logger.Info("waiting for node pool machine class", "nkeCluster", cluster.Name, "nodePool", pool.Name, "machineClass", pool.MachineClass)
	}
	return machineClass, found, err
}

func (c *Controller) getMachineClass(ctx context.Context, name string, clusterName string) (enginev1.MachineClass, bool, error) {
	var machineClass enginev1.MachineClass
	key := types.NamespacedName{Name: name}
	if err := c.k8s.Get(ctx, key, &machineClass); err != nil {
		if apierrors.IsNotFound(err) {
			return enginev1.MachineClass{}, false, nil
		}
		return enginev1.MachineClass{}, false, fmt.Errorf("getting MachineClass %s for NKECluster %s: %w", name, clusterName, err)
	}
	return machineClass, true, nil
}

func (c *Controller) resolveClusterRelease(ctx context.Context, cluster enginev1.NKECluster) (*enginev1.ClusterRelease, *enginev1.MachineImage, bool, error) {
	// FIXME: POC: release channel promotion is not modeled yet. Treat channel.name as a direct ClusterRelease name for now.
	name := string(cluster.Spec.Release.Channel.Name)
	release, image, err := machinerelease.ResolveClusterRelease(ctx, c.k8s, name)
	if err != nil {
		if releaseUnavailable(err) {
			c.logger.Info("waiting for NKECluster release", "nkeCluster", cluster.Name, "clusterRelease", name, "err", err)
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("resolving ClusterRelease %s: %w", name, err)
	}
	return release, image, true, nil
}

func (c *Controller) resolveNodePoolRelease(ctx context.Context, cluster enginev1.NKECluster, pool enginev1.NodePool) (*enginev1.NodePoolRelease, *enginev1.MachineImage, bool, error) {
	// FIXME: POC: nodeProfile tag resolution needs the product catalog contract. Treat <profile>:<tag> as release name <profile> for now.
	releaseName := strings.SplitN(string(pool.NodeProfile), ":", 2)[0]
	release, image, err := machinerelease.ResolveNodePoolRelease(ctx, c.k8s, releaseName)
	if err != nil {
		if releaseUnavailable(err) {
			c.logger.Info("waiting for node pool release", "nkeCluster", cluster.Name, "nodePool", pool.Name, "nodeProfile", pool.NodeProfile, "nodePoolRelease", releaseName, "err", err)
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("resolving NodePoolRelease %s: %w", releaseName, err)
	}
	return release, image, true, nil
}

func (c *Controller) ensureDBCluster(
	ctx context.Context,
	cluster enginev1.NKECluster,
	controlPlaneClass enginev1.ControlPlaneClass,
	machineClass enginev1.MachineClass,
	kubernetesVersion string,
	clusterRelease *enginev1.ClusterRelease,
	machineImage *enginev1.MachineImage,
) (db.Cluster, error) {
	name := cluster.Name
	var ensured db.Cluster
	if err := c.db.RunInTx(ctx, func(ctx context.Context, tx db.Tx) error {
		clusters, err := tx.ListClusters(ctx, db.ClusterFilters{Name: &name})
		if err != nil {
			return fmt.Errorf("listing clusters by name %s: %w", name, err)
		}
		desired, err := dbClusterFromNKECluster(ctx, c.cfg, cluster, controlPlaneClass, machineClass, kubernetesVersion, clusterRelease, machineImage)
		if err != nil {
			return err
		}
		if len(clusters) > 0 {
			ensured = clusters[0]
			applyDBClusterProjection(&ensured, desired)
			cpSpec := db.ConfigSpecFromFull(ensured.ControlPlaneConfig)
			if err := db.CreateControlPlaneConfigRowIfChanged(ctx, tx, &ensured, cpSpec, db.CreateControlPlaneConfigRowOpts{
				ChangeReason: "NKECluster reconcile controlPlaneClass",
			}); err != nil {
				return fmt.Errorf("updating compatibility control-plane config: %w", err)
			}
			if _, err := tx.UpdateCluster(ctx, &ensured); err != nil {
				return fmt.Errorf("updating compatibility cluster %s: %w", name, err)
			}
			if ensured.Conditions.Provisioned.Status != db.ConditionStatusTrue && ensured.Conditions.Deleting.Status != db.ConditionStatusTrue {
				// FIXME: POC: NKECluster ownership should get a real desired-state control-plane lifecycle instead of replaying the legacy outbox.
				workflowID := fmt.Sprintf("%s%s-nkecluster-%d", constants.ProvisionClusterWorkflowPrefix, ensured.ID.String(), cluster.Generation)
				if _, err := tx.CreateWorkflowOutboxEntry(ctx, "ProvisionCluster", workflowID, map[string]interface{}{"clusterID": ensured.ID}); err != nil {
					return fmt.Errorf("creating ProvisionCluster outbox entry: %w", err)
				}
			}
			return nil
		}

		// Cluster does not exist. Create it
		created, err := tx.CreateCluster(ctx, &desired)
		if err != nil {
			return fmt.Errorf("creating compatibility cluster %s: %w", name, err)
		}

		cpSpec := db.ConfigSpecFromFull(created.ControlPlaneConfig)
		if err := db.CreateControlPlaneConfigRowIfChanged(ctx, tx, &created, cpSpec, db.CreateControlPlaneConfigRowOpts{
			ChangeReason: "NKECluster create controlPlaneClass",
		}); err != nil {
			return fmt.Errorf("creating compatibility control-plane config: %w", err)
		}
		if _, err := tx.UpdateCluster(ctx, &created); err != nil {
			return fmt.Errorf("persisting compatibility control-plane config: %w", err)
		}

		workflowID := constants.ProvisionClusterWorkflowPrefix + created.ID.String()
		if _, err := tx.CreateWorkflowOutboxEntry(ctx, "ProvisionCluster", workflowID, map[string]interface{}{"clusterID": created.ID}); err != nil {
			return fmt.Errorf("creating ProvisionCluster outbox entry: %w", err)
		}
		ensured = created
		return nil
	}); err != nil {
		return db.Cluster{}, err
	}
	c.logger.Debug("ensured compatibility db.Cluster for NKECluster", "nkeCluster", cluster.Name, "clusterID", ensured.ID)
	return ensured, nil
}

func applyDBClusterProjection(existing *db.Cluster, desired db.Cluster) {
	existing.CniPlugin = desired.CniPlugin
	existing.CniVersion = desired.CniVersion
	existing.KubernetesVersion = desired.KubernetesVersion
	existing.OIDCAuthEnabled = desired.OIDCAuthEnabled
	existing.PodCIDR = desired.PodCIDR
	existing.ServiceCIDR = desired.ServiceCIDR
	existing.ControlPlaneConfig = desired.ControlPlaneConfig
	if desired.Nico != nil {
		existing.SetNicoProvider(*desired.Nico)
	}
	if desired.Capi != nil {
		existing.SetCAPIProvider(*desired.Capi)
	}
}

func (c *Controller) applyNodePool(
	ctx context.Context,
	cluster enginev1.NKECluster,
	pool enginev1.NodePool,
	machineClass enginev1.MachineClass,
	kubernetesVersion string,
	nodePoolRelease *enginev1.NodePoolRelease,
	machineImage *enginev1.MachineImage,
) error {
	name := capibuild.MachineDeploymentName(cluster.Name, string(pool.Name))
	version := normalizeVersion(kubernetesVersion)
	labels := machineLabels(cluster.Name, string(pool.Name))

	infraTemplate, err := c.buildNicoMachineTemplate(cluster, pool, machineClass, machineImage, name, labels)
	if err != nil {
		return err
	}
	// FIXME: POC: release MachineInit-to-CABPK composition needs a real product contract.
	releaseBootstrap, err := c.renderReleaseBootstrapForCAPI(ctx, cluster, version, nodePoolRelease.Spec.MachineInit.UserDataTemplate, name)
	if err != nil {
		return fmt.Errorf("rendering worker release bootstrap from NodePoolRelease %s: %w", nodePoolRelease.Name, err)
	}
	bootstrapTemplate, err := buildKubeadmConfigTemplate(cluster, machineClass, releaseBootstrap, name, labels)
	if err != nil {
		return err
	}
	machineDeployment, err := buildMachineDeployment(cluster, pool, name, infraTemplate.Name, bootstrapTemplate.Name, version, labels)
	if err != nil {
		return err
	}

	for _, object := range []client.Object{infraTemplate, bootstrapTemplate, machineDeployment} {
		if err := c.k8s.Patch(ctx, object, client.Apply, client.FieldOwner(c.cfg.FieldManagerName), client.ForceOwnership); err != nil {
			return fmt.Errorf("applying %s %s: %w", object.GetObjectKind().GroupVersionKind().Kind, object.GetName(), err)
		}
	}
	c.logger.Debug("applied tenant NKECluster node pool", "nkeCluster", cluster.Name, "nodePool", pool.Name, "machineDeployment", machineDeployment.Name)
	return nil
}

func (c *Controller) renderReleaseBootstrapForCAPI(ctx context.Context, cluster enginev1.NKECluster, kubernetesVersion, userDataTemplate, hostname string) (capibuild.BootstrapProfile, error) {
	dbCluster := db.Cluster{
		Name:       cluster.Name,
		CniPlugin:  db.CNICalico,
		CniVersion: db.EffectiveCNIVersion(db.CNICalico, ""),
	}
	return capibuild.RenderBootstrapProfileForCAPI(ctx, userDataTemplate, capibuild.BootstrapDataInput{
		Config:                  c.cfg,
		Hostname:                hostname,
		SpiffeEnabled:           c.cfg.SpiffeEnabled,
		TrustDomain:             c.trustDomain,
		DiskSelectionStrategy:   db.DiskSelectionStrategy(pocDiskSelectionStrategy),
		KubernetesVersion:       kubernetesVersion,
		Cluster:                 dbCluster,
		IncludeSpireTrustBundle: false,
	})
}

func (c *Controller) buildNicoMachineTemplate(
	cluster enginev1.NKECluster,
	pool enginev1.NodePool,
	machineClass enginev1.MachineClass,
	machineImage *enginev1.MachineImage,
	baseName string,
	labels map[string]string,
) (*nicov1.NicoMachineTemplate, error) {
	ipxeScript, err := capibuild.RenderIPXEScript(machineImage.Spec.NICo.IPXEScript, pocDiskSelectionStrategy)
	if err != nil {
		return nil, fmt.Errorf("rendering iPXE script from MachineImage %s: %w", machineImage.Name, err)
	}
	interfaces, err := nicoMachineInterfaces(pool)
	if err != nil {
		return nil, err
	}
	instanceTypeID, err := nicoInstanceTypeID(pool, machineClass)
	if err != nil {
		return nil, err
	}
	spec := nicov1.NicoMachineSpec{
		VPCID:                   cluster.Spec.Network.VPCID,
		Interfaces:              interfaces,
		InstanceTypeID:          instanceTypeID,
		IpxeScript:              ipxeScript,
		CloudInitInjectHostname: true,
		Labels:                  labels,
	}
	name, err := templateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming NicoMachineTemplate: %w", err)
	}
	return &nicov1.NicoMachineTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoMachineTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: nicov1.NicoMachineTemplateSpec{
			Template: nicov1.NicoMachineTemplateResource{Spec: spec},
		},
	}, nil
}

func buildKubeadmConfigTemplate(
	cluster enginev1.NKECluster,
	machineClass enginev1.MachineClass,
	releaseBootstrap capibuild.BootstrapProfile,
	baseName string,
	labels map[string]string,
) (*bootstrapv1.KubeadmConfigTemplate, error) {
	patch := kubeadmconfig.KubeletConfigurationPatch(machineClass.Spec.Kubelet.SystemReservedMemory, machineClass.Spec.Kubelet.KubeReservedMemory)
	files := make([]bootstrapv1.File, 0, len(releaseBootstrap.Files)+2)
	for _, file := range releaseBootstrap.Files {
		if file.Path == constants.CAPISpireTrustBundlePath {
			continue
		}
		// FIXME: POC: CABPK worker bootstrap needs a first-class SPIRE path-selector contract.
		content := file.Content
		if file.Path == constants.CAPISpireAgentConfigPath {
			content = ensureSpireUnixPathDiscovery(content)
		}
		files = append(files, bootstrapv1.File{
			Path:        file.Path,
			Owner:       file.Owner,
			Permissions: file.Permissions,
			Encoding:    bootstrapv1.Encoding(file.Encoding),
			Content:     content,
		})
	}
	files = append(files,
		bootstrapv1.File{
			Path:        patch.Path,
			Owner:       patch.Owner,
			Permissions: fmt.Sprintf("%04o", patch.Mode),
			Content:     string(patch.Content),
		},
		bootstrapv1.File{
			Path:        constants.CAPISpireTrustBundlePath,
			Owner:       "root:root",
			Permissions: "0644",
			ContentFrom: bootstrapv1.FileSource{
				Secret: bootstrapv1.SecretFileSource{
					Name: constants.CAPISpireTrustBundleObjectName,
					Key:  constants.CAPISpireTrustBundleSecretKey,
				},
			},
		},
		bootstrapv1.File{
			Path:        pocSpireAgentPathSelectorDropInPath,
			Owner:       "root:root",
			Permissions: "0644",
			Content:     pocSpireAgentPathSelectorDropInContent,
		},
	)
	spec := bootstrapv1.KubeadmConfigSpec{
		Files: files,
		PreKubeadmCommands: insertBeforeCommandContaining(
			releaseBootstrap.PreKubeadmCommands,
			pocSpireExecAuthInstallCommand,
			"systemctl restart spire-agent.service",
		),
		JoinConfiguration: bootstrapv1.JoinConfiguration{
			NodeRegistration: bootstrapv1.NodeRegistrationOptions{
				CRISocket: workerCRISocket,
			},
			Discovery: bootstrapv1.Discovery{
				File: bootstrapv1.FileDiscovery{
					KubeConfigPath: constants.CAPIDiscoveryKubeconfigPath,
					KubeConfig: bootstrapv1.FileDiscoveryKubeConfig{
						User: bootstrapv1.KubeConfigUser{
							Exec: bootstrapv1.KubeConfigAuthExec{
								APIVersion: constants.CAPISpireExecPluginAPIVersion,
								Command:    constants.CAPISpireExecPluginCommand,
								Env: []bootstrapv1.KubeConfigAuthExecEnv{
									{Name: "SPIFFE_ENDPOINT_SOCKET", Value: constants.CAPIWorkerSpiffeEndpointSocket},
									{Name: "SPIFFE_JWT_AUDIENCE", Value: cluster.Name},
									{Name: "SPIFFE_JWT_HINT", Value: constants.CAPIWorkerJoinSpiffeHint},
								},
							},
						},
					},
				},
			},
			Patches: bootstrapv1.Patches{Directory: path.Dir(patch.Path)},
		},
	}
	name, err := templateName(baseName, spec)
	if err != nil {
		return nil, fmt.Errorf("naming KubeadmConfigTemplate: %w", err)
	}
	return &bootstrapv1.KubeadmConfigTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: bootstrapv1.GroupVersion.String(),
			Kind:       "KubeadmConfigTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: bootstrapv1.KubeadmConfigTemplateSpec{
			Template: bootstrapv1.KubeadmConfigTemplateResource{Spec: spec},
		},
	}, nil
}

func ensureSpireUnixPathDiscovery(content string) string {
	if strings.Contains(content, "discover_workload_path = true") {
		return content
	}
	return strings.Replace(content, "WorkloadAttestor \"unix\" {\n        plugin_data {}", "WorkloadAttestor \"unix\" {\n        plugin_data {\n            discover_workload_path = true\n        }", 1)
}

func insertBeforeCommandContaining(commands []string, command, needle string) []string {
	result := make([]string, 0, len(commands)+1)
	inserted := false
	for _, existing := range commands {
		if !inserted && strings.Contains(existing, needle) {
			result = append(result, command)
			inserted = true
		}
		result = append(result, existing)
	}
	if !inserted {
		result = append(result, command)
	}
	return result
}

func buildMachineDeployment(cluster enginev1.NKECluster, pool enginev1.NodePool, name, infraTemplateName, bootstrapTemplateName, kubernetesVersion string, labels map[string]string) (*clusterv1.MachineDeployment, error) {
	if pool.Replicas == nil {
		return nil, fmt.Errorf("replicas is nil")
	}
	replicas := *pool.Replicas
	return &clusterv1.MachineDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "MachineDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: constants.NamespaceNKESystem,
			Labels:    labels,
		},
		Spec: clusterv1.MachineDeploymentSpec{
			ClusterName: cluster.Name,
			Replicas:    &replicas,
			Deletion: clusterv1.MachineDeploymentDeletionSpec{
				Order: clusterv1.OldestMachineSetDeletionOrder,
			},
			Rollout: clusterv1.MachineDeploymentRolloutSpec{
				Strategy: clusterv1.MachineDeploymentRolloutStrategy{
					Type: clusterv1.OnDeleteMachineDeploymentStrategyType,
				},
			},
			Selector: metav1.LabelSelector{MatchLabels: labels},
			Template: clusterv1.MachineTemplateSpec{
				ObjectMeta: clusterv1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						constants.MachineAnnotationKeyDisruptionAuthorizationHook: "",
					},
				},
				Spec: clusterv1.MachineSpec{
					ClusterName: cluster.Name,
					Version:     kubernetesVersion,
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							APIGroup: bootstrapv1.GroupVersion.Group,
							Kind:     "KubeadmConfigTemplate",
							Name:     bootstrapTemplateName,
						},
					},
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{
						APIGroup: nicov1.GroupVersion.Group,
						Kind:     "NicoMachineTemplate",
						Name:     infraTemplateName,
					},
				},
			},
		},
	}, nil
}

func dbClusterFromNKECluster(
	ctx context.Context,
	cfg config.Config,
	cluster enginev1.NKECluster,
	controlPlaneClass enginev1.ControlPlaneClass,
	machineClass enginev1.MachineClass,
	kubernetesVersion string,
	clusterRelease *enginev1.ClusterRelease,
	machineImage *enginev1.MachineImage,
) (db.Cluster, error) {
	version := strings.TrimPrefix(normalizeVersion(kubernetesVersion), "v")
	dbCluster := db.Cluster{
		Name:              cluster.Name,
		CniPlugin:         db.CNICalico,
		CniVersion:        db.EffectiveCNIVersion(db.CNICalico, ""),
		KubernetesVersion: version,
		OIDCAuthEnabled:   cfg.NkeOIDCAuth.Enabled,
		PodCIDR:           firstCIDR(cluster.Spec.ClusterNetwork.Pods.CIDRBlocks, "192.168.0.0/16"),
		ServiceCIDR:       firstCIDR(cluster.Spec.ClusterNetwork.Services.CIDRBlocks, "10.96.0.0/12"),
		ControlPlaneConfig: db.ControlPlaneConfig{
			DesiredCount: controlPlaneClass.Spec.Replicas.Default,
			MaxSurge:     controlPlaneClass.Spec.Replicas.MaxSurge,
		},
		Conditions: db.ClusterConditions{
			Provisioning:          db.NewCondition(true, "Starting provisioning from NKECluster"),
			Provisioned:           db.NewCondition(false, "Not yet provisioned"),
			Deleting:              db.NewCondition(false, "Cluster is not being deleted"),
			ControlPlaneUpgrading: db.NewCondition(false, "No upgrade in progress"),
		},
	}
	cpSpec := db.ControlPlaneConfigSpec{
		KubernetesVersion:    version,
		SystemReservedMemory: machineClass.Spec.Kubelet.SystemReservedMemory,
		KubeReservedMemory:   machineClass.Spec.Kubelet.KubeReservedMemory,
	}
	capiProvider, err := capiControlPlaneProviderFromClass(ctx, cfg, cluster, controlPlaneClass, machineClass, version, clusterRelease, machineImage, dbCluster)
	if err != nil {
		return db.Cluster{}, err
	}
	cpSpec.SetCapi(capiProvider)
	dbCluster.ControlPlaneConfig.ControlPlaneConfigSpec = cpSpec
	capiClusterProvider, err := capiClusterProviderFromNKECluster(cluster)
	if err != nil {
		return db.Cluster{}, err
	}
	dbCluster.SetCAPIProvider(capiClusterProvider)
	return dbCluster, nil
}

func capiClusterProviderFromNKECluster(cluster enginev1.NKECluster) (db.CAPIClusterProvider, error) {
	if _, err := uuid.Parse(cluster.Spec.Network.VPCID); err != nil {
		return db.CAPIClusterProvider{}, fmt.Errorf("parsing NKECluster network vpcID %q: %w", cluster.Spec.Network.VPCID, err)
	}
	// FIXME: POC: the tenant-facing API still needs a provider-neutral placement shape; this reuses the nico-vms site identity for now.
	infraCluster, err := json.Marshal(&nicov1.NicoCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: constants.NamespaceNKESystem,
		},
		Spec: nicov1.NicoClusterSpec{
			SiteID:                pocNicoSiteID,
			FailureDomainLabelKey: "failure_domain",
		},
	})
	if err != nil {
		return db.CAPIClusterProvider{}, fmt.Errorf("encoding NicoCluster scaffold: %w", err)
	}
	return db.CAPIClusterProvider{InfraCluster: infraCluster}, nil
}

func nicoInstanceTypeID(pool enginev1.NodePool, machineClass enginev1.MachineClass) (string, error) {
	if machineClass.Spec.NICo != nil && machineClass.Spec.NICo.InstanceTypeID != "" {
		return machineClass.Spec.NICo.InstanceTypeID, nil
	}
	if pool.InstanceType != "" {
		return string(pool.InstanceType), nil
	}
	return "", fmt.Errorf("node pool %s must select a MachineClass with spec.nico.instanceTypeID or set instanceType", pool.Name)
}

func nicoMachineInterfaces(pool enginev1.NodePool) ([]nicov1.NicoMachineInterface, error) {
	switch {
	case pool.VPCSubnetID != "":
		return []nicov1.NicoMachineInterface{{SubnetID: pool.VPCSubnetID}}, nil
	case pool.VPCPrefixID != "":
		return []nicov1.NicoMachineInterface{{VPCPrefixID: pool.VPCPrefixID}}, nil
	default:
		return nil, fmt.Errorf("node pool %s must set vpcSubnetID or vpcPrefixID", pool.Name)
	}
}

func kubernetesVersionFromRelease(release metav1.Object, selectedVersion string) (string, error) {
	version, injected, err := machinerelease.ParseKubernetesVersionAnnotations(release)
	if err != nil {
		return "", err
	}
	if !injected {
		return version, nil
	}
	if strings.TrimSpace(selectedVersion) == "" {
		return "", fmt.Errorf("release %s accepts an injected Kubernetes version, but NKECluster did not select one", release.GetName())
	}
	return strings.TrimPrefix(selectedVersion, "v"), nil
}

func releaseUnavailable(err error) bool {
	return errors.Is(err, machinerelease.ErrInvalidRelease) && strings.Contains(err.Error(), " is not installed")
}

func capiControlPlaneProviderFromClass(
	ctx context.Context,
	cfg config.Config,
	cluster enginev1.NKECluster,
	controlPlaneClass enginev1.ControlPlaneClass,
	machineClass enginev1.MachineClass,
	kubernetesVersion string,
	clusterRelease *enginev1.ClusterRelease,
	machineImage *enginev1.MachineImage,
	dbCluster db.Cluster,
) (db.CAPIControlPlaneProvider, error) {
	if controlPlaneClass.Spec.Replicas.Default < 1 {
		return db.CAPIControlPlaneProvider{}, fmt.Errorf("ControlPlaneClass %s replicas.default must be at least 1", controlPlaneClass.Name)
	}
	ipxeScript, err := capibuild.RenderIPXEScript(machineImage.Spec.NICo.IPXEScript, pocDiskSelectionStrategy)
	if err != nil {
		return db.CAPIControlPlaneProvider{}, fmt.Errorf("rendering control-plane iPXE script from MachineImage %s: %w", machineImage.Name, err)
	}

	infraTemplates := make([]json.RawMessage, 0, len(controlPlaneClass.Spec.Machine.InfrastructureTemplatePreferences))
	for i, template := range controlPlaneClass.Spec.Machine.InfrastructureTemplatePreferences {
		if len(template.Raw) == 0 {
			return db.CAPIControlPlaneProvider{}, fmt.Errorf("ControlPlaneClass %s infrastructureTemplatePreferences[%d] is empty", controlPlaneClass.Name, i)
		}
		templateWithMachineClass, err := controlPlaneTemplateWithMachineClass(template.Raw, machineClass, ipxeScript)
		if err != nil {
			return db.CAPIControlPlaneProvider{}, fmt.Errorf("ControlPlaneClass %s infrastructureTemplatePreferences[%d]: %w", controlPlaneClass.Name, i, err)
		}
		infraTemplates = append(infraTemplates, templateWithMachineClass)
	}

	// FIXME: POC: CAPI-template control-plane bootstrap should have a first-class contract for how release machineInit composes with class bootstrap.
	releaseBootstrap, err := capibuild.RenderBootstrapProfileForCAPI(ctx, clusterRelease.Spec.MachineInit.UserDataTemplate, capibuild.BootstrapDataInput{
		Config:                  cfg,
		Hostname:                cluster.Name + "-control-plane",
		SpiffeEnabled:           false,
		DiskSelectionStrategy:   db.DiskSelectionStrategy(pocDiskSelectionStrategy),
		KubernetesVersion:       kubernetesVersion,
		Cluster:                 dbCluster,
		IncludeSpireTrustBundle: false,
	})
	if err != nil {
		return db.CAPIControlPlaneProvider{}, fmt.Errorf("rendering control-plane release bootstrap from ClusterRelease %s: %w", clusterRelease.Name, err)
	}

	files := make([]db.CloudInitFile, 0, len(releaseBootstrap.Files)+len(controlPlaneClass.Spec.Bootstrap.Files))
	files = append(files, releaseBootstrap.Files...)
	for _, file := range controlPlaneClass.Spec.Bootstrap.Files {
		files = append(files, db.CloudInitFile{
			Path:        file.Path,
			Content:     file.Content,
			Owner:       file.Owner,
			Permissions: file.Permissions,
			Encoding:    file.Encoding,
		})
	}

	return db.CAPIControlPlaneProvider{
		InfraMachineTemplatePreferences: infraTemplates,
		SpireAttestationMethod:          db.SpireAttestationMethod(controlPlaneClass.Spec.Machine.SpireAttestationMethod),
		BootstrapFiles:                  files,
		PreKubeadmCommands:              append(releaseBootstrap.PreKubeadmCommands, controlPlaneClass.Spec.Bootstrap.PreKubeadmCommands...),
	}, nil
}

func controlPlaneTemplateWithMachineClass(raw []byte, machineClass enginev1.MachineClass, ipxeScript string) (json.RawMessage, error) {
	var template nicov1.NicoMachineTemplate
	if err := json.Unmarshal(raw, &template); err != nil {
		return nil, fmt.Errorf("decoding NicoMachineTemplate: %w", err)
	}
	if template.Kind != "" && template.Kind != "NicoMachineTemplate" {
		return nil, fmt.Errorf("expected NicoMachineTemplate, got %s", template.Kind)
	}
	template.APIVersion = nicov1.GroupVersion.String()
	template.Kind = "NicoMachineTemplate"
	template.Spec.Template.Spec.IpxeScript = ipxeScript
	if machineClass.Spec.NICo != nil && machineClass.Spec.NICo.InstanceTypeID != "" {
		template.Spec.Template.Spec.InstanceTypeID = machineClass.Spec.NICo.InstanceTypeID
	}
	out, err := json.Marshal(template)
	if err != nil {
		return nil, fmt.Errorf("encoding NicoMachineTemplate: %w", err)
	}
	return json.RawMessage(out), nil
}

func firstCIDR(cidrs []enginev1.NetworkCIDR, fallback string) string {
	if len(cidrs) == 0 {
		return fallback
	}
	return string(cidrs[0])
}

func templateName(base string, spec any) (string, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshaling template spec: %w", err)
	}
	sum := sha256.Sum256(payload)
	suffix := hex.EncodeToString(sum[:])[:templateHashLength]
	maxBaseLength := 63 - len(suffix) - 1
	if len(base) > maxBaseLength {
		base = strings.TrimRight(base[:maxBaseLength], "-")
	}
	return fmt.Sprintf("%s-%s", base, suffix), nil
}

func machineLabels(clusterName, nodePoolName string) map[string]string {
	return map[string]string{
		clusterv1.ClusterNameLabel: clusterName,
		labelNKECluster:            clusterName,
		labelNodePool:              nodePoolName,
	}
}

func normalizeVersion(version string) string {
	if version == "" {
		return "v0.0.0"
	}
	if strings.HasPrefix(version, "v") {
		return version
	}
	return "v" + version
}
