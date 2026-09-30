// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tenantaccess

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

// upsertCAPISecret upserts one Opaque Secret whose name and label satisfy a CAPI
// lookup contract. The kubeconfigs and the cluster CA differ only in name, data and
// kubeconfig-mode.
//
// Secret Type is Opaque, not clusterv1.ClusterSecretType by design.
// Nothing resolving these reads the type, CAPI finds a kubeconfig and CABPK finds a CA by
// name and data key, while ClusterSecretType is CAPI's marker for a Secret its own components
// generated, and is what KCP (if ever introduced) matches on to seize the controller reference.
func (c *Controller) upsertCAPISecret(ctx context.Context, cluster db.Cluster, name string, data map[string][]byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: SecretsNamespace,
			Labels: map[string]string{
				clusterv1.ClusterNameLabel: cluster.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	return c.upsertSecret(ctx, cluster, secret)
}

// upsertSecret publishes one Controller-owned Secret.
//
// The live row is re-read here, immediately before the write, and not only once
// per publish: on the OIDC path publishSPIRE applies RBAC into the tenant
// cluster between the publish-time refresh and these writes, and that apply is
// bounded by tenantRequestTimeout, not by anything quick. A cluster that started
// deleting during it would otherwise have its Secrets re-created after teardown
// had already removed them, orphaning them for good once the row is gone.
func (c *Controller) upsertSecret(ctx context.Context, cluster db.Cluster, desired *corev1.Secret) error {
	live, skip, err := c.refreshCluster(ctx, cluster)
	if err != nil || skip {
		return err
	}

	capiCluster := &clusterv1.Cluster{}
	key := types.NamespacedName{Namespace: SecretsNamespace, Name: live.Name}
	if err := c.k8sClient.Get(ctx, key, capiCluster); err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Debug("CAPI cluster no longer exists; skipping tenant-access publish", "clusterID", live.ID, "clusterName", live.Name)
			return nil
		}
		return fmt.Errorf("getting CAPI cluster %s before publish: %w", key, err)
	}

	if err := setSecretOwnership(desired, live, capiCluster, c.cfg.ManagedByLabelValue, c.scheme); err != nil {
		return err
	}

	return upsertOwnedSecret(ctx, c.k8sClient, c.logger, live, desired)
}

func upsertOwnedSecret(
	ctx context.Context,
	k8sClient client.Client,
	logger *slog.Logger,
	cluster db.Cluster,
	desired *corev1.Secret,
) error {
	existing := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting secret %s/%s before upsert: %w", desired.Namespace, desired.Name, err)
	} else if err == nil && existing.Type != desired.Type {
		logger.Info(
			"Replacing tenant-access secret with incompatible type",
			"clusterName", cluster.Name,
			"secret", desired.Name,
			"existingType", existing.Type,
			"desiredType", desired.Type,
		)
		deleteOptions := &client.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &existing.UID},
		}
		if err := k8sClient.Delete(ctx, existing, deleteOptions); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting secret %s/%s with incompatible type: %w", desired.Namespace, desired.Name, err)
		}
	}

	secret := &corev1.Secret{
		ObjectMeta: desired.ObjectMeta,
	}

	op, err := controllerutil.CreateOrUpdate(ctx, k8sClient, secret, func() error {
		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		for key, value := range desired.Labels {
			secret.Labels[key] = value
		}
		secret.OwnerReferences = desired.OwnerReferences
		secret.Type = desired.Type
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		for key, value := range desired.Data {
			secret.Data[key] = value
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("upserting secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}

	switch op {
	case controllerutil.OperationResultCreated:
		logger.Info("Tenant-access secret created", "clusterName", cluster.Name, "secret", desired.Name)
	case controllerutil.OperationResultNone:
	case controllerutil.OperationResultUpdated,
		controllerutil.OperationResultUpdatedStatus,
		controllerutil.OperationResultUpdatedStatusOnly:
		logger.Debug("Tenant-access secret updated", "clusterName", cluster.Name, "secret", desired.Name, "operation", string(op))
	}
	return nil
}

func setSecretOwnership(secret *corev1.Secret, cluster db.Cluster, capiCluster *clusterv1.Cluster, managedByLabelValue string, scheme *runtime.Scheme) error {
	if secret.Labels == nil {
		secret.Labels = map[string]string{}
	}
	secret.Labels[constants.LabelKeyNkeManagedBy] = managedByLabelValue
	secret.Labels[constants.LabelKeyClusterID] = cluster.ID.String()

	if err := controllerutil.SetControllerReference(capiCluster, secret, scheme); err != nil {
		return fmt.Errorf("setting owner reference on secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}

	return nil
}
