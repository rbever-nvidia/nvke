// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tenantaccess

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/nvkerbac"
)

const (
	// rbacConfigMapName is the operator-managed ConfigMap holding the tenant RBAC
	// manifest that gets applied into OIDC tenant clusters.
	rbacConfigMapName = nvkerbac.ConfigMapName
	// rbacConfigMapKey is the ConfigMap data key containing the multi-document
	// tenant RBAC manifest.
	rbacConfigMapKey = nvkerbac.ConfigMapKey
	// rbacFieldManager is the Server-Side Apply field owner used when applying the
	// RBAC manifest into a tenant cluster.
	rbacFieldManager = "nvke"
	// legacyRBACName is the name the pre-rename manifest gave the controller's own
	// Role, ClusterRole and their bindings.
	legacyRBACName = "nke:cwe"
	// legacyBootstrapGroupName is the broad group subject used by the earlier
	// bootstrap RBAC model before per-Machine subjects were added dynamically.
	legacyBootstrapGroupName = "nvke:node-bootstrapper"
)

// legacyRBACObject is a tenant RBAC object an earlier release applied under the
// field manager "nke". Nothing owns it once the manifest is renamed, yet it
// keeps its permissions, so it is deleted explicitly.
type legacyRBACObject struct {
	kind      string
	namespace string
	name      string
}

type bootstrapBindingRef struct {
	name      string
	namespace string
	cluster   bool
}

// legacyRBACObjects returns everything the pre-rename manifest created: the
// nke:cwe set and the nke:capi-cluster-admin binding.
func legacyRBACObjects() []legacyRBACObject {
	return []legacyRBACObject{
		{"ClusterRole", "", legacyRBACName},
		{"ClusterRoleBinding", "", legacyRBACName},
		{"ClusterRoleBinding", "", "nke:capi-cluster-admin"},
		{"Role", "kube-system", legacyRBACName},
		{"RoleBinding", "kube-system", legacyRBACName},
		{"Role", "kube-node-lease", legacyRBACName},
		{"RoleBinding", "kube-node-lease", legacyRBACName},
	}
}

func bootstrapBindingsWithLegacyGroup() []bootstrapBindingRef {
	return []bootstrapBindingRef{
		{name: "nvke:node-bootstrapper", cluster: true},
		{name: "nvke:node-bootstrapper:get-nodes", cluster: true},
		{name: "nvke:node-bootstrapper:kubelet-config", namespace: "kube-system"},
		{name: "nvke:node-bootstrapper:nodes-kubeadm-config", namespace: "kube-system"},
	}
}

// pruneLegacyRBAC deletes the legacy objects the manifest does not itself
// carry, so a site that pins the old tenantRbacName keeps what it applies. Call
// it only after the manifest applied: the new permissions must exist before the
// old ones go. Deleting an absent object is not an error, which makes the prune
// safe to repeat on every reconcile.
func pruneLegacyRBAC(ctx context.Context, c ctrlclient.Client, manifest []byte) error {
	kept := make(map[legacyRBACObject]bool)
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decoding RBAC manifest: %w", err)
		}
		kept[legacyRBACObject{obj.GetKind(), obj.GetNamespace(), obj.GetName()}] = true
	}

	for _, legacy := range legacyRBACObjects() {
		if kept[legacy] {
			continue
		}
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("rbac.authorization.k8s.io/v1")
		obj.SetKind(legacy.kind)
		obj.SetNamespace(legacy.namespace)
		obj.SetName(legacy.name)
		err := c.Delete(ctx, obj)
		if ctrlclient.IgnoreNotFound(err) != nil {
			return fmt.Errorf("pruning legacy %s %s: %w", legacy.kind, legacy.name, err)
		}
	}
	return nil
}

func pruneLegacyBootstrapGroupSubjects(ctx context.Context, c ctrlclient.Client) error {
	for _, binding := range bootstrapBindingsWithLegacyGroup() {
		if binding.cluster {
			if err := pruneLegacyBootstrapGroupFromClusterRoleBinding(ctx, c, binding.name); err != nil {
				return err
			}
			continue
		}
		if err := pruneLegacyBootstrapGroupFromRoleBinding(ctx, c, binding.namespace, binding.name); err != nil {
			return err
		}
	}
	return nil
}

func pruneLegacyBootstrapGroupFromClusterRoleBinding(ctx context.Context, c ctrlclient.Client, name string) error {
	binding := &rbacv1.ClusterRoleBinding{}
	if err := c.Get(ctx, ctrlclient.ObjectKey{Name: name}, binding); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting bootstrap ClusterRoleBinding %s: %w", name, err)
	}
	updatedSubjects, changed := removeLegacyBootstrapGroup(binding.Subjects)
	if !changed {
		return nil
	}
	patchBase := binding.DeepCopy()
	binding.Subjects = updatedSubjects
	if err := c.Patch(ctx, binding, ctrlclient.MergeFrom(patchBase)); err != nil {
		return fmt.Errorf("pruning legacy bootstrap group from ClusterRoleBinding %s: %w", name, err)
	}
	return nil
}

func pruneLegacyBootstrapGroupFromRoleBinding(ctx context.Context, c ctrlclient.Client, namespace, name string) error {
	binding := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, binding); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting bootstrap RoleBinding %s/%s: %w", namespace, name, err)
	}
	updatedSubjects, changed := removeLegacyBootstrapGroup(binding.Subjects)
	if !changed {
		return nil
	}
	patchBase := binding.DeepCopy()
	binding.Subjects = updatedSubjects
	if err := c.Patch(ctx, binding, ctrlclient.MergeFrom(patchBase)); err != nil {
		return fmt.Errorf("pruning legacy bootstrap group from RoleBinding %s/%s: %w", namespace, name, err)
	}
	return nil
}

func removeLegacyBootstrapGroup(subjects []rbacv1.Subject) ([]rbacv1.Subject, bool) {
	updated := subjects[:0]
	changed := false
	for _, subject := range subjects {
		if subject.Kind == rbacv1.GroupKind && subject.Name == legacyBootstrapGroupName {
			changed = true
			continue
		}
		updated = append(updated, subject)
	}
	return updated, changed
}

// readRBACManifest reads the operator-managed nke-rbac ConfigMap from the
// management cluster and returns the multi-document tenant RBAC manifest.
func (c *Controller) readRBACManifest(ctx context.Context) ([]byte, error) {
	return nvkerbac.ReadManifest(ctx, c.k8sClient)
}

// applyManifestSSA splits a multi-document YAML manifest and Server-Side-Applies
// each document into the given client under the supplied field manager.
func applyManifestSSA(ctx context.Context, c ctrlclient.Client, manifest []byte, fieldManager string) error {
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decoding RBAC manifest: %w", err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		// Server-Side Apply each document. The object is decoded straight from the
		// manifest YAML (not converted from a typed API object), so wrapping it with
		// ApplyConfigurationFromUnstructured is safe — there is no ambiguity between
		// an unset field and an explicit zero value.
		if err := c.Apply(
			ctx,
			ctrlclient.ApplyConfigurationFromUnstructured(obj),
			ctrlclient.FieldOwner(fieldManager),
			ctrlclient.ForceOwnership,
		); err != nil {
			return fmt.Errorf("applying %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
}
