// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nodeconditions

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
)

// ApplyNeedsRecycle marks a tenant Node as needing recycle.
func ApplyNeedsRecycle(ctx context.Context, tenantClient client.Client, nodeName, message string) error {
	node := &corev1.Node{}
	if err := tenantClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("getting tenant Node %s: %w", nodeName, err)
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle) &&
			condition.Status == corev1.ConditionTrue {
			return nil
		}
	}

	conditionPatch := struct {
		Status struct {
			Conditions []corev1.NodeCondition `json:"conditions"`
		} `json:"status"`
	}{}
	conditionPatch.Status.Conditions = []corev1.NodeCondition{{
		Type:               corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle),
		Status:             corev1.ConditionTrue,
		Reason:             "Deprecated",
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}}
	patch, err := json.Marshal(conditionPatch)
	if err != nil {
		return fmt.Errorf("marshaling Node condition patch: %w", err)
	}

	patchNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
	if err := tenantClient.Status().Patch(
		ctx,
		patchNode,
		client.RawPatch(types.StrategicMergePatchType, patch),
	); err != nil {
		return fmt.Errorf("applying condition %s to Kubernetes Node %s: %w", constants.NodeConditionTypeNeedsRecycle, nodeName, err)
	}
	return nil
}
