// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package noderemediation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/temporal/workflows"
)

func (r *Reconciler) remediateMachineDeploymentWorker(ctx context.Context, kubeNode *corev1.Node, machine *clusterv1.Machine) (reconcile.Result, error) {
	// Terminate requested via terminate label is explicit approval for remediation.
	reason, terminateLabelPresent := kubeNode.GetLabels()[constants.LabelKeyNodeLabelTerminate]
	if terminateLabelPresent {
		return r.terminateMachine(ctx, kubeNode, machine, reason)
	}

	// If node-signal disruption authorization is disabled, the NeedsRecycle node condition should terminate the Machine.
	if !r.cfg.NodeSignalDisruptionAuthorizationEnabled {
		if needsRecycle, reason := hasNeedsRecycleNodeCondition(kubeNode); needsRecycle {
			return r.terminateMachine(ctx, kubeNode, machine, reason)
		}
	}

	reason, rebootLabelPresent := kubeNode.GetLabels()[constants.LabelKeyNodeLabelReboot]
	if rebootLabelPresent {
		return r.rebootMachine(ctx, kubeNode, machine, reason)
	}

	// No remediation requested, nothing to do.
	return reconcile.Result{}, nil
}

func hasNeedsRecycleNodeCondition(kubeNode *corev1.Node) (bool, string) {
	for _, condition := range kubeNode.Status.Conditions {
		if condition.Type != corev1.NodeConditionType(constants.NodeConditionTypeNeedsRecycle) || condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Message != "" {
			return true, condition.Message
		}
		return true, constants.NodeConditionTypeNeedsRecycle
	}
	return false, ""
}

func (r *Reconciler) terminateMachine(ctx context.Context, kubeNode *corev1.Node, machine *clusterv1.Machine, reason string) (reconcile.Result, error) {
	if !machine.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	machineHealthIssue, err := getMachineHealthIssueAnnotationValue(kubeNode)
	if err != nil {
		return reconcile.Result{}, err
	}

	if machineHealthIssue != nil {
		r.logger.Info("Machine health issue reported", "annotation", *machineHealthIssue, "machine", machine.Name, "nodeName", kubeNode.Name)
		if err := r.applyMachineAnnotation(ctx, machine, constants.MachineAnnotationKeyMachineHealthIssue, *machineHealthIssue); err != nil {
			return reconcile.Result{}, fmt.Errorf("applying machine health issue annotation to Machine %s/%s: %w", machine.Namespace, machine.Name, err)
		}
	}

	if err := r.k8sClient.Delete(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, fmt.Errorf("deleting Machine %s/%s for tenant Node terminate label %q: %w", machine.Namespace, machine.Name, reason, err)
	}

	r.logger.Info("Started worker Machine deletion", "machine", machine.Name, "nodeName", kubeNode.Name, "reason", reason, "nvidiaLabels", nvidiaLabels(kubeNode.GetLabels()))
	return reconcile.Result{}, nil
}

func (r *Reconciler) rebootMachine(ctx context.Context, kubeNode *corev1.Node, machine *clusterv1.Machine, reason string) (reconcile.Result, error) {
	now := time.Now().UTC()

	err := r.applyMachineAnnotation(ctx, machine, constants.MachineAnnotationKeyReboot, now.Format(time.RFC3339))
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("applying reboot annotation to Machine %s/%s for tenant Node reboot label %q: %w", machine.Namespace, machine.Name, reason, err)
	}
	if err := r.markTenantNodeCAPIRebootStarted(ctx, kubeNode, constants.FormatTimeForK8sLabel(now)); err != nil {
		return reconcile.Result{}, fmt.Errorf("marking tenant Node %s reboot started: %w", kubeNode.Name, err)
	}

	r.logger.Info("Applied worker Machine reboot annotation", "machine", machine.Name, "nodeName", kubeNode.Name, "reason", reason, "nvidiaLabels", nvidiaLabels(kubeNode.GetLabels()))
	return reconcile.Result{}, nil
}

func (r *Reconciler) markTenantNodeCAPIRebootStarted(ctx context.Context, kubeNode *corev1.Node, timestamp string) error {
	before := kubeNode.DeepCopy()
	labels := kubeNode.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	delete(labels, constants.LabelKeyNodeLabelReboot)
	// FIXME: POC: This is only async provider-trigger acknowledgement. Full parity with existing reboot labels needs additional logic and/or a Machine reboot controller.
	labels[constants.LabelKeyNodeLabelRebootStarted] = timestamp
	labels[constants.LabelKeyNodeLabelRebootStatus] = string(constants.RebootStatusCAPIRebootStarted)
	labels[constants.LabelKeyNodeLabelRebootStatusTimestamp] = timestamp
	kubeNode.SetLabels(labels)
	return r.client.Patch(ctx, kubeNode, ctrlclient.MergeFrom(before))
}

func (r *Reconciler) applyMachineAnnotation(ctx context.Context, machine *clusterv1.Machine, key, value string) error {
	before := machine.DeepCopy()
	annotations := machine.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[key] = value
	machine.SetAnnotations(annotations)
	return r.k8sClient.Patch(ctx, machine, ctrlclient.MergeFrom(before))
}

func getMachineHealthIssueAnnotationValue(kubeNode *corev1.Node) (*string, error) {
	annotation, ok := kubeNode.GetAnnotations()[constants.NodeAnnotationKeyMachineHealthIssue]
	if !ok {
		return nil, nil
	}
	machineHealthIssue, err := workflows.NewMachineHealthIssueFromJSON([]byte(annotation))
	if err != nil {
		return nil, fmt.Errorf("failed to parse machine health issue annotation: %w", err)
	}
	if machineHealthIssue == nil {
		return nil, nil
	}
	data, err := json.Marshal(machineHealthIssue)
	if err != nil {
		return &machineHealthIssue.Summary, nil
	}
	value := string(data)
	return &value, nil
}
