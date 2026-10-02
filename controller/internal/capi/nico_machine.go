// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"text/template"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	nicov1 "github.com/dsx-ai-factory/cluster-api-provider-nico/api/v1alpha1"
)

// NicoMachineInput contains the provider values used to build a NicoMachine.
type NicoMachineInput struct {
	Name                     string
	Namespace                string
	ClusterName              string
	ControlPlane             bool
	VPCID                    uuid.UUID
	InstanceTypeID           uuid.UUID
	SSHKeyGroupIDs           []uuid.UUID
	SubnetIDs                []uuid.UUID
	VPCPrefixIDs             []uuid.UUID
	InfinibandPartitionID    *uuid.UUID
	NVLinkLogicalPartitionID *uuid.UUID
	IPXEScript               string
	InstanceLabels           map[string]string
}

// BuildNicoMachine builds a NicoMachine from resolved cluster and pool settings.
func BuildNicoMachine(input NicoMachineInput) (*nicov1.NicoMachine, error) {
	interfaces, err := BuildNicoMachineInterfaces(input.SubnetIDs, input.VPCPrefixIDs)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		clusterv1.ClusterNameLabel: input.ClusterName,
	}
	if input.ControlPlane {
		labels[clusterv1.MachineControlPlaneLabel] = ""
	}

	return &nicov1.NicoMachine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nicov1.GroupVersion.String(),
			Kind:       "NicoMachine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      input.Name,
			Namespace: input.Namespace,
			Labels:    labels,
		},
		Spec: nicov1.NicoMachineSpec{
			VPCID:                    input.VPCID.String(),
			InstanceTypeID:           input.InstanceTypeID.String(),
			Interfaces:               interfaces,
			InfinibandPartitionID:    optionalUUIDString(input.InfinibandPartitionID),
			NVLinkLogicalPartitionID: optionalUUIDString(input.NVLinkLogicalPartitionID),
			SSHKeyGroupIDs:           uuidStrings(input.SSHKeyGroupIDs),
			IpxeScript:               input.IPXEScript,
			// CAPNICo adds machine and topology labels after creating the instance.
			Labels:                   input.InstanceLabels,
		},
	}, nil
}

// BuildNicoMachineInterfaces selects the first configured VPC prefix or subnet.
func BuildNicoMachineInterfaces(subnetIDs, vpcPrefixIDs []uuid.UUID) ([]nicov1.NicoMachineInterface, error) {
	if len(vpcPrefixIDs) > 0 {
		return []nicov1.NicoMachineInterface{{VPCPrefixID: vpcPrefixIDs[0].String()}}, nil
	}
	if len(subnetIDs) > 0 {
		return []nicov1.NicoMachineInterface{{SubnetID: subnetIDs[0].String()}}, nil
	}
	return nil, fmt.Errorf("both subnetIDs and vpcPrefixIDs are empty")
}

// RenderIPXEScriptFromProfile loads and renders the iPXE template in an OS profile.
func RenderIPXEScriptFromProfile(osProfileDir, diskSelectionStrategy string) (string, error) {
	script, err := os.ReadFile(path.Join(osProfileDir, "ipxeScript"))
	if err != nil {
		return "", fmt.Errorf("reading iPXE script: %w", err)
	}
	return RenderIPXEScript(string(script), diskSelectionStrategy)
}

// RenderIPXEScript renders an iPXE template with the selected disk strategy.
func RenderIPXEScript(scriptTemplate, diskSelectionStrategy string) (string, error) {
	tmpl, err := template.New("ipxeScript").Parse(scriptTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing iPXE script: %w", err)
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, struct{ DiskSelectionStrategy string }{diskSelectionStrategy}); err != nil {
		return "", fmt.Errorf("rendering iPXE script: %w", err)
	}
	return rendered.String(), nil
}

func optionalUUIDString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func uuidStrings(ids []uuid.UUID) []string {
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, id.String())
	}
	return result
}
