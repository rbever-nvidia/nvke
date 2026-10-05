// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package naming

import (
	"fmt"
	"slices"
	"strings"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	// MaxGeneratedNodeNameLength is the Kubernetes limit for Node object names.
	MaxGeneratedNodeNameLength = 63
	// NodeNameSuffixLength is the width of the decimal suffix CWE assigns (%04d).
	NodeNameSuffixLength = 4
	// NodeNameSuffixModulo is the exclusive upper bound for random suffix selection (Intn).
	NodeNameSuffixModulo   = 10000
	nodeNameSeparatorCount = 2

	capiRandomSuffixLength = 5
	capiGeneratedSuffixes  = 2

	// GeneratedNodeNameFormat documents the CWE node naming contract for error messages.
	GeneratedNodeNameFormat = "<cluster name>-<nodeNamePrefix>-<4-digit-suffix>"
	// GeneratedMachineNameFormat documents the CAPI worker Machine naming contract.
	GeneratedMachineNameFormat = "<cluster name>-<node pool name>-<machineset suffix>-<machine suffix>"
	// formatNodeNameTemplate is the template for FormatNodeName.
	formatNodeNameTemplate = "%s-%s-%0*d"
)

// FormatNodeName formats a node name as {cluster}-{prefix}-{suffix}.
// Example: "mycluster-cp-0123"
func FormatNodeName(clusterName, prefix string, suffix int) string {
	return fmt.Sprintf(formatNodeNameTemplate, clusterName, prefix, NodeNameSuffixLength, suffix)
}

// GenerateNodeName returns a unique node name using a random suffix, retrying on collision.
func GenerateNodeName(existingNames []string, clusterName, prefix string, randIntn func(int) int) string {
	for {
		name := FormatNodeName(clusterName, prefix, randIntn(NodeNameSuffixModulo))
		if !slices.Contains(existingNames, name) {
			return name
		}
	}
}

// MaxNodeNamePrefixLen returns the maximum allowed len(nodeNamePrefix) for a cluster name.
func MaxNodeNamePrefixLen(clusterName string) int {
	return MaxGeneratedNodeNameLength - len(clusterName) - nodeNameSeparatorCount - NodeNameSuffixLength
}

// MaxMachineDeploymentNodePoolNameLen returns the longest node pool name that
// leaves room for CAPI's MachineSet and Machine random suffixes.
func MaxMachineDeploymentNodePoolNameLen(clusterName string) int {
	suffixLength := capiGeneratedSuffixes * (1 + capiRandomSuffixLength)
	return MaxGeneratedNodeNameLength - len(clusterName) - 1 - suffixLength
}

// ValidateClusterNameForNodeNaming rejects cluster names that leave no room for a nodeNamePrefix
// in the generated node name format.
func ValidateClusterNameForNodeNaming(clusterName string) error {
	if MaxNodeNamePrefixLen(clusterName) >= 1 {
		return nil
	}
	return fmt.Errorf(
		"cluster name %q is too long (%d chars): node names use format %q and must fit within %d characters",
		clusterName, len(clusterName), GeneratedNodeNameFormat, MaxGeneratedNodeNameLength,
	)
}

// ValidateNodeNamePrefix rejects prefixes that would produce node names longer than 63 characters.
func ValidateNodeNamePrefix(clusterName, prefix string) error {
	if err := ValidateClusterNameForNodeNaming(clusterName); err != nil {
		return err
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return fmt.Errorf("nodeNamePrefix must not be empty")
	}
	maxLen := MaxNodeNamePrefixLen(clusterName)
	if len(prefix) > maxLen {
		// generate a dummy name to check the length
		result := FormatNodeName(clusterName, prefix, 0)
		return fmt.Errorf(
			"nodeNamePrefix %q is too long for cluster %q: generated node names use format %q; max prefix length is %d, but got %d (resulting name %q would be %d characters; limit is %d)",
			prefix, clusterName, GeneratedNodeNameFormat, maxLen, len(prefix), result, len(result), MaxGeneratedNodeNameLength,
		)
	}
	return nil
}

// ValidateNodePoolName rejects names that cannot safely produce CAPI Machine
// names. MachineDeployment worker names are derived from the cluster and node
// pool names, not nodeNamePrefix.
func ValidateNodePoolName(clusterName, name string) error {
	name = strings.TrimSpace(name)
	if errors := k8svalidation.IsDNS1123Label(name); len(errors) > 0 {
		return fmt.Errorf("node pool name %q must be a DNS label: %s", name, strings.Join(errors, "; "))
	}
	machineDeploymentName := fmt.Sprintf("%s-%s", clusterName, name)
	if errors := k8svalidation.IsDNS1123Label(machineDeploymentName); len(errors) > 0 {
		return fmt.Errorf(
			"cluster and node pool names produce invalid MachineDeployment name %q: %s",
			machineDeploymentName,
			strings.Join(errors, "; "),
		)
	}

	maxLen := MaxMachineDeploymentNodePoolNameLen(clusterName)
	if maxLen < 1 {
		return fmt.Errorf(
			"cluster name %q is too long: generated Machine names use format %q and must fit within %d characters",
			clusterName,
			GeneratedMachineNameFormat,
			MaxGeneratedNodeNameLength,
		)
	}
	if len(name) > maxLen {
		generatedName := fmt.Sprintf(
			"%s-%s-%s-%s",
			clusterName,
			name,
			strings.Repeat("x", capiRandomSuffixLength),
			strings.Repeat("x", capiRandomSuffixLength),
		)
		return fmt.Errorf(
			"node pool name %q is too long for cluster %q: generated Machine names use format %q; max name length is %d, but got %d (resulting name %q would be %d characters; limit is %d)",
			name,
			clusterName,
			GeneratedMachineNameFormat,
			maxLen,
			len(name),
			generatedName,
			len(generatedName),
			MaxGeneratedNodeNameLength,
		)
	}
	return nil
}
