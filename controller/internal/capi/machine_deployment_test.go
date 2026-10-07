// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

func TestTranslateWorkerProfilePreKubeadmCommands(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cniPlugin db.CniPlugin
		userData  string
		want      []string
	}{
		"ovn appends openvswitch unit": {
			cniPlugin: db.CNIOvnKubernetes,
			userData:  "runcmd:\n  - echo profile\n",
			want:      []string{"echo profile", workerOpenVSwitchPreKubeadmUnit},
		},
		"calico preserves profile commands": {
			cniPlugin: db.CNICalico,
			userData:  "runcmd:\n  - echo profile\n",
			want:      []string{"echo profile"},
		},
		"ovn with no profile commands": {
			cniPlugin: db.CNIOvnKubernetes,
			userData:  "{}",
			want:      []string{workerOpenVSwitchPreKubeadmUnit},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bootstrap, err := translateWorkerProfile(tc.userData, tc.cniPlugin)

			require.NoError(t, err)
			assert.Equal(t, tc.want, bootstrap.PreKubeadmCommands)
		})
	}
}

func TestBuildWorkerKubeadmConfigTemplate(t *testing.T) {
	t.Parallel()

	template, err := buildWorkerKubeadmConfigTemplate(
		db.Cluster{Name: "cluster-1", CniPlugin: db.CNIOvnKubernetes},
		db.NodePool{},
		"cluster-1-workers",
		"v1.34.2",
		workerBootstrap{PreKubeadmCommands: workerPreKubeadmCommands(db.CNIOvnKubernetes, []string{"echo profile"})},
	)

	require.NoError(t, err)
	spec := template.Spec.Template.Spec
	assert.Equal(t, []string{"echo profile", workerOpenVSwitchPreKubeadmUnit}, spec.PreKubeadmCommands)
	assert.Nil(t, spec.JoinConfiguration.NodeRegistration.Taints)
}

func TestBuildWorkerMachineDeploymentDeletionOrder(t *testing.T) {
	t.Parallel()

	machineDeployment, err := buildWorkerMachineDeployment(
		db.Cluster{Name: "cluster-1"},
		db.NodePool{Name: "workers", DesiredCount: 3},
		"cluster-1-workers",
		"cluster-1-workers-infra",
		"cluster-1-workers-bootstrap",
		"v1.34.2",
	)

	require.NoError(t, err)
	assert.Equal(t, clusterv1.OldestMachineSetDeletionOrder, machineDeployment.Spec.Deletion.Order)
}
