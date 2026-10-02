// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

func TestBuildNicoMachineInterfaces(t *testing.T) {
	subnetID := uuid.New()
	vpcPrefixID := uuid.New()

	tests := map[string]struct {
		subnetIDs      []uuid.UUID
		vpcPrefixIDs   []uuid.UUID
		wantSubnetID   string
		wantPrefixID   string
		wantErrContain string
	}{
		"prefers vpc prefix over subnet": {
			subnetIDs:    []uuid.UUID{subnetID},
			vpcPrefixIDs: []uuid.UUID{vpcPrefixID},
			wantPrefixID: vpcPrefixID.String(),
		},
		"uses subnet when prefix is absent": {
			subnetIDs:    []uuid.UUID{subnetID},
			wantSubnetID: subnetID.String(),
		},
		"requires a candidate": {
			wantErrContain: "both subnetIDs and vpcPrefixIDs are empty",
		},
	}

	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			interfaces, err := BuildNicoMachineInterfaces(params.subnetIDs, params.vpcPrefixIDs)
			if params.wantErrContain != "" {
				require.ErrorContains(t, err, params.wantErrContain)
				return
			}
			require.NoError(t, err)
			require.Len(t, interfaces, 1)
			assert.Equal(t, params.wantSubnetID, interfaces[0].SubnetID)
			assert.Equal(t, params.wantPrefixID, interfaces[0].VPCPrefixID)
		})
	}
}

func TestRenderIPXEScript(t *testing.T) {
	script, err := RenderIPXEScript("set disk-strategy {{ .DiskSelectionStrategy }}\n", string(db.DiskSelectionStrategyLargest))
	require.NoError(t, err)
	assert.Equal(t, "set disk-strategy largest\n", script)
}
