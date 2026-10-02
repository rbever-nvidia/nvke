// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

//nolint:gochecknoinits // this is the typical way to register bun migrations
func init() {
	migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		if _, err := db.ExecContext(ctx, `
			ALTER TABLE node_pools
			ADD COLUMN lifecycle_mode text NOT NULL DEFAULT 'legacy'
			CHECK (lifecycle_mode IN ('legacy', 'machineDeployment'))
		`); err != nil {
			return fmt.Errorf("add node_pools.lifecycle_mode: %w", err)
		}
		return nil
	}, func(context.Context, *bun.DB) error {
		return fmt.Errorf("rollback not supported")
	})
}
