// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package activities

import (
	"context"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nke/controller/internal/agent"
	"github.com/dsx-ai-factory/nke/controller/internal/capi"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/spiffe"
)

type trustDomainAndBundleGetter func(context.Context, config.Config) (string, string, error)

type Activities struct {
	cfg                      config.Config
	db                       db.DB
	k8sClient                ctrlclient.Client
	nodeAgentClient          agent.Client
	tenantClientProvider     TenantClientProvider
	trustDomain              string
	controllerClientCABundle string
	getTrustDomainBundle     trustDomainAndBundleGetter
}

func NewActivities(
	cfg config.Config,
	database db.DB,
	k8sClient ctrlclient.Client,
	nodeAgentClient agent.Client,
	trustDomain string,
	tenantClientProvider TenantClientProvider,
	controllerClientCABundle string,
) *Activities {
	return &Activities{
		cfg:                      cfg,
		db:                       database,
		k8sClient:                k8sClient,
		nodeAgentClient:          nodeAgentClient,
		tenantClientProvider:     tenantClientProvider,
		trustDomain:              trustDomain,
		controllerClientCABundle: controllerClientCABundle,
		getTrustDomainBundle:     spiffe.GetTrustDomainAndBundle,
	}
}

func (a *Activities) nodeAgentBootstrapFiles(_ db.Node) ([]db.CloudInitFile, error) {
	return capi.BuildNodeAgentBootstrapFiles(a.cfg, a.controllerClientCABundle)
}
