// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/spiffe"
)

type x509ContextWatchFunc func(context.Context, workloadapi.X509ContextWatcher) error

// SpireTrustBundleController watches CWE's Workload API X.509 context and publishes its trust bundle to the Secret CABPK resolves for new Machines.
type SpireTrustBundleController struct {
	cfg       config.Config
	k8sClient client.Client
	logger    *slog.Logger
	watch     x509ContextWatchFunc
	events    chan event.TypedGenericEvent[*corev1.Secret]

	mu           sync.RWMutex
	latestBundle []byte
}

// NewSpireTrustBundleController creates a SPIRE trust-bundle controller.
func NewSpireTrustBundleController(cfg config.Config, k8sClient client.Client, logger *slog.Logger) *SpireTrustBundleController {
	return &SpireTrustBundleController{
		cfg:       cfg,
		k8sClient: k8sClient,
		logger:    logger,
		watch: func(ctx context.Context, watcher workloadapi.X509ContextWatcher) error {
			return workloadapi.WatchX509Context(ctx, watcher, workloadapi.WithAddr(cfg.SpiffeEndpointSocket))
		},
		events: make(chan event.TypedGenericEvent[*corev1.Secret], 1),
	}
}

// SetupWithManager registers two runnables on the same manager, unlike a
// more common k8s-native controller which only does
// NewControllerManagedBy(mgr).For(...).Complete(c).
//
// mgr.Add(c) is extra. Controller also implements manager.Runnable
// (Start) and LeaderElectionRunnable (NeedLeaderElection). The manager
// calls Start after it becomes leader; that loop is a SPIRE Workload API
// watch, not a kube watch. Updates from Start enqueue via c.events into
// the same Reconcile as the Secret watch.
func (c *SpireTrustBundleController) SetupWithManager(mgr manager.Manager) error {
	if err := mgr.Add(c); err != nil {
		return fmt.Errorf("adding SPIRE trust-bundle watcher: %w", err)
	}

	matchesTargetSecret := func(obj client.Object) bool {
		secret, ok := obj.(*corev1.Secret)
		return ok &&
			secret.Namespace == constants.NamespaceNKESystem &&
			secret.Name == constants.CAPISpireTrustBundleObjectName
	}

	secretPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return matchesTargetSecret(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return matchesTargetSecret(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return matchesTargetSecret(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return matchesTargetSecret(e.Object)
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}, builder.WithPredicates(secretPredicate)).
		WatchesRawSource(source.Channel(c.events, &handler.TypedEnqueueRequestForObject[*corev1.Secret]{})).
		Named("spire-trust-bundle").
		Complete(c)
}

// Start watches the Workload API until the manager stops.
//
// Not called directly by NKE code. manager.Start invokes it because SetupWithManager
// registered this Controller with mgr.Add.
func (c *SpireTrustBundleController) Start(ctx context.Context) error {
	err := c.watch(ctx, c)
	if isCanceled(err) {
		return nil
	}
	return fmt.Errorf("watching SPIRE X.509 context: %w", err)
}

// NeedLeaderElection ensures only the active manager publishes the bundle.
func (c *SpireTrustBundleController) NeedLeaderElection() bool {
	return true
}

// OnX509ContextUpdate records the newest bundle and enqueues its publication.
func (c *SpireTrustBundleController) OnX509ContextUpdate(x509Context *workloadapi.X509Context) {
	_, bundle, err := spiffe.TrustDomainAndBundle(x509Context)
	if err != nil {
		c.logger.Error("Failed to read SPIRE trust bundle update", "error", err)
		return
	}

	c.mu.Lock()
	c.latestBundle = []byte(bundle)
	c.mu.Unlock()

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      constants.CAPISpireTrustBundleObjectName,
		Namespace: constants.NamespaceNKESystem,
	}}
	// Enqueue the Secret to wake Reconcile().
	// Do not block the SPIRE watch callback if a reconcile is already queued.
	// Reconcile reads latestBundle, so one queued event is enough to publish
	// the newest bundle even if several Workload API updates arrive quickly.
	select {
	case c.events <- event.TypedGenericEvent[*corev1.Secret]{Object: secret}:
	default:
	}
}

// OnX509ContextWatchError logs transient Workload API watch failures. The Workload API client reconnects with backoff.
func (c *SpireTrustBundleController) OnX509ContextWatchError(err error) {
	if !isCanceled(err) {
		c.logger.Warn("SPIRE X.509 context watch interrupted", "error", err)
	}
}

func isCanceled(err error) bool {
	return err == nil || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// Reconcile publishes the latest trust bundle.
func (c *SpireTrustBundleController) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	c.mu.RLock()
	bundle := make([]byte, len(c.latestBundle))
	copy(bundle, c.latestBundle)
	c.mu.RUnlock()

	if len(bundle) == 0 {
		return reconcile.Result{}, fmt.Errorf("SPIRE trust bundle is not available")
	}

	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.CAPISpireTrustBundleObjectName,
			Namespace: constants.NamespaceNKESystem,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			constants.CAPISpireTrustBundleSecretKey: bundle,
		},
	}

	err := c.k8sClient.Patch(ctx, secret, client.Apply, client.FieldOwner(c.cfg.FieldManagerName), client.ForceOwnership)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("publishing SPIRE trust bundle Secret: %w", err)
	}

	return reconcile.Result{}, nil
}
