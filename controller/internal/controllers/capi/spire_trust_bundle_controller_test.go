// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package capi

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"testing"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/constants"
)

func TestSpireTrustBundleControllerStart(t *testing.T) {
	t.Run("runs the Workload API watch", func(t *testing.T) {
		watchErr := errors.New("watch failed")
		controller := newSpireTrustBundleControllerForTest(t)
		controller.watch = func(_ context.Context, watcher workloadapi.X509ContextWatcher) error {
			require.Same(t, controller, watcher)
			return watchErr
		}

		err := controller.Start(context.Background())
		require.ErrorIs(t, err, watchErr)
		require.ErrorContains(t, err, "watching SPIRE X.509 context")
	})

	for name, watchErr := range map[string]error{
		"context cancellation": context.Canceled,
		"gRPC cancellation":    status.Error(codes.Canceled, "context canceled"),
	} {
		t.Run("treats "+name+" as a clean shutdown", func(t *testing.T) {
			controller := newSpireTrustBundleControllerForTest(t)
			controller.watch = func(context.Context, workloadapi.X509ContextWatcher) error {
				return watchErr
			}

			require.NoError(t, controller.Start(context.Background()))
		})
	}
}

func TestSpireTrustBundleControllerOnX509ContextUpdatePublishesBundle(t *testing.T) {
	controller := newSpireTrustBundleControllerForTest(t)
	x509Context, expectedBundle := newX509ContextForTest(t)

	controller.OnX509ContextUpdate(x509Context)

	select {
	case published := <-controller.events:
		require.Equal(t, constants.NamespaceNKESystem, published.Object.Namespace)
		require.Equal(t, constants.CAPISpireTrustBundleObjectName, published.Object.Name)
	default:
		t.Fatal("expected trust-bundle publication event")
	}

	_, err := controller.Reconcile(context.Background(), reconcile.Request{})
	require.NoError(t, err)

	secret := getTrustBundleSecretForTest(t, controller)
	require.Equal(t, expectedBundle, secret.Data[constants.CAPISpireTrustBundleSecretKey])
}

func TestSpireTrustBundleControllerReconcileRepairsSecretDrift(t *testing.T) {
	controller := newSpireTrustBundleControllerForTest(t)
	controller.latestBundle = []byte("expected bundle")

	_, err := controller.Reconcile(context.Background(), reconcile.Request{})
	require.NoError(t, err)

	secret := getTrustBundleSecretForTest(t, controller)
	secret.Data[constants.CAPISpireTrustBundleSecretKey] = []byte("changed bundle")
	require.NoError(t, controller.k8sClient.Update(context.Background(), secret))

	_, err = controller.Reconcile(context.Background(), reconcile.Request{})
	require.NoError(t, err)

	secret = getTrustBundleSecretForTest(t, controller)
	require.Equal(t, []byte("expected bundle"), secret.Data[constants.CAPISpireTrustBundleSecretKey])
}

func TestSpireTrustBundleControllerReconcileWaitsForBundle(t *testing.T) {
	controller := newSpireTrustBundleControllerForTest(t)

	_, err := controller.Reconcile(context.Background(), reconcile.Request{})
	require.ErrorContains(t, err, "SPIRE trust bundle is not available")
}

func TestSpireTrustBundleControllerNeedsLeaderElection(t *testing.T) {
	require.True(t, newSpireTrustBundleControllerForTest(t).NeedLeaderElection())
}

func newSpireTrustBundleControllerForTest(t *testing.T) *SpireTrustBundleController {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return &SpireTrustBundleController{
		cfg:       config.Config{FieldManagerName: "test-manager"},
		k8sClient: fake.NewClientBuilder().WithScheme(scheme).Build(),
		logger:    slog.New(slog.DiscardHandler),
		events:    make(chan event.TypedGenericEvent[*corev1.Secret], 1),
	}
}

func newX509ContextForTest(t *testing.T) (*workloadapi.X509Context, []byte) {
	t.Helper()

	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	bundle := x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{{Raw: []byte("test certificate")}})
	bundlePEM, err := bundle.Marshal()
	require.NoError(t, err)

	return &workloadapi.X509Context{
		SVIDs: []*x509svid.SVID{{
			ID: spiffeid.RequireFromString("spiffe://example.org/controller"),
		}},
		Bundles: x509bundle.NewSet(bundle),
	}, bundlePEM
}

func getTrustBundleSecretForTest(t *testing.T, controller *SpireTrustBundleController) *corev1.Secret {
	t.Helper()

	secret := &corev1.Secret{}
	require.NoError(t, controller.k8sClient.Get(context.Background(), types.NamespacedName{
		Namespace: constants.NamespaceNKESystem,
		Name:      constants.CAPISpireTrustBundleObjectName,
	}, secret))
	return secret
}
