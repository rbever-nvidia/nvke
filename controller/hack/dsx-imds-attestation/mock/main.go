// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	imdsListen := flag.String("imds-listen", ":8080", "IMDS HTTP listen address")
	issuerListen := flag.String("issuer-listen", ":8081", "OIDC issuer HTTP listen address")
	issuerTLSListen := flag.String("issuer-tls-listen", "", "optional OIDC issuer HTTPS listen address")
	issuerTLSCert := flag.String("issuer-tls-cert", "", "certificate file for -issuer-tls-listen")
	issuerTLSKey := flag.String("issuer-tls-key", "", "private key file for -issuer-tls-listen")
	issuer := flag.String("issuer", "https://ds9.dsx.nvid.id", "issuer for host identity JWTs")
	tenant := flag.String("tenant", "operator-1", "tenant encoded in host identities")
	region := flag.String("region", "ds9", "region encoded in host identities")
	partition := flag.String("partition", "nvid.id", "partition encoded in host identities")
	flag.Parse()

	imds, err := newIMDSServer(serverOptions{
		issuer: *issuer, tenant: *tenant, region: *region, partition: *partition,
	})
	if err != nil {
		return fmt.Errorf("create IMDS server: %w", err)
	}
	imdsServer := &http.Server{Addr: *imdsListen, Handler: imds.imdsHandler(), ReadHeaderTimeout: 10 * time.Second}
	issuerServer := &http.Server{Addr: *issuerListen, Handler: imds.issuerHandler(), ReadHeaderTimeout: 10 * time.Second}
	var issuerTLSServer *http.Server
	if *issuerTLSListen != "" {
		if *issuerTLSCert == "" || *issuerTLSKey == "" {
			return errors.New("-issuer-tls-cert and -issuer-tls-key are required with -issuer-tls-listen")
		}
		issuerTLSServer = &http.Server{Addr: *issuerTLSListen, Handler: imds.issuerHandler(), ReadHeaderTimeout: 10 * time.Second}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := imdsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Failed to shut down DSX IMDS mock", "error", err)
		}
		if err := issuerServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Failed to shut down DSX issuer mock", "error", err)
		}
		if issuerTLSServer != nil {
			if err := issuerTLSServer.Shutdown(shutdownCtx); err != nil {
				slog.Error("Failed to shut down DSX TLS issuer mock", "error", err)
			}
		}
	}()

	servers := 2
	errs := make(chan error, 3)
	if issuerTLSServer != nil {
		servers++
		go func() {
			slog.Info("Starting DSX TLS issuer mock", "listen", *issuerTLSListen, "issuer", *issuer)
			if err := issuerTLSServer.ListenAndServeTLS(*issuerTLSCert, *issuerTLSKey); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- fmt.Errorf("serve DSX TLS issuer mock: %w", err)
				return
			}
			errs <- nil
		}()
	}
	go func() {
		slog.Info("Starting DSX IMDS mock", "listen", *imdsListen, "issuer", *issuer)
		if err := imdsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("serve DSX IMDS mock: %w", err)
			return
		}
		errs <- nil
	}()
	go func() {
		slog.Info("Starting DSX issuer mock", "listen", *issuerListen, "issuer", *issuer)
		if err := issuerServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("serve DSX issuer mock: %w", err)
			return
		}
		errs <- nil
	}()

	for i := 0; i < servers; i++ {
		if err := <-errs; err != nil {
			stop()
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("DSX IMDS mock failed", "error", err)
		os.Exit(1)
	}
}
