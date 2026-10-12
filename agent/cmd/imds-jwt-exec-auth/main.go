// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package main implements a Kubernetes exec credential plugin for kubeadm
// discovery on MachineDeployment workers. The binary runs on the joining host,
// asks local IMDS for an audience-bound identity token, and returns that token
// to client-go as an ExecCredential. The plugin does not verify the JWT
// signature itself; kube-apiserver verifies the token against the DSX issuer and
// JWKS configured in its AuthenticationConfiguration.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

const (
	defaultIMDSEndpoint  = "http://169.254.169.254"
	envIMDSEndpoint      = "IMDS_ENDPOINT"
	envIMDSAudience      = "IMDS_JWT_AUDIENCE"
	envAllowNonLinkLocal = "IMDS_ALLOW_NON_LINK_LOCAL"
	metadataPath         = "/latest/meta-data/identity"
	requestTimeout       = 10 * time.Second
)

func main() {
	if err := run(context.Background(), os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, out io.Writer) error {
	audience := strings.TrimSpace(getenv(envIMDSAudience))
	if audience == "" {
		return fmt.Errorf("%s is required", envIMDSAudience)
	}
	endpoint := strings.TrimSpace(getenv(envIMDSEndpoint))
	if endpoint == "" {
		endpoint = defaultIMDSEndpoint
	}
	if err := validateEndpoint(endpoint, getenv(envAllowNonLinkLocal) == "true"); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	token, err := fetchToken(ctx, endpoint, audience)
	if err != nil {
		return err
	}
	expiry, err := jwtExpiry(token)
	if err != nil {
		return err
	}

	credential := clientauthv1.ExecCredential{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "client.authentication.k8s.io/v1",
			Kind:       "ExecCredential",
		},
		Status: &clientauthv1.ExecCredentialStatus{
			ExpirationTimestamp: &metav1.Time{Time: expiry},
			Token:               token,
		},
	}
	return json.NewEncoder(out).Encode(credential)
}

func validateEndpoint(raw string, allowNonLinkLocal bool) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parsing IMDS endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("IMDS endpoint must use http or https")
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("IMDS endpoint must include a host")
	}
	if allowNonLinkLocal {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLinkLocalUnicast() {
		return fmt.Errorf("IMDS endpoint host %q is not link-local; set %s=true for development fixtures", host, envAllowNonLinkLocal)
	}
	return nil
}

func fetchToken(ctx context.Context, endpoint, audience string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parsing IMDS endpoint: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + metadataPath
	query := base.Query()
	query.Set("aud", audience)
	base.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), http.NoBody)
	if err != nil {
		return "", fmt.Errorf("building IMDS identity request: %w", err)
	}
	req.Header.Set("Metadata", "true")
	// IMDS defaults to JSON ({token, expires_in, spiffe_id}). text/plain
	// is the compact JWT, which is what we want.
	req.Header.Set("Accept", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling IMDS identity endpoint: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("reading IMDS identity response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS identity endpoint returned HTTP %d", resp.StatusCode)
	}
	token := strings.TrimSpace(string(body))
	if token == "" {
		return "", errors.New("IMDS identity endpoint returned an empty token")
	}
	return token, nil
}

func jwtExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("IMDS identity response is not a compact JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("decoding JWT payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("decoding JWT claims: %w", err)
	}
	if claims.Exp <= 0 {
		return time.Time{}, errors.New("JWT has no exp claim")
	}
	return time.Unix(claims.Exp, 0).UTC(), nil
}
