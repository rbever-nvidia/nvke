// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

type serverOptions struct {
	issuer    string
	tenant    string
	region    string
	partition string
}

type imdsServer struct {
	options   serverOptions
	signer    jose.Signer
	publicKey jose.JSONWebKey
	now       func() time.Time
}

func newIMDSServer(options serverOptions) (*imdsServer, error) {
	if options.issuer == "" || options.tenant == "" || options.region == "" || options.partition == "" {
		return nil, fmt.Errorf("issuer, tenant, region, and partition are required")
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	publicKey := jose.JSONWebKey{Key: &privateKey.PublicKey, Algorithm: string(jose.ES256), Use: "sig"}
	thumbprint, err := publicKey.Thumbprint(crypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("calculate signing key thumbprint: %w", err)
	}
	publicKey.KeyID = base64.RawURLEncoding.EncodeToString(thumbprint)
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.ES256, Key: privateKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), publicKey.KeyID),
	)
	if err != nil {
		return nil, fmt.Errorf("create JWT signer: %w", err)
	}
	return &imdsServer{options: options, signer: signer, publicKey: publicKey, now: time.Now}, nil
}

// imdsHandler mocks the IMDS server that a joining worker calls for a host
// identity token. In the real DSX flow, this metadata service returns the JWT
// but is not the public OIDC/JWT provider and does not expose discovery or
// signing keys.
func (s *imdsServer) imdsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /latest/meta-data/identity", s.handleIdentity)
	mux.HandleFunc("GET /live", handleHealthy)
	mux.HandleFunc("GET /ready", handleHealthy)
	return mux
}

// issuerHandler mocks the OIDC/JWT provider for the tokens returned by the
// IMDS mock. Kube-apiserver never asks IMDS how to validate a JWT; it follows
// the token's issuer configuration to discover signing keys. The local fixture
// keeps this provider in the same process as the IMDS mock only to avoid a
// second test binary, and exposes it on a separate listener so that distinction
// stays visible in the test topology.
func (s *imdsServer) issuerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", s.handleDiscovery)
	mux.HandleFunc("GET /.well-known/jwks.json", s.handleJWKS)
	mux.HandleFunc("GET /live", handleHealthy)
	mux.HandleFunc("GET /ready", handleHealthy)
	return mux
}

func (s *imdsServer) handleIdentity(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Metadata") != "true" {
		http.Error(w, "Metadata header must be true", http.StatusForbidden)
		return
	}
	audience := r.URL.Query().Get("aud")
	if audience == "" {
		http.Error(w, "aud query parameter is required", http.StatusBadRequest)
		return
	}
	plain, ok := identityPlainText(r.Header.Get("Accept"))
	if !ok {
		http.Error(w, "Accept must be text/plain or application/json", http.StatusNotAcceptable)
		return
	}

	host := r.RemoteAddr
	if parsedHost, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = parsedHost
	}
	digest := sha256.Sum256([]byte(host))
	subject := fmt.Sprintf(
		"spiffe://%s.%s.dsx.%s/bm/host-%x",
		s.options.tenant,
		s.options.region,
		s.options.partition,
		digest[:8],
	)
	now := s.now()
	expires := now.Add(5 * time.Minute)
	token, err := jwt.Signed(s.signer).Claims(map[string]any{
		"aud": audience,
		"exp": expires.Unix(),
		"iat": now.Unix(),
		"iss": s.options.issuer,
		"sub": subject,
	}).Serialize()
	if err != nil {
		http.Error(w, "failed to sign host identity", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if plain {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(token))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":      token,
		"expires_in": int(expires.Sub(now).Seconds()),
		"spiffe_id":  subject,
	})
}

// identityPlainText reports whether the caller asked for the compact JWT.
// An empty Accept header follows the real metadata service and means JSON.
func identityPlainText(accept string) (bool, bool) {
	if accept == "" || accept == "*/*" {
		return false, true
	}
	if strings.Contains(accept, "text/plain") {
		return true, true
	}
	if strings.Contains(accept, "application/json") || strings.Contains(accept, "*/*") {
		return false, true
	}
	return false, false
}

func (s *imdsServer) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	if err := json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{s.publicKey}}); err != nil {
		http.Error(w, "failed to encode signing keys", http.StatusInternalServerError)
	}
}

func (s *imdsServer) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	discovery := map[string]any{
		"issuer":                                s.options.issuer,
		"jwks_uri":                              discoveryJWKSURI(r),
		"id_token_signing_alg_values_supported": []string{"ES256"},
		"response_types_supported":              []string{"id_token"},
		"subject_types_supported":               []string{"public"},
	}
	if err := json.NewEncoder(w).Encode(discovery); err != nil {
		http.Error(w, "failed to encode discovery", http.StatusInternalServerError)
	}
}

func discoveryJWKSURI(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/.well-known/jwks.json", scheme, r.Host)
}

func handleHealthy(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
