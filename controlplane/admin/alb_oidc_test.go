//go:build kubernetes

package admin

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// testALBSigningKey generates a throwaway P-256 key for signed test JWTs.
func testALBSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	return key
}

// testALBVerifier returns a verifier whose key endpoint is a local test
// server that serves the public half of key for any kid.
func testALBVerifier(t *testing.T, key *ecdsa.PrivateKey, issuer, clientID string) *ALBOIDCVerifier {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal test public key: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(pemBytes)
	}))
	t.Cleanup(srv.Close)

	v, err := NewALBOIDCVerifier("us-east-1", issuer, clientID)
	if err != nil {
		t.Fatalf("NewALBOIDCVerifier: %v", err)
	}
	v.keyURL = func(kid string) string { return srv.URL + "/" + url.PathEscape(kid) }
	v.httpClient = srv.Client()
	return v
}

// signedOIDC builds an ES256-signed JWT with the given claims, in the
// X-Amzn-Oidc-Data wire format (header.payload.signature).
func signedOIDC(t *testing.T, key *ecdsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	headerSeg := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256","kid":"test-kid","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payloadSeg := base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(headerSeg + "." + payloadSeg))
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("sign test JWT: %v", err)
	}
	return headerSeg + "." + payloadSeg + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// validALBClaims returns claims that pass the verifier's claim checks for a
// verifier built with issuer testALBIssuer and clientID testALBClientID.
// Callers override individual keys to build rejection cases.
const (
	testALBIssuer   = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_TEST"
	testALBClientID = "test-client-id"
)

func validALBClaims(overrides map[string]any) map[string]any {
	claims := map[string]any{
		"exp":    float64(time.Now().Add(time.Hour).Unix()),
		"signer": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/internal/test/abc",
		"iss":    testALBIssuer,
		"client": testALBClientID,
	}
	for k, v := range overrides {
		claims[k] = v
	}
	return claims
}

func TestALBOIDCVerifierAcceptsValidToken(t *testing.T) {
	key := testALBSigningKey(t)
	v := testALBVerifier(t, key, testALBIssuer, testALBClientID)
	claims, err := v.Verify(signedOIDC(t, key, validALBClaims(map[string]any{"email": "a@posthog.com"})))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims["email"] != "a@posthog.com" {
		t.Fatalf("email = %v", claims["email"])
	}
}

func TestALBOIDCVerifierRejections(t *testing.T) {
	key := testALBSigningKey(t)
	otherKey := testALBSigningKey(t)
	v := testALBVerifier(t, key, testALBIssuer, testALBClientID)

	wrongSig := signedOIDC(t, otherKey, validALBClaims(nil))

	tampered := signedOIDC(t, key, validALBClaims(nil))
	// Replace the payload with attacker claims, keep the original signature.
	forgedPayload, _ := json.Marshal(validALBClaims(map[string]any{"email": "admin@posthog.com", "role": "admin"}))
	parts := splitForTest(t, tampered)
	tampered = parts[0] + "." + base64.RawURLEncoding.EncodeToString(forgedPayload) + "." + parts[2]

	cases := []struct {
		name  string
		token string
	}{
		{"signature from wrong key", wrongSig},
		{"tampered payload", tampered},
		{"unsigned legacy token", mkUnsignedOIDC(map[string]any{"email": "a@posthog.com"})},
		{"expired", signedOIDC(t, key, validALBClaims(map[string]any{"exp": float64(time.Now().Add(-time.Hour).Unix())}))},
		{"missing exp", signedOIDC(t, key, map[string]any{"signer": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/internal/test/abc", "iss": testALBIssuer, "client": testALBClientID})},
		{"signer in another region", signedOIDC(t, key, validALBClaims(map[string]any{"signer": "arn:aws:elasticloadbalancing:eu-central-1:123456789012:loadbalancer/internal/test/abc"}))},
		{"wrong issuer", signedOIDC(t, key, validALBClaims(map[string]any{"iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_OTHER"}))},
		{"wrong client", signedOIDC(t, key, validALBClaims(map[string]any{"client": "other-client"}))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := v.Verify(tc.token); err == nil {
				t.Fatal("Verify accepted a token it must reject")
			}
		})
	}
}

// splitForTest splits a JWT into its three segments.
func splitForTest(t *testing.T, token string) [3]string {
	t.Helper()
	var out [3]string
	start := 0
	for i := 0; i < 2; i++ {
		idx := -1
		for j := start; j < len(token); j++ {
			if token[j] == '.' {
				idx = j
				break
			}
		}
		if idx < 0 {
			t.Fatalf("token has fewer than 3 segments")
		}
		out[i] = token[start:idx]
		start = idx + 1
	}
	out[2] = token[start:]
	return out
}
