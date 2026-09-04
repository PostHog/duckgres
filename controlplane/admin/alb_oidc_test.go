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
	v.now = func() time.Time { return testALBNow }
	return v
}

// signedOIDC builds an AWS ALB X-Amzn-Oidc-Data JWT. ALB security metadata
// lives in the protected header, while the payload contains only user claims.
func signedOIDC(t *testing.T, key *ecdsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	return signedOIDCWithHeader(t, key, validALBHeader(nil), claims)
}

func signedOIDCWithHeader(t *testing.T, key *ecdsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	signingInput := oidcSigningInput(t, header, claims)
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("sign test JWT: %v", err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signingInput + "." + base64.URLEncoding.EncodeToString(sig)
}

func signedOIDCASN1(t *testing.T, key *ecdsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	signingInput := oidcSigningInput(t, header, claims)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("sign test JWT: %v", err)
	}
	return signingInput + "." + base64.URLEncoding.EncodeToString(sig)
}

func oidcSigningInput(t *testing.T, header, claims map[string]any) string {
	t.Helper()
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	// AWS ALB uses padded base64url segments.
	return base64.URLEncoding.EncodeToString(headerJSON) + "." + base64.URLEncoding.EncodeToString(payloadJSON)
}

const (
	testALBIssuer   = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_TEST"
	testALBClientID = "test-client-id"
	testALBSigner   = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc"
)

var testALBNow = time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)

// validALBHeader returns the protected header emitted by AWS ALB. Callers
// override individual fields to build rejection cases.
func validALBHeader(overrides map[string]any) map[string]any {
	header := map[string]any{
		"alg":    "ES256",
		"kid":    "test-kid",
		"signer": testALBSigner,
		"iss":    testALBIssuer,
		"client": testALBClientID,
		"exp":    testALBNow.Add(time.Hour).Unix(),
	}
	for k, v := range overrides {
		header[k] = v
	}
	return header
}

func TestALBOIDCVerifierAcceptsValidToken(t *testing.T) {
	key := testALBSigningKey(t)
	v := testALBVerifier(t, key, testALBIssuer, testALBClientID)
	claims, err := v.Verify(signedOIDC(t, key, map[string]any{"email": "a@posthog.com"}))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims["email"] != "a@posthog.com" {
		t.Fatalf("email = %v", claims["email"])
	}
	if _, ok := claims["exp"]; ok {
		t.Fatal("Verify returned protected-header metadata as a payload claim")
	}
}

func TestALBOIDCVerifierRejections(t *testing.T) {
	key := testALBSigningKey(t)
	otherKey := testALBSigningKey(t)
	v := testALBVerifier(t, key, testALBIssuer, testALBClientID)

	wrongSig := signedOIDC(t, otherKey, map[string]any{"email": "a@posthog.com"})

	tampered := signedOIDC(t, key, map[string]any{"email": "a@posthog.com"})
	// Replace the payload with attacker claims, keep the original signature.
	forgedPayload, _ := json.Marshal(map[string]any{"email": "admin@posthog.com", "role": "admin"})
	parts := splitForTest(t, tampered)
	tampered = parts[0] + "." + base64.URLEncoding.EncodeToString(forgedPayload) + "." + parts[2]

	missingExpHeader := validALBHeader(nil)
	delete(missingExpHeader, "exp")
	metadataInPayload := validALBHeader(nil)
	metadataInPayload["email"] = "a@posthog.com"
	noUserClaims := map[string]any{}
	validParts := splitForTest(t, signedOIDC(t, key, map[string]any{"email": "a@posthog.com"}))
	shortSignature := validParts[0] + "." + validParts[1] + "." + base64.URLEncoding.EncodeToString(make([]byte, 63))

	cases := []struct {
		name  string
		token string
	}{
		{"signature from wrong key", wrongSig},
		{"tampered payload", tampered},
		{"unsigned legacy token", mkUnsignedOIDC(map[string]any{"email": "a@posthog.com"})},
		{"ASN.1 signature", signedOIDCASN1(t, key, validALBHeader(nil), map[string]any{"email": "a@posthog.com"})},
		{"short JWS signature", shortSignature},
		{"expired", signedOIDCWithHeader(t, key, validALBHeader(map[string]any{"exp": testALBNow.Add(-time.Hour).Unix()}), noUserClaims)},
		{"missing exp", signedOIDCWithHeader(t, key, missingExpHeader, noUserClaims)},
		{"non-integer exp", signedOIDCWithHeader(t, key, validALBHeader(map[string]any{"exp": "not-a-number"}), noUserClaims)},
		{"metadata only in payload", signedOIDCWithHeader(t, key, map[string]any{"alg": "ES256", "kid": "test-kid"}, metadataInPayload)},
		{"signer in another region", signedOIDCWithHeader(t, key, validALBHeader(map[string]any{"signer": "arn:aws:elasticloadbalancing:eu-central-1:123456789012:loadbalancer/app/test/abc"}), noUserClaims)},
		{"wrong issuer", signedOIDCWithHeader(t, key, validALBHeader(map[string]any{"iss": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_OTHER"}), noUserClaims)},
		{"wrong client", signedOIDCWithHeader(t, key, validALBHeader(map[string]any{"client": "other-client"}), noUserClaims)},
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
