//go:build kubernetes

package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ALBOIDCVerifier verifies the X-Amzn-Oidc-Data JWT that an AWS ALB injects
// after a successful Cognito authentication.
//
// The verifier checks the ES256 signature against the ALB's regional public
// key. AWS publishes these keys at a fixed HTTPS endpoint per region. The
// verifier also checks the exp, signer, iss, and client protected-header
// fields. A request that never crossed the ALB cannot produce a JWT that
// passes these checks.
//
// This verification is the trust boundary for operator identity. Earlier
// versions trusted the header without a signature check. That trusted the
// network path instead: any caller that reached the pod directly could forge
// the header.
type ALBOIDCVerifier struct {
	region   string
	issuer   string
	clientID string

	// keyURL builds the public-key URL for a kid. Tests override it.
	keyURL func(kid string) string
	// httpClient fetches public keys. Tests override it.
	httpClient *http.Client
	// now returns the current time. Tests override it.
	now func() time.Time

	mu   sync.Mutex
	keys map[string]*ecdsa.PublicKey
}

// albKeyIDPattern restricts kid values before they enter a URL path.
// AWS key IDs contain only these characters.
var albKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// NewALBOIDCVerifier returns a verifier for ALB OIDC JWTs.
//
// region is the AWS region of the ALB. The verifier uses it for the public
// key endpoint and the signer claim prefix. issuer is the expected iss claim.
// clientID is the expected client claim. Empty issuer or clientID skips that
// check. Keep both set in production.
func NewALBOIDCVerifier(region, issuer, clientID string) (*ALBOIDCVerifier, error) {
	if region == "" {
		return nil, fmt.Errorf("ALB OIDC verifier requires an AWS region")
	}
	v := &ALBOIDCVerifier{
		region:   region,
		issuer:   issuer,
		clientID: clientID,
		now:      time.Now,
	}
	v.keyURL = func(kid string) string {
		return fmt.Sprintf("https://public-keys.auth.elb.%s.amazonaws.com/%s", region, kid)
	}
	v.httpClient = &http.Client{Timeout: 5 * time.Second}
	return v, nil
}

// Verify checks the signature and claims of an X-Amzn-Oidc-Data JWT. It
// returns the claims on success.
func (v *ALBOIDCVerifier) Verify(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errMalformedJWT
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		headerBytes, err = base64.URLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, errMalformedJWT
		}
	}
	var header struct {
		Alg      string `json:"alg"`
		Kid      string `json:"kid"`
		Signer   string `json:"signer"`
		Issuer   string `json:"iss"`
		ClientID string `json:"client"`
		Expires  *int64 `json:"exp"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, errMalformedJWT
	}
	// The ALB signs with ES256. Reject any other algorithm. This prevents
	// algorithm-confusion attacks.
	if header.Alg != "ES256" {
		return nil, &jwtError{"unexpected JWT algorithm"}
	}
	if !albKeyIDPattern.MatchString(header.Kid) {
		return nil, &jwtError{"invalid JWT key id"}
	}

	key, err := v.publicKey(header.Kid)
	if err != nil {
		return nil, fmt.Errorf("fetch ALB public key: %w", err)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		sig, err = base64.URLEncoding.DecodeString(parts[2])
		if err != nil {
			return nil, errMalformedJWT
		}
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	// JWS encodes an ES256 signature as the fixed-width concatenation R || S,
	// with 32 bytes per integer. It is not an ASN.1 DER signature.
	if len(sig) != 64 {
		return nil, &jwtError{"JWT signature verification failed"}
	}
	r := new(big.Int).SetBytes(sig[:32])
	s := new(big.Int).SetBytes(sig[32:])
	if !ecdsa.Verify(key, digest[:], r, s) {
		return nil, &jwtError{"JWT signature verification failed"}
	}
	if err := v.checkHeader(header.Expires, header.Signer, header.Issuer, header.ClientID); err != nil {
		return nil, err
	}

	claims, err := decodeJWTClaims(token)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// checkHeader validates the ALB metadata in the verified JWT's protected
// header. The payload contains only the user claims returned by the IdP.
func (v *ALBOIDCVerifier) checkHeader(exp *int64, signer, issuer, clientID string) error {
	// The ALB always sets exp. Reject tokens without it.
	if exp == nil {
		return &jwtError{"missing exp claim"}
	}
	// Allow 60 seconds of clock skew between the ALB and this process.
	if time.Unix(*exp, 0).Add(60 * time.Second).Before(v.now()) {
		return &jwtError{"JWT expired"}
	}
	// The signer header field names the ALB that signed the JWT. Require an ALB
	// ARN in the configured region.
	expectedPrefix := "arn:aws:elasticloadbalancing:" + v.region + ":"
	if !strings.HasPrefix(signer, expectedPrefix) {
		return &jwtError{"unexpected JWT signer"}
	}
	if v.issuer != "" && issuer != v.issuer {
		return &jwtError{"unexpected JWT issuer"}
	}
	if v.clientID != "" && clientID != v.clientID {
		return &jwtError{"unexpected JWT client"}
	}
	return nil
}

// publicKey returns the cached key for kid. It fetches the key from AWS on
// a cache miss. A forged JWT with an unknown kid causes one fetch per kid.
// The kid pattern and the per-kid cache bound that fetch rate.
func (v *ALBOIDCVerifier) publicKey(kid string) (*ecdsa.PublicKey, error) {
	v.mu.Lock()
	if v.keys == nil {
		v.keys = make(map[string]*ecdsa.PublicKey)
	}
	if key, ok := v.keys[kid]; ok {
		v.mu.Unlock()
		return key, nil
	}
	v.mu.Unlock()

	key, err := v.fetchKey(kid)
	if err != nil {
		return nil, err
	}

	v.mu.Lock()
	v.keys[kid] = key
	v.mu.Unlock()
	return key, nil
}

// fetchKey downloads the PEM-encoded public key for kid from AWS.
func (v *ALBOIDCVerifier) fetchKey(kid string) (*ecdsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, v.keyURL(kid), nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("key endpoint returned %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(body)
	if block == nil {
		return nil, fmt.Errorf("key endpoint returned no PEM block")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is not ECDSA")
	}
	if key.Curve != elliptic.P256() {
		return nil, fmt.Errorf("public key is not P-256")
	}
	return key, nil
}
