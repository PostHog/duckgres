package usersecrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// EnvKeyName is the env-only knob holding the base64-encoded 32-byte AES key
// used to encrypt user secret statements at rest in the config store. There
// is deliberately no CLI flag or YAML field: the key should only ever arrive
// via a mounted K8s Secret.
const EnvKeyName = "DUCKGRES_USER_SECRET_KEY"

// ciphertextVersion prefixes every sealed payload so the format can evolve
// (key rotation, algorithm change) without a table migration.
const ciphertextVersion = byte(1)

// Cipher seals and opens user secret statements with AES-256-GCM. The
// (org, user, name) row key is bound in as additional authenticated data, so
// a ciphertext copied onto another row fails to decrypt.
type Cipher struct {
	aead cipher.AEAD
}

// NewCipher builds a Cipher from a base64-encoded (std or url, padded or raw)
// 32-byte key.
func NewCipher(encodedKey string) (*Cipher, error) {
	var key []byte
	var err error
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	} {
		key, err = enc.DecodeString(encodedKey)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%s is not valid base64: %w", EnvKeyName, err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%s must decode to 32 bytes, got %d", EnvKeyName, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

func rowAAD(orgID, username, secretName string) []byte {
	return []byte(orgID + "\x00" + username + "\x00" + secretName)
}

// Seal encrypts a secret statement for storage on the (org, user, name) row.
func (c *Cipher) Seal(orgID, username, secretName, statement string) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+len(nonce)+len(statement)+c.aead.Overhead())
	out = append(out, ciphertextVersion)
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, []byte(statement), rowAAD(orgID, username, secretName)), nil
}

// Open decrypts a stored secret statement from the (org, user, name) row.
func (c *Cipher) Open(orgID, username, secretName string, ciphertext []byte) (string, error) {
	if len(ciphertext) < 1+c.aead.NonceSize() {
		return "", fmt.Errorf("ciphertext too short")
	}
	if ciphertext[0] != ciphertextVersion {
		return "", fmt.Errorf("unknown ciphertext version %d", ciphertext[0])
	}
	nonce := ciphertext[1 : 1+c.aead.NonceSize()]
	plain, err := c.aead.Open(nil, nonce, ciphertext[1+c.aead.NonceSize():], rowAAD(orgID, username, secretName))
	if err != nil {
		return "", fmt.Errorf("decrypt user secret: %w", err)
	}
	return string(plain), nil
}
