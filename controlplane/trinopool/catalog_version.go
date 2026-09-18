// Package trinopool holds the durable model and the pure logic of the shared
// Trino compute pool: the catalog publisher's wire-compatible hashing, the
// immutable instance blueprint, and the pool/instance/operation state machine.
// Nothing in this package talks to Kubernetes, so it builds and is tested
// without the `kubernetes` build tag.
package trinopool

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"unicode/utf16"
)

// CatalogVersion reproduces, byte for byte, the catalog version that the Trino
// coordinator computes in
// io.trino.plugin.catalogstore.posthog.CatalogVersions#computeCatalogVersion.
//
// Duckgres writes `trino_catalogs` rows directly, so a version that differs by
// a single byte makes every coordinator treat an unchanged catalog as a new
// one. Three details of the Java code are load-bearing and easy to get wrong in
// Go:
//
//   - Guava's Hasher.putUnencodedChars writes UTF-16 code units little-endian,
//     not UTF-8 bytes.
//   - The length prefix is Java's String.length(), the UTF-16 code-unit count,
//     not the rune count and not the byte count.
//   - ImmutableSortedMap orders keys with String.compareTo, i.e. by UTF-16 code
//     unit. That differs from Go's UTF-8 byte order for supplementary-plane
//     characters, which sort below U+E000 in UTF-16 and above it in UTF-8.
//
// Pinned by TestCatalogVersionMatchesJavaGoldenVector against the vector the
// Java test pins. Changing any of it invalidates every version already stored.
func CatalogVersion(catalogName, connectorName string, properties map[string]string) string {
	digest := sha256.New()
	hashChars(digest, "catalog-hash")
	hashLengthPrefixed(digest, catalogName)
	hashLengthPrefixed(digest, connectorName)
	hashInt(digest, int32(len(properties)))
	for _, key := range sortedUTF16Keys(properties) {
		hashLengthPrefixed(digest, key)
		hashLengthPrefixed(digest, properties[key])
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// sortedUTF16Keys orders keys the way Java's String.compareTo does.
func sortedUTF16Keys(properties map[string]string) []string {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
	return keys
}

// lessUTF16 compares two strings by UTF-16 code unit, matching
// java.lang.String#compareTo.
func lessUTF16(left, right string) bool {
	leftUnits, rightUnits := utf16.Encode([]rune(left)), utf16.Encode([]rune(right))
	for index := 0; index < len(leftUnits) && index < len(rightUnits); index++ {
		if leftUnits[index] != rightUnits[index] {
			return leftUnits[index] < rightUnits[index]
		}
	}
	return len(leftUnits) < len(rightUnits)
}

type byteWriter interface{ Write([]byte) (int, error) }

func hashLengthPrefixed(digest byteWriter, value string) {
	units := utf16.Encode([]rune(value))
	hashInt(digest, int32(len(units)))
	hashUnits(digest, units)
}

func hashChars(digest byteWriter, value string) {
	hashUnits(digest, utf16.Encode([]rune(value)))
}

func hashUnits(digest byteWriter, units []uint16) {
	buffer := make([]byte, 2*len(units))
	for index, unit := range units {
		binary.LittleEndian.PutUint16(buffer[2*index:], unit)
	}
	_, _ = digest.Write(buffer)
}

func hashInt(digest byteWriter, value int32) {
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, uint32(value))
	_, _ = digest.Write(buffer)
}
