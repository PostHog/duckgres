package trinopool

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"unicode/utf16"
)

// The Trino coordinator derives a catalog's version from its own
// io.trino.plugin.catalogstore.posthog.CatalogVersions implementation. Once
// duckgres writes catalog rows directly it has to produce byte-identical
// versions, otherwise every existing row looks changed to every coordinator.
// This vector is the one pinned by the Java test.
const javaGoldenCatalogVersion = "0b48ccd298e6f062b4f8d81466bbeab8ef9cea832c4c08356f7e661cad1d040c"

func goldenProperties() map[string]string {
	return map[string]string{
		"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
		"ducklake.data-path":               "s3://bucket/prefix/",
	}
}

func TestCatalogVersionMatchesJavaGoldenVector(t *testing.T) {
	if version := CatalogVersion("orders", "ducklake", goldenProperties()); version != javaGoldenCatalogVersion {
		t.Fatalf("catalog version = %q, want %q", version, javaGoldenCatalogVersion)
	}
}

func TestCatalogVersionIgnoresPropertyOrder(t *testing.T) {
	first := CatalogVersion("orders", "ducklake", map[string]string{
		"ducklake.data-path":               "s3://bucket/prefix/",
		"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
	})
	second := CatalogVersion("orders", "ducklake", goldenProperties())
	if first != second {
		t.Fatalf("property order changed the version: %q vs %q", first, second)
	}
}

func TestCatalogVersionEveryInputChangesTheVersion(t *testing.T) {
	base := CatalogVersion("orders", "ducklake", goldenProperties())
	cases := map[string]string{
		"catalog name":   CatalogVersion("invoices", "ducklake", goldenProperties()),
		"connector name": CatalogVersion("orders", "iceberg", goldenProperties()),
		"extra property": CatalogVersion("orders", "ducklake", map[string]string{
			"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
			"ducklake.data-path":               "s3://bucket/prefix/",
			"ducklake.max-split-size":          "32MB",
		}),
		"changed value": CatalogVersion("orders", "ducklake", map[string]string{
			"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
			"ducklake.data-path":               "s3://other-bucket/prefix/",
		}),
		"no properties": CatalogVersion("orders", "ducklake", nil),
	}
	for name, version := range cases {
		if version == base {
			t.Errorf("%s did not change the catalog version", name)
		}
	}
}

// Java hashes a length prefix before each string, so moving a character across
// the key/value boundary has to change the version.
func TestCatalogVersionLengthPrefixSeparatesAdjacentStrings(t *testing.T) {
	if CatalogVersion("orders", "ducklake", map[string]string{"ab": "cd"}) ==
		CatalogVersion("orders", "ducklake", map[string]string{"a": "bcd"}) {
		t.Fatal("adjacent strings are not separated by a length prefix")
	}
}

// Java sorts properties with String.compareTo, which compares UTF-16 code
// units. Go's natural string order is UTF-8 byte order, and the two disagree
// exactly when a supplementary-plane character meets one from U+E000..U+FFFF:
// the surrogate pair starts at 0xD83D in UTF-16, below U+E000, while its UTF-8
// encoding starts at 0xF0, above U+E000's 0xEE. Sorting the Go way silently
// produces a version no coordinator agrees with.
func TestCatalogVersionSortsPropertiesInUTF16Order(t *testing.T) {
	properties := map[string]string{
		"\U0001F600": "emoji",
		"":          "private-use",
	}
	want := referenceCatalogVersion("orders", "ducklake", [][2]string{
		{"\U0001F600", "emoji"},
		{"", "private-use"},
	})
	if got := CatalogVersion("orders", "ducklake", properties); got != want {
		t.Fatalf("properties were not sorted in UTF-16 order: got %q, want %q", got, want)
	}
}

// Guards the reference implementation the previous test compares against: if it
// drifts from Java, its verdict is worthless.
func TestReferenceCatalogVersionMatchesJavaGoldenVector(t *testing.T) {
	got := referenceCatalogVersion("orders", "ducklake", [][2]string{
		{"ducklake.data-path", "s3://bucket/prefix/"},
		{"ducklake.metadata.connection-url", "jdbc:postgresql://db:5432/lake"},
	})
	if got != javaGoldenCatalogVersion {
		t.Fatalf("reference implementation = %q, want %q", got, javaGoldenCatalogVersion)
	}
}

// referenceCatalogVersion is a deliberately literal, independently written
// transcription of the Java hasher over already-ordered pairs. It exists so the
// ordering and length-prefix rules are pinned by something other than the
// implementation under test.
func referenceCatalogVersion(catalog, connector string, ordered [][2]string) string {
	digest := sha256.New()
	putChars := func(value string) {
		for _, unit := range utf16.Encode([]rune(value)) {
			_, _ = digest.Write([]byte{byte(unit), byte(unit >> 8)})
		}
	}
	putInt := func(value int32) {
		buffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(buffer, uint32(value))
		_, _ = digest.Write(buffer)
	}
	putString := func(value string) {
		putInt(int32(len(utf16.Encode([]rune(value)))))
		putChars(value)
	}

	putChars("catalog-hash")
	putString(catalog)
	putString(connector)
	putInt(int32(len(ordered)))
	for _, pair := range ordered {
		putString(pair[0])
		putString(pair[1])
	}
	return hex.EncodeToString(digest.Sum(nil))
}
