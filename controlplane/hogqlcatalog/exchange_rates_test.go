package hogqlcatalog

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMemoryStorePersistsImmutableExchangeRateGenerations(t *testing.T) {
	store := NewMemoryStore()
	first := testExchangeRateSnapshot(1)
	if err := store.PublishExchangeRates(t.Context(), first); err != nil {
		t.Fatalf("publish generation 1: %v", err)
	}
	first.Rates[0].UnscaledRate = "1"

	latest, err := store.LatestExchangeRates(t.Context())
	if err != nil {
		t.Fatalf("read latest generation: %v", err)
	}
	if latest.Rates[0].UnscaledRate != "36725000000" {
		t.Fatalf("published snapshot leaked caller mutation: rate = %q", latest.Rates[0].UnscaledRate)
	}

	second := testExchangeRateSnapshot(2)
	second.Rates[1].UnscaledRate = "9100000000"
	if err := store.PublishExchangeRates(t.Context(), second); err != nil {
		t.Fatalf("publish generation 2: %v", err)
	}
	if err := store.PublishExchangeRates(t.Context(), second); err != nil {
		t.Fatalf("retry generation 2: %v", err)
	}

	pinned, err := store.ExchangeRateGeneration(t.Context(), 1)
	if err != nil {
		t.Fatalf("read generation 1: %v", err)
	}
	if pinned.Rates[1].UnscaledRate != "9049000000" {
		t.Fatalf("generation 1 rate = %q, want 9049000000", pinned.Rates[1].UnscaledRate)
	}
	conflict := testExchangeRateSnapshot(2)
	if err := store.PublishExchangeRates(t.Context(), conflict); !errors.Is(err, ErrExchangeRateGenerationConflict) {
		t.Fatalf("changed generation error = %v, want ErrExchangeRateGenerationConflict", err)
	}
	if err := store.PublishExchangeRates(t.Context(), testExchangeRateSnapshot(1)); !errors.Is(err, ErrExchangeRateGenerationRegression) {
		t.Fatalf("regressed generation error = %v, want ErrExchangeRateGenerationRegression", err)
	}
}

func TestExchangeRateSnapshotValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ExchangeRateSnapshot)
	}{
		{name: "protocol", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.ProtocolVersion++ }},
		{name: "schema", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.SchemaVersion++ }},
		{name: "generation", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Generation = 0 }},
		{name: "base currency", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.BaseCurrency = "EUR" }},
		{name: "decimal scale", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.DecimalScale = 4 }},
		{name: "missing rates", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates = nil }},
		{name: "empty rates", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates = []ExchangeRateEntry{} }},
		{name: "lowercase currency", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].Currency = "aed" }},
		{name: "invalid currency", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].Currency = "USDT" }},
		{name: "noncanonical date", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].EffectiveDate = "2024-1-01" }},
		{name: "invalid date", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].EffectiveDate = "2024-02-30" }},
		{name: "negative rate", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].UnscaledRate = "-1" }},
		{name: "leading zero rate", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].UnscaledRate = "01" }},
		{name: "rate exceeds Decimal64", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[0].UnscaledRate = "1000000000000000000" }},
		{name: "base rate", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[2].UnscaledRate = "9999999999" }},
		{name: "missing base rate", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates = snapshot.Rates[:2] }},
		{name: "duplicate entry", mutate: func(snapshot *ExchangeRateSnapshot) { snapshot.Rates[1] = snapshot.Rates[0] }},
		{name: "unsorted entries", mutate: func(snapshot *ExchangeRateSnapshot) {
			snapshot.Rates[0], snapshot.Rates[1] = snapshot.Rates[1], snapshot.Rates[0]
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testExchangeRateSnapshot(1)
			test.mutate(snapshot)
			if _, err := normalizeAndValidateExchangeRateSnapshot(snapshot); !errors.Is(err, ErrInvalidExchangeRateSnapshot) {
				t.Fatalf("validation error = %v, want ErrInvalidExchangeRateSnapshot", err)
			}
		})
	}
}

func TestDecodeExchangeRateSnapshotRejectsIncompatibleJSON(t *testing.T) {
	valid := `{"protocolVersion":1,"schemaVersion":1,"generation":1,"baseCurrency":"USD","decimalScale":10,"rates":[{"currency":"USD","effectiveDate":"1970-01-01","unscaledRate":"10000000000"}]}`
	tests := map[string]string{
		"unknown field":          strings.Replace(valid, `"generation":1`, `"generation":1,"unknown":true`, 1),
		"missing required field": strings.Replace(valid, `,"decimalScale":10`, ``, 1),
		"trailing document":      valid + `{}`,
		"null document":          `null`,
		"null entry":             strings.Replace(valid, `{"currency":"USD","effectiveDate":"1970-01-01","unscaledRate":"10000000000"}`, `null`, 1),
	}

	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeExchangeRateSnapshot(strings.NewReader(document)); err == nil {
				t.Fatal("decoder accepted an incompatible document")
			}
		})
	}
}

func TestExchangeRateSnapshotRoundTrip(t *testing.T) {
	snapshot := testExchangeRateSnapshot(7)
	normalized, err := normalizeAndValidateExchangeRateSnapshot(snapshot)
	if err != nil {
		t.Fatalf("validate snapshot: %v", err)
	}
	if !reflect.DeepEqual(normalized, snapshot) {
		t.Fatalf("normalized snapshot changed\n got: %#v\nwant: %#v", normalized, snapshot)
	}
}

func testExchangeRateSnapshot(generation int64) *ExchangeRateSnapshot {
	return &ExchangeRateSnapshot{
		ProtocolVersion: ExchangeRateProtocolVersion,
		SchemaVersion:   ExchangeRateSchemaVersion,
		Generation:      generation,
		BaseCurrency:    ExchangeRateBaseCurrency,
		DecimalScale:    ExchangeRateDecimalScale,
		Rates: []ExchangeRateEntry{
			{Currency: "AED", EffectiveDate: "2024-01-01", UnscaledRate: "36725000000"},
			{Currency: "EUR", EffectiveDate: "2024-01-01", UnscaledRate: "9049000000"},
			{Currency: "USD", EffectiveDate: "1970-01-01", UnscaledRate: "10000000000"},
		},
	}
}
