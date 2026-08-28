package hogqlcatalog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	ExchangeRateProtocolVersion = 1
	ExchangeRateSchemaVersion   = 1
	ExchangeRateDecimalScale    = 10
	ExchangeRateBaseCurrency    = "USD"
	maximumExchangeRateEntries  = 1_000_000
)

var (
	ErrInvalidExchangeRateSnapshot      = errors.New("invalid HogQL exchange-rate snapshot")
	ErrExchangeRatesNotFound            = errors.New("HogQL exchange rates not found")
	ErrExchangeRateGenerationNotFound   = errors.New("HogQL exchange-rate generation not found")
	ErrExchangeRateGenerationRegression = errors.New("HogQL exchange-rate generation regressed")
	ErrExchangeRateGenerationConflict   = errors.New("HogQL exchange-rate generation conflicts with published content")
	exchangeRateCurrencyPattern         = regexp.MustCompile(`^[A-Z]{3}$`)
	exchangeRateUnscaledValuePattern    = regexp.MustCompile(`^(0|[1-9][0-9]{0,17})$`)
)

type ExchangeRateSnapshot struct {
	ProtocolVersion int                 `json:"protocolVersion"`
	SchemaVersion   int                 `json:"schemaVersion"`
	Generation      int64               `json:"generation"`
	BaseCurrency    string              `json:"baseCurrency"`
	DecimalScale    int                 `json:"decimalScale"`
	Rates           []ExchangeRateEntry `json:"rates"`
}

type ExchangeRateEntry struct {
	Currency      string `json:"currency"`
	EffectiveDate string `json:"effectiveDate"`
	UnscaledRate  string `json:"unscaledRate"`
}

func normalizeAndValidateExchangeRateSnapshot(snapshot *ExchangeRateSnapshot) (*ExchangeRateSnapshot, error) {
	if snapshot == nil {
		return nil, invalidExchangeRateSnapshot("snapshot is null")
	}
	if snapshot.ProtocolVersion != ExchangeRateProtocolVersion {
		return nil, invalidExchangeRateSnapshot("unsupported protocolVersion %d", snapshot.ProtocolVersion)
	}
	if snapshot.SchemaVersion != ExchangeRateSchemaVersion {
		return nil, invalidExchangeRateSnapshot("unsupported schemaVersion %d", snapshot.SchemaVersion)
	}
	if snapshot.Generation <= 0 {
		return nil, invalidExchangeRateSnapshot("generation must be positive")
	}
	if snapshot.BaseCurrency != ExchangeRateBaseCurrency {
		return nil, invalidExchangeRateSnapshot("baseCurrency must be %q", ExchangeRateBaseCurrency)
	}
	if snapshot.DecimalScale != ExchangeRateDecimalScale {
		return nil, invalidExchangeRateSnapshot("decimalScale must be %d", ExchangeRateDecimalScale)
	}
	if len(snapshot.Rates) == 0 {
		return nil, invalidExchangeRateSnapshot("rates must be a non-empty array")
	}
	if len(snapshot.Rates) > maximumExchangeRateEntries {
		return nil, invalidExchangeRateSnapshot("rates exceeds the entry limit")
	}

	normalized := cloneExchangeRateSnapshot(snapshot)
	baseCurrencyPresent := false
	for index, rate := range normalized.Rates {
		if !exchangeRateCurrencyPattern.MatchString(rate.Currency) {
			return nil, invalidExchangeRateSnapshot("rate %d has a noncanonical currency", index)
		}
		parsedDate, err := time.Parse(time.DateOnly, rate.EffectiveDate)
		if err != nil || parsedDate.Format(time.DateOnly) != rate.EffectiveDate {
			return nil, invalidExchangeRateSnapshot("rate %d has a noncanonical effectiveDate", index)
		}
		if !exchangeRateUnscaledValuePattern.MatchString(rate.UnscaledRate) {
			return nil, invalidExchangeRateSnapshot("rate %d has a noncanonical unscaledRate", index)
		}
		if rate.Currency == ExchangeRateBaseCurrency {
			baseCurrencyPresent = true
			if rate.UnscaledRate != "10000000000" {
				return nil, invalidExchangeRateSnapshot("base-currency rates must equal one")
			}
		}
		if index > 0 && compareExchangeRateEntries(normalized.Rates[index-1], rate) >= 0 {
			return nil, invalidExchangeRateSnapshot("rates must be strictly sorted by currency and effectiveDate")
		}
	}
	if !baseCurrencyPresent {
		return nil, invalidExchangeRateSnapshot("rates must include the base currency")
	}
	return normalized, nil
}

func compareExchangeRateEntries(left, right ExchangeRateEntry) int {
	if comparison := strings.Compare(left.Currency, right.Currency); comparison != 0 {
		return comparison
	}
	return strings.Compare(left.EffectiveDate, right.EffectiveDate)
}

func cloneExchangeRateSnapshot(snapshot *ExchangeRateSnapshot) *ExchangeRateSnapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.Rates = append([]ExchangeRateEntry(nil), snapshot.Rates...)
	return &cloned
}

func invalidExchangeRateSnapshot(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidExchangeRateSnapshot, fmt.Sprintf(format, arguments...))
}
