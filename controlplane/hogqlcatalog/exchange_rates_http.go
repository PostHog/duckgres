package hogqlcatalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/gin-gonic/gin"
)

const maxExchangeRateSnapshotRequestBytes = 32 << 20

type exchangeRateAPIHandler struct {
	reader    ExchangeRateReader
	publisher ExchangeRatePublisher
}

func RegisterExchangeRateAPI(readRouter, publishRouter gin.IRouter, reader ExchangeRateReader, publisher ExchangeRatePublisher) {
	handler := &exchangeRateAPIHandler{reader: reader, publisher: publisher}
	publishRouter.PUT("/compatibility/exchange-rates", handler.publish)
	readRouter.GET("/compatibility/exchange-rates", handler.read)
}

func DecodeExchangeRateSnapshot(reader io.Reader) (*ExchangeRateSnapshot, error) {
	document, err := io.ReadAll(io.LimitReader(reader, maxExchangeRateSnapshotRequestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read HogQL exchange-rate snapshot: %w", err)
	}
	if len(document) > maxExchangeRateSnapshotRequestBytes {
		return nil, errors.New("decode HogQL exchange-rate snapshot: document exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var snapshot ExchangeRateSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode HogQL exchange-rate snapshot: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return nil, err
	}
	if err := validateRequiredJSONFields(document, reflect.TypeFor[ExchangeRateSnapshot](), "snapshot"); err != nil {
		return nil, err
	}
	return normalizeAndValidateExchangeRateSnapshot(&snapshot)
}

func (h *exchangeRateAPIHandler) publish(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxExchangeRateSnapshotRequestBytes)
	snapshot, err := DecodeExchangeRateSnapshot(c.Request.Body)
	if err != nil {
		writeError(c, http.StatusBadRequest, "HOGQL_EXCHANGE_RATES_INVALID_SNAPSHOT", "invalid HogQL exchange-rate snapshot")
		return
	}
	if err := h.publisher.PublishExchangeRates(c.Request.Context(), snapshot); err != nil {
		switch {
		case errors.Is(err, ErrInvalidExchangeRateSnapshot):
			writeError(c, http.StatusBadRequest, "HOGQL_EXCHANGE_RATES_INVALID_SNAPSHOT", "invalid HogQL exchange-rate snapshot")
		case errors.Is(err, ErrExchangeRateGenerationRegression), errors.Is(err, ErrExchangeRateGenerationConflict):
			writeError(c, http.StatusConflict, "HOGQL_EXCHANGE_RATES_GENERATION_CONFLICT", "snapshot generation conflicts with the published exchange rates")
		default:
			writeError(c, http.StatusServiceUnavailable, "HOGQL_EXCHANGE_RATES_UNAVAILABLE", "HogQL exchange rates are unavailable")
		}
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *exchangeRateAPIHandler) read(c *gin.Context) {
	generation, err := decodeExchangeRateCompatibilityRequest(c)
	if err != nil {
		if errors.Is(err, errProtocolMismatch) {
			writeError(c, http.StatusConflict, "HOGQL_EXCHANGE_RATES_PROTOCOL_MISMATCH", "requested protocol version is not supported")
			return
		}
		writeError(c, http.StatusBadRequest, "HOGQL_EXCHANGE_RATES_INVALID_REQUEST", "invalid exchange-rate compatibility request")
		return
	}

	var snapshot *ExchangeRateSnapshot
	if generation == 0 {
		snapshot, err = h.reader.LatestExchangeRates(c.Request.Context())
	} else {
		snapshot, err = h.reader.ExchangeRateGeneration(c.Request.Context(), generation)
	}
	if err != nil {
		switch {
		case errors.Is(err, ErrExchangeRatesNotFound):
			writeError(c, http.StatusNotFound, "HOGQL_EXCHANGE_RATES_NOT_FOUND", "HogQL exchange rates were not found")
		case errors.Is(err, ErrExchangeRateGenerationNotFound):
			writeError(c, http.StatusNotFound, "HOGQL_EXCHANGE_RATES_GENERATION_NOT_FOUND", "HogQL exchange-rate generation was not found")
		default:
			writeError(c, http.StatusServiceUnavailable, "HOGQL_EXCHANGE_RATES_UNAVAILABLE", "HogQL exchange rates are unavailable")
		}
		return
	}
	if snapshot == nil || generation != 0 && snapshot.Generation != generation {
		writeError(c, http.StatusConflict, "HOGQL_EXCHANGE_RATES_GENERATION_MISMATCH", "published snapshot does not match requested exchange-rate generation")
		return
	}
	c.Header("ETag", fmt.Sprintf(`"hogql-exchange-rates-%d"`, snapshot.Generation))
	c.JSON(http.StatusOK, snapshot)
}

func decodeExchangeRateCompatibilityRequest(c *gin.Context) (int64, error) {
	query := c.Request.URL.Query()
	if len(query) < 1 || len(query) > 2 {
		return 0, errors.New("unexpected query fields")
	}
	for name := range query {
		if name != "protocolVersion" && name != "generation" {
			return 0, errors.New("unexpected query field")
		}
	}
	protocolVersion, ok := singleQueryValue(query, "protocolVersion")
	if !ok {
		return 0, errors.New("protocolVersion is required")
	}
	if protocolVersion != strconv.Itoa(ExchangeRateProtocolVersion) {
		if _, err := strconv.Atoi(protocolVersion); err != nil {
			return 0, errors.New("protocolVersion must be an integer")
		}
		return 0, errProtocolMismatch
	}
	generationValue, present := query["generation"]
	if !present {
		return 0, nil
	}
	if len(generationValue) != 1 {
		return 0, errors.New("generation must occur once")
	}
	generation, err := strconv.ParseInt(generationValue[0], 10, 64)
	if err != nil || generation <= 0 {
		return 0, errors.New("generation must be a positive integer")
	}
	return generation, nil
}
