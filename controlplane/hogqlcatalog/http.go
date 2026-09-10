package hogqlcatalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

const maxSnapshotRequestBytes = 2 << 20

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type apiHandler struct {
	reader    Reader
	publisher Publisher
}

func RegisterAPI(readRouter, publishRouter gin.IRouter, reader Reader, publisher Publisher) {
	handler := &apiHandler{reader: reader, publisher: publisher}
	publishRouter.PUT("/compatibility/semantic-catalog", handler.publish)
	readRouter.GET("/compatibility/semantic-catalog", handler.read)
}

func DecodeSnapshot(reader io.Reader) (*HogQLSemanticCatalogSnapshot, error) {
	document, err := io.ReadAll(io.LimitReader(reader, maxSnapshotRequestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read HogQL semantic catalog snapshot: %w", err)
	}
	if len(document) > maxSnapshotRequestBytes {
		return nil, errors.New("decode HogQL semantic catalog snapshot: document exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var snapshot HogQLSemanticCatalogSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode HogQL semantic catalog snapshot: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return nil, err
	}
	if err := validateRequiredJSONFields(document, reflect.TypeFor[HogQLSemanticCatalogSnapshot](), "snapshot"); err != nil {
		return nil, err
	}
	return normalizeAndValidateSnapshot(&snapshot)
}

func validateRequiredJSONFields(document []byte, valueType reflect.Type, path string) error {
	if bytes.Equal(bytes.TrimSpace(document), []byte("null")) {
		return fmt.Errorf("decode HogQL semantic catalog snapshot: %s is null", path)
	}
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	switch valueType.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(document, &object); err != nil {
			return fmt.Errorf("decode HogQL semantic catalog snapshot: %s must be an object: %w", path, err)
		}
		for index := range valueType.NumField() {
			field := valueType.Field(index)
			jsonTag := strings.Split(field.Tag.Get("json"), ",")
			jsonName := jsonTag[0]
			if jsonName == "" || jsonName == "-" {
				continue
			}
			fieldDocument, exists := object[jsonName]
			if slices.Contains(jsonTag[1:], "omitempty") && !exists {
				continue
			}
			if !exists {
				return fmt.Errorf("decode HogQL semantic catalog snapshot: %s.%s is required", path, jsonName)
			}
			if err := validateRequiredJSONFields(fieldDocument, field.Type, path+"."+jsonName); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if err := json.Unmarshal(document, &values); err != nil {
			return fmt.Errorf("decode HogQL semantic catalog snapshot: %s must be an array: %w", path, err)
		}
		for index, value := range values {
			if err := validateRequiredJSONFields(value, valueType.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode HogQL semantic catalog snapshot: trailing JSON document")
		}
		return fmt.Errorf("decode HogQL semantic catalog snapshot trailing data: %w", err)
	}
	return nil
}

func (h *apiHandler) publish(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSnapshotRequestBytes)
	snapshot, err := DecodeSnapshot(c.Request.Body)
	if err != nil {
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_MANIFEST", "invalid HogQL semantic catalog snapshot")
		return
	}
	if err := h.publisher.Publish(c.Request.Context(), snapshot); err != nil {
		switch {
		case errors.Is(err, ErrInvalidSnapshot):
			writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_MANIFEST", "invalid HogQL semantic catalog snapshot")
		case errors.Is(err, ErrGenerationRegression), errors.Is(err, ErrGenerationConflict):
			writeError(c, http.StatusConflict, "HOGQL_CATALOG_GENERATION_CONFLICT", "snapshot generation conflicts with the published catalog")
		default:
			writeError(c, http.StatusServiceUnavailable, "HOGQL_CATALOG_UNAVAILABLE", "HogQL semantic catalog is unavailable")
		}
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *apiHandler) read(c *gin.Context) {
	request, err := decodeCompatibilityRequest(c)
	if err != nil {
		if errors.Is(err, errProtocolMismatch) {
			writeError(c, http.StatusConflict, "HOGQL_CATALOG_PROTOCOL_MISMATCH", "requested protocol version is not supported")
			return
		}
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_REQUEST", "invalid semantic catalog compatibility request")
		return
	}

	var snapshot *HogQLSemanticCatalogSnapshot
	if request.generation == 0 {
		snapshot, err = h.reader.Latest(c.Request.Context(), request.catalog)
	} else {
		snapshot, err = h.reader.Generation(c.Request.Context(), request.catalog, request.generation)
	}
	if err != nil {
		switch {
		case errors.Is(err, ErrCatalogNotFound):
			writeError(c, http.StatusNotFound, "HOGQL_CATALOG_NOT_FOUND", "HogQL semantic catalog was not found")
		case errors.Is(err, ErrGenerationNotFound):
			writeError(c, http.StatusNotFound, "HOGQL_CATALOG_GENERATION_NOT_FOUND", "HogQL semantic catalog generation was not found")
		default:
			writeError(c, http.StatusServiceUnavailable, "HOGQL_CATALOG_UNAVAILABLE", "HogQL semantic catalog is unavailable")
		}
		return
	}
	if snapshot == nil || snapshot.Catalog != request.catalog {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_MISMATCH", "published snapshot does not match requested catalog")
		return
	}
	if request.generation != 0 && snapshot.Generation != request.generation {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_GENERATION_MISMATCH", "published snapshot does not match requested generation")
		return
	}
	if snapshot.LanguageVersion != request.languageVersion {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_LANGUAGE_MISMATCH", "published snapshot does not match requested language version")
		return
	}
	c.Header("ETag", fmt.Sprintf(`"hogql-%d"`, snapshot.Generation))
	c.JSON(http.StatusOK, snapshot)
}

var errProtocolMismatch = errors.New("HogQL catalog protocol mismatch")

type compatibilityRequest struct {
	catalog         PhysicalIdentifier
	languageVersion string
	generation      int64
}

func decodeCompatibilityRequest(c *gin.Context) (compatibilityRequest, error) {
	query := c.Request.URL.Query()
	if len(query) < 4 || len(query) > 5 {
		return compatibilityRequest{}, errors.New("unexpected query fields")
	}
	for name := range query {
		if name != "protocolVersion" && name != "languageVersion" && name != "catalog" && name != "catalogDelimited" && name != "generation" {
			return compatibilityRequest{}, errors.New("unexpected query field")
		}
	}
	protocolVersion, ok := singleQueryValue(query, "protocolVersion")
	if !ok {
		return compatibilityRequest{}, errors.New("protocolVersion is required")
	}
	if protocolVersion != strconv.Itoa(SnapshotProtocolVersion) {
		if _, err := strconv.Atoi(protocolVersion); err != nil {
			return compatibilityRequest{}, errors.New("protocolVersion must be an integer")
		}
		return compatibilityRequest{}, errProtocolMismatch
	}
	languageVersion, ok := singleQueryValue(query, "languageVersion")
	if !ok || !languageVersionPattern.MatchString(languageVersion) {
		return compatibilityRequest{}, errors.New("languageVersion is invalid")
	}
	catalogValue, ok := singleQueryValue(query, "catalog")
	if !ok {
		return compatibilityRequest{}, errors.New("catalog is required")
	}
	catalogDelimitedValue, ok := singleQueryValue(query, "catalogDelimited")
	if !ok || (catalogDelimitedValue != "true" && catalogDelimitedValue != "false") {
		return compatibilityRequest{}, errors.New("catalogDelimited must be a boolean")
	}
	catalog, err := normalizedCatalog(PhysicalIdentifier{
		Value:     catalogValue,
		Delimited: catalogDelimitedValue == "true",
	})
	if err != nil {
		return compatibilityRequest{}, err
	}

	var generation int64
	if generationValue, exists := query["generation"]; exists {
		if len(generationValue) != 1 {
			return compatibilityRequest{}, errors.New("generation must appear once")
		}
		generation, err = strconv.ParseInt(generationValue[0], 10, 64)
		if err != nil || generation <= 0 {
			return compatibilityRequest{}, errors.New("generation must be a positive integer")
		}
	}
	return compatibilityRequest{catalog: catalog, languageVersion: languageVersion, generation: generation}, nil
}

func singleQueryValue(query map[string][]string, name string) (string, bool) {
	values, exists := query[name]
	if !exists || len(values) != 1 {
		return "", false
	}
	return values[0], true
}

func writeError(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, errorResponse{Code: code, Message: message})
}
