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

func RegisterAPI(router gin.IRouter, reader Reader, publisher Publisher) {
	handler := &apiHandler{reader: reader, publisher: publisher}
	router.PUT("/catalogs/:catalog/snapshots", handler.publish)
	router.GET("/catalogs/:catalog/snapshots/latest", handler.latest)
	router.GET("/catalogs/:catalog/snapshots/:generation", handler.generation)
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
			jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
			if jsonName == "" || jsonName == "-" {
				continue
			}
			fieldDocument, exists := object[jsonName]
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
	requestedCatalog, err := catalogFromPath(c.Param("catalog"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_REQUEST", "invalid catalog identifier")
		return
	}
	if snapshot.Catalog != requestedCatalog {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_MISMATCH", "snapshot catalog does not match requested catalog")
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

func (h *apiHandler) latest(c *gin.Context) {
	h.read(c, 0)
}

func (h *apiHandler) generation(c *gin.Context) {
	generation, err := strconv.ParseInt(c.Param("generation"), 10, 64)
	if err != nil || generation <= 0 {
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_REQUEST", "generation must be a positive integer")
		return
	}
	h.read(c, generation)
}

func (h *apiHandler) read(c *gin.Context, generation int64) {
	catalog, err := catalogFromPath(c.Param("catalog"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_REQUEST", "invalid catalog identifier")
		return
	}
	languageVersion, ok := requestedLanguageVersion(c)
	if !ok {
		writeError(c, http.StatusBadRequest, "HOGQL_CATALOG_INVALID_REQUEST", "languageVersion is required and must be a supported version string")
		return
	}

	var snapshot *HogQLSemanticCatalogSnapshot
	if generation == 0 {
		snapshot, err = h.reader.Latest(c.Request.Context(), catalog)
	} else {
		snapshot, err = h.reader.Generation(c.Request.Context(), catalog, generation)
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
	if snapshot == nil || snapshot.Catalog != catalog {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_MISMATCH", "published snapshot does not match requested catalog")
		return
	}
	if generation != 0 && snapshot.Generation != generation {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_GENERATION_MISMATCH", "published snapshot does not match requested generation")
		return
	}
	if snapshot.LanguageVersion != languageVersion {
		writeError(c, http.StatusConflict, "HOGQL_CATALOG_LANGUAGE_MISMATCH", "published snapshot does not match requested language version")
		return
	}
	c.Header("ETag", fmt.Sprintf(`"hogql-%s-%d"`, catalog.Value, snapshot.Generation))
	c.JSON(http.StatusOK, snapshot)
}

func requestedLanguageVersion(c *gin.Context) (string, bool) {
	query := c.Request.URL.Query()
	values, exists := query["languageVersion"]
	if !exists || len(query) != 1 || len(values) != 1 || !languageVersionPattern.MatchString(values[0]) {
		return "", false
	}
	return values[0], true
}

func catalogFromPath(value string) (PhysicalIdentifier, error) {
	return normalizedCatalog(PhysicalIdentifier{Value: value})
}

func writeError(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, errorResponse{Code: code, Message: message})
}
