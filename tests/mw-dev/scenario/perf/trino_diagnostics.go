package perf

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/trinodb/trino-go-client/trino"
)

var diagnosticSymbol = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_.$<>]*$`)

// Only structured codes and Java symbols enter public scenario artifacts.
// Free-form messages, SQL, URLs and driver errors may contain private values.
func trinoFailureDiagnostic(err error) string {
	var failure *trino.ErrTrino
	if errors.As(err, &failure) {
		symbol := func(value string) string {
			if len(value) <= 256 && diagnosticSymbol.MatchString(value) {
				return value
			}
			return "unavailable"
		}
		parts := []string{fmt.Sprintf("code=%d name=%s type=%s", failure.ErrorCode, symbol(failure.ErrorName), symbol(failure.ErrorType))}
		for info, depth := &failure.FailureInfo, 0; info != nil && depth < 6; info, depth = info.Cause, depth+1 {
			parts = append(parts, "cause="+symbol(info.Type))
			for index, frame := range info.Stack {
				if index >= 8 {
					break
				}
				// Keep the method symbol only, dropping source names and any other text.
				method, _, _ := strings.Cut(frame, "(")
				parts = append(parts, "at="+symbol(method))
			}
		}
		return strings.Join(parts, " ")
	}
	var queryFailure *trino.ErrQueryFailed
	if errors.As(err, &queryFailure) {
		return fmt.Sprintf("http_status=%d", queryFailure.StatusCode)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	return fmt.Sprintf("driver_error_type=%T", err)
}

func retainTrinoFailure(phase string, err error, previous *string) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	current := trinoFailureDiagnostic(err)
	if current != *previous {
		fmt.Printf("Cached Trino %s failure: %s\n", phase, current)
		*previous = current
	}
}
