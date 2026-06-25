package provision

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	ErrorClassConfig             = "configuration_error"
	ErrorClassProvisionAPI       = "provision_api_error"
	ErrorClassProvisionFailed    = "provision_failed"
	ErrorClassWaitTimeout        = "wait_timeout"
	ErrorClassCleanupError       = "cleanup_error"
	ErrorClassCleanupTimeout     = "cleanup_timeout"
	ErrorClassUnsupportedStep    = "unsupported_step"
	ErrorClassInvalidStepConfig  = "invalid_step_config"
	ErrorClassProvisionStepError = "provision_step_error"
)

var (
	ErrUnexpectedStatus = errors.New("unexpected provisioning api status")
	ErrWarehouseFailed  = errors.New("warehouse failed")
	ErrWaitTimeout      = errors.New("warehouse wait timeout")
)

type classifiedError struct {
	class string
	err   error
}

func (e classifiedError) Error() string {
	return e.err.Error()
}

func (e classifiedError) Unwrap() error {
	return e.err
}

func (e classifiedError) ErrorClass() string {
	return e.class
}

func classified(class string, err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{class: class, err: err}
}

type APIError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s %s: %v: HTTP %d", e.Method, e.Path, ErrUnexpectedStatus, e.StatusCode)
	}
	return fmt.Sprintf("%s %s: %v: HTTP %d: %s", e.Method, e.Path, ErrUnexpectedStatus, e.StatusCode, e.Body)
}

func (e *APIError) Unwrap() error {
	return ErrUnexpectedStatus
}

func (e *APIError) ErrorClass() string {
	return ErrorClassProvisionAPI
}

func (e *APIError) NotFound() bool {
	return e.StatusCode == http.StatusNotFound
}
