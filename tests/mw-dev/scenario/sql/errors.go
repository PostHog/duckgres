package sql

import (
	"errors"
	"fmt"
)

const (
	ErrorClassConfig            = "sql_configuration_error"
	ErrorClassInvalidStepConfig = "invalid_step_config"
	ErrorClassSQL               = "sql_error"
	ErrorClassTransientTimeout  = "transient_sql_timeout"
	ErrorClassUnsupportedStep   = "unsupported_step"
)

var ErrTransientRetriesExhausted = errors.New("transient SQL retries exhausted")

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

func invalidStep(stepID, format string, args ...any) error {
	return classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s: %s", stepID, fmt.Sprintf(format, args...)))
}
