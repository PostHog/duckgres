package perf

const (
	ErrorClassConfig          = "perf_config"
	ErrorClassPerf            = "perf_execution"
	ErrorClassUnsupportedStep = "unsupported_step"
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
	return classifiedError{class: class, err: err}
}
