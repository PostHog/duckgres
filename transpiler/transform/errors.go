package transform

// CodedError is a transform-detected error that carries an explicit PostgreSQL
// SQLSTATE. conn_errors.go reads the code via the SQLState() method when sending the
// error to the client; errors without a code default to 42704.
type CodedError struct {
	Code    string
	Message string
}

func (e *CodedError) Error() string    { return e.Message }
func (e *CodedError) SQLState() string { return e.Code }

// NewFeatureNotSupported returns a 0A000 (feature_not_supported) error. Use it
// for PostgreSQL features the active backend cannot honor (e.g. ON CONFLICT ON
// CONSTRAINT or arbitrary ALTER COLUMN TYPE ... USING on a constraint-less
// catalog), so clients receive a clean, predictable PostgreSQL error instead of
// a raw DuckDB/Flight failure.
func NewFeatureNotSupported(message string) *CodedError {
	return &CodedError{Code: "0A000", Message: message}
}
