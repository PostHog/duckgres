package duckdbservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/pgbinary"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// IsCopyFromStdinDescriptor reports whether desc is the custom DoPut
// descriptor used by the control plane to stream a COPY input spool file for
// COPY FROM STDIN. customActionServer.DoPut peeks at the first frame
// of every DoPut stream and routes here when this returns true.
func IsCopyFromStdinDescriptor(desc *flight.FlightDescriptor) bool {
	if desc == nil {
		return false
	}
	if desc.Type != flight.DescriptorPATH {
		return false
	}
	if len(desc.Path) == 0 {
		return false
	}
	return desc.Path[0] == flightclient.CopyFromStdinDescriptorPath
}

func substituteCopyFromStdinPlaceholders(copySQL, tmpPath string, bytesWritten int64) string {
	result := strings.ReplaceAll(copySQL, flightclient.CopyFromStdinPathPlaceholder, tmpPath)
	return strings.ReplaceAll(result, flightclient.CopyFromStdinSizePlaceholder, strconv.FormatInt(bytesWritten, 10))
}

func copyFromStdinDescriptorRequest(desc *flight.FlightDescriptor) (string, []string, error) {
	if desc == nil || len(desc.Cmd) == 0 {
		return "", nil, status.Error(codes.InvalidArgument, "copy-from-stdin: missing COPY SQL in descriptor.Cmd")
	}
	if len(desc.Path) == 1 {
		copySQL := string(desc.Cmd)
		// Unversioned descriptors are the legacy text/CSV protocol and always
		// carry a COPY statement. Native PostgreSQL binary loads use the
		// versioned structured request below; reject any unversioned non-COPY
		// command without inferring its format from table or function names.
		fields := strings.Fields(copySQL)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "COPY") {
			return "", nil, status.Error(codes.InvalidArgument,
				"copy-from-stdin: native PostgreSQL binary COPY is missing exact schema metadata")
		}
		return copySQL, nil, nil
	}
	if len(desc.Path) != 2 || desc.Path[1] != flightclient.CopyFromStdinPostgresBinaryPathVersion {
		return "", nil, status.Error(codes.InvalidArgument, "copy-from-stdin: unsupported binary COPY schema metadata")
	}
	var request sqlcore.CopyFromStdinRequest
	if err := json.Unmarshal(desc.Cmd, &request); err != nil {
		return "", nil, status.Errorf(codes.InvalidArgument, "copy-from-stdin: invalid binary COPY request: %v", err)
	}
	if request.SQLTemplate == "" || len(request.PostgresBinaryDatabaseTypeNames) == 0 {
		return "", nil, status.Error(codes.InvalidArgument, "copy-from-stdin: incomplete binary COPY request")
	}
	return request.SQLTemplate, request.PostgresBinaryDatabaseTypeNames, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

type copyFromStdinPreparationIOError struct {
	err error
}

func (e *copyFromStdinPreparationIOError) Error() string { return e.err.Error() }
func (e *copyFromStdinPreparationIOError) Unwrap() error { return e.err }

func copyFromStdinPreparationIOFailure(err error) error {
	return &copyFromStdinPreparationIOError{err: err}
}

func copyFromStdinPreparationErrorClass(err error) copyFromStdinErrorClass {
	var ioErr *copyFromStdinPreparationIOError
	if errors.As(err, &ioErr) {
		return copyFromStdinErrorInternal
	}
	return copyFromStdinErrorInvalidInput
}

type copyFromStdinPreparationReader struct {
	r io.Reader
}

func (r *copyFromStdinPreparationReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return n, err
	}
	return n, copyFromStdinPreparationIOFailure(err)
}

type copyFromStdinPreparationWriter struct {
	w io.Writer
}

func (w *copyFromStdinPreparationWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err != nil {
		return n, copyFromStdinPreparationIOFailure(err)
	}
	return n, nil
}

const copyFromStdinErrorDomain = "duckgres.copy"

type copyFromStdinErrorClass uint8

const (
	copyFromStdinErrorInternal copyFromStdinErrorClass = iota
	copyFromStdinErrorInvalidRequest
	copyFromStdinErrorInvalidInput
	copyFromStdinErrorLoadRejected
	copyFromStdinErrorCanceled
	copyFromStdinErrorDeadlineExceeded
	copyFromStdinErrorUnavailable
)

func (c copyFromStdinErrorClass) reasonAndMessage() (reason, message string) {
	switch c {
	case copyFromStdinErrorInvalidRequest:
		return "INVALID_REQUEST", "copy-from-stdin: invalid request"
	case copyFromStdinErrorInvalidInput:
		return "INVALID_INPUT", "copy-from-stdin: invalid input"
	case copyFromStdinErrorLoadRejected:
		return "LOAD_REJECTED", "copy-from-stdin: load rejected"
	case copyFromStdinErrorCanceled:
		return "CANCELED", "copy-from-stdin: canceled"
	case copyFromStdinErrorDeadlineExceeded:
		return "DEADLINE_EXCEEDED", "copy-from-stdin: deadline exceeded"
	case copyFromStdinErrorUnavailable:
		return "UNAVAILABLE", "copy-from-stdin: unavailable"
	default:
		return "INTERNAL", "copy-from-stdin: internal error"
	}
}

// sanitizeCopyFromStdinRPCError is the sole error boundary for the custom COPY
// DoPut RPC. DuckDB errors can echo generated SQL, input values, and worker-local
// spool paths, so only a fixed classification is allowed to cross this boundary.
// The original gRPC code is retained; ErrorInfo provides a stable reason for
// rolling control-plane/worker compatibility without carrying raw details.
func sanitizeCopyFromStdinRPCError(err error, class copyFromStdinErrorClass) error {
	if err == nil {
		return nil
	}

	code := status.Code(err)
	switch {
	case errors.Is(err, context.Canceled) || code == codes.Canceled:
		class = copyFromStdinErrorCanceled
		if code == codes.Unknown {
			code = codes.Canceled
		}
	case errors.Is(err, context.DeadlineExceeded) || code == codes.DeadlineExceeded:
		class = copyFromStdinErrorDeadlineExceeded
		if code == codes.Unknown {
			code = codes.DeadlineExceeded
		}
	}
	if code == codes.OK {
		code = codes.Unknown
	}

	reason, message := class.reasonAndMessage()
	sanitized := status.New(code, message)
	withDetails, detailsErr := sanitized.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: copyFromStdinErrorDomain,
	})
	if detailsErr != nil {
		return sanitized.Err()
	}
	return withDetails.Err()
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func preparePostgresBinaryCopy(ctx context.Context, tmpPath string, databaseTypeNames []string) (string, int64, func(), error) {
	if err := ctx.Err(); err != nil {
		return "", 0, func() {}, err
	}
	schema, err := pgbinary.SchemaFromDatabaseTypes(databaseTypeNames)
	if err != nil {
		return "", 0, func() {}, err
	}

	source, err := os.Open(tmpPath)
	if err != nil {
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("open binary COPY spool: %w", err))
	}
	inspection, inspectErr := pgbinary.InspectProtocolCompleted(&copyFromStdinPreparationReader{
		r: &contextReader{ctx: ctx, r: source},
	}, schema)
	closeErr := source.Close()
	if inspectErr != nil {
		return "", 0, func() {}, inspectErr
	}
	if closeErr != nil {
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("close binary COPY spool: %w", closeErr))
	}

	if !inspection.NeedsRewrite {
		info, err := os.Stat(tmpPath)
		if err != nil {
			return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("stat binary COPY spool: %w", err))
		}
		return tmpPath, info.Size(), func() {}, nil
	}

	source, err = os.Open(tmpPath)
	if err != nil {
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("reopen binary COPY spool: %w", err))
	}
	normalized, err := os.CreateTemp("", "duckgres-worker-copy-normalized-*.copy")
	if err != nil {
		_ = source.Close()
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("create normalized binary COPY spool: %w", err))
	}
	normalizedPath := normalized.Name()
	cleanup := func() { _ = os.Remove(normalizedPath) }
	_, rewriteErr := pgbinary.RewriteProtocolCompleted(
		&copyFromStdinPreparationWriter{w: normalized},
		&copyFromStdinPreparationReader{r: &contextReader{ctx: ctx, r: source}},
		schema,
	)
	sourceCloseErr := source.Close()
	normalizedCloseErr := normalized.Close()
	if rewriteErr != nil {
		cleanup()
		return "", 0, func() {}, rewriteErr
	}
	if sourceCloseErr != nil {
		cleanup()
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("close binary COPY spool: %w", sourceCloseErr))
	}
	if normalizedCloseErr != nil {
		cleanup()
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("close normalized binary COPY spool: %w", normalizedCloseErr))
	}
	info, err := os.Stat(normalizedPath)
	if err != nil {
		cleanup()
		return "", 0, func() {}, copyFromStdinPreparationIOFailure(fmt.Errorf("stat normalized binary COPY spool: %w", err))
	}
	return normalizedPath, info.Size(), cleanup, nil
}

// doCopyFromStdin handles a COPY input spool DoPut from the control plane.
// It writes streamed bytes to a worker-local tempfile, executes the
// COPY SQL with the placeholder substituted, and returns the row count
// in the standard DoPutUpdateResult AppMetadata so the existing client
// drain path continues to work.
func (h *FlightSQLHandler) doCopyFromStdin(
	ctx context.Context,
	first *flight.FlightData,
	stream flight.FlightService_DoPutServer,
) (retErr error) {
	errorClass := copyFromStdinErrorInternal
	defer func() {
		retErr = sanitizeCopyFromStdinRPCError(retErr, errorClass)
	}()

	errorClass = copyFromStdinErrorInvalidRequest
	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}
	errorClass = copyFromStdinErrorUnavailable
	finishOperation, ok := session.beginOperation()
	if busyErr := sessionBusyStatus(ok); busyErr != nil {
		return busyErr
	}
	defer finishOperation()
	endConnWork, ok := session.beginConnWork()
	if !ok {
		return status.Error(codes.NotFound, "session closed")
	}
	finishDrain, err := h.pool.beginDrainWork(session.allowsDrainContinuation(""))
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		endConnWork()
		return drainErr
	}
	if err != nil {
		endConnWork()
		return status.Errorf(codes.Internal, "copy-from-stdin: start drain tracking: %v", err)
	}
	defer endConnWork()
	defer finishDrain()

	desc := first.GetFlightDescriptor()
	errorClass = copyFromStdinErrorInvalidRequest
	copySQL, binaryDatabaseTypeNames, err := copyFromStdinDescriptorRequest(desc)
	if err != nil {
		return err
	}
	if !strings.Contains(copySQL, flightclient.CopyFromStdinPathPlaceholder) {
		return status.Errorf(codes.InvalidArgument,
			"copy-from-stdin: COPY SQL missing %q placeholder", flightclient.CopyFromStdinPathPlaceholder)
	}

	errorClass = copyFromStdinErrorInternal
	tmpFile, err := os.CreateTemp("", "duckgres-worker-copy-*.copy")
	if err != nil {
		return status.Errorf(codes.Internal, "copy-from-stdin: create tempfile: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	var bytesWritten int64
	var frames int
	closed := false
	closeOnce := func() {
		if !closed {
			_ = tmpFile.Close()
			closed = true
		}
	}
	defer closeOnce()

	// Drain the byte stream from the CP. The CP closes send after the
	// last chunk; that surfaces here as io.EOF.
	for {
		// First frame is already in hand from customActionServer.DoPut;
		// any byte payload on it is part of the data stream.
		var data *flight.FlightData
		if first != nil {
			data = first
			first = nil
		} else {
			errorClass = copyFromStdinErrorUnavailable
			data, err = stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				switch status.Code(err) {
				case codes.Canceled:
					errorClass = copyFromStdinErrorCanceled
				case codes.DeadlineExceeded:
					errorClass = copyFromStdinErrorDeadlineExceeded
				}
				return status.Errorf(codes.Aborted, "copy-from-stdin: recv: %v", err)
			}
		}
		body := data.GetDataBody()
		if len(body) == 0 {
			continue
		}
		errorClass = copyFromStdinErrorInternal
		n, werr := tmpFile.Write(body)
		if werr != nil {
			return status.Errorf(codes.Internal, "copy-from-stdin: write tempfile: %v", werr)
		}
		bytesWritten += int64(n)
		frames++
	}
	closeOnce()

	loadPath := tmpPath
	loadBytes := bytesWritten
	cleanupPrepared := func() {}
	if len(binaryDatabaseTypeNames) > 0 {
		// Reaching preparation means Flight ended cleanly. The control plane only
		// closes its send side after the frontend sent pgwire CopyDone; CopyFail
		// and transport loss abort the Flight stream before this point.
		loadPath, loadBytes, cleanupPrepared, err = preparePostgresBinaryCopy(ctx, tmpPath, binaryDatabaseTypeNames)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return status.FromContextError(err).Err()
			}
			errorClass = copyFromStdinPreparationErrorClass(err)
			if errorClass == copyFromStdinErrorInternal {
				return status.Errorf(codes.Internal, "copy-from-stdin: prepare PostgreSQL binary COPY: %v", err)
			}
			return status.Errorf(codes.InvalidArgument, "copy-from-stdin: invalid PostgreSQL binary COPY: %v", err)
		}
	}
	defer cleanupPrepared()

	finalSQL := substituteCopyFromStdinPlaceholders(copySQL, loadPath, loadBytes)
	slog.Debug("copy-from-stdin: executing worker load statement",
		"frames", frames, "bytes", bytesWritten)

	errorClass = copyFromStdinErrorLoadRejected
	res, execErr := session.execConn(ctx, finalSQL)
	if execErr != nil {
		if closedErr := sessionClosedStatus(execErr); closedErr != nil {
			errorClass = copyFromStdinErrorUnavailable
			return closedErr
		}
		return status.Errorf(codes.InvalidArgument, "failed to execute update: %v", execErr)
	}
	var rowCount int64
	if res != nil {
		rowCount, _ = res.RowsAffected()
	}

	updateResult := &pb.DoPutUpdateResult{RecordCount: rowCount}
	errorClass = copyFromStdinErrorInternal
	app, mErr := proto.Marshal(updateResult)
	if mErr != nil {
		return status.Errorf(codes.Internal, "marshal DoPutUpdateResult: %v", mErr)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	errorClass = copyFromStdinErrorUnavailable
	if err := stream.Send(&flight.PutResult{AppMetadata: app}); err != nil {
		return fmt.Errorf("copy-from-stdin: send PutResult: %w", err)
	}
	return nil
}
