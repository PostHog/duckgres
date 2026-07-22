package flightclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CopyFromStdinDescriptorPath is the FlightDescriptor path that marks a
// DoPut stream as a spool-and-COPY upload rather than a standard
// Flight SQL CommandStatementUpdate. The duckdbservice worker matches
// on this path and routes to its CopyFromStdin handler.
const CopyFromStdinDescriptorPath = "duckgres-copy-from-stdin"

// CopyFromStdinPostgresBinaryPathVersion marks descriptor.Cmd as a structured,
// versioned PostgreSQL binary COPY request. An older worker fails closed when
// it receives the JSON request instead of executing scanner SQL without the
// accompanying validation metadata.
const CopyFromStdinPostgresBinaryPathVersion = "postgres-binary-v1"

// CopyFromStdinPathPlaceholder is the substring inside the COPY SQL that
// the worker replaces with the worker-local spool file path before
// executing the COPY. The control plane builds the COPY SQL with
// BuildDuckDBCopyFromSQL using this token as the file path.
const CopyFromStdinPathPlaceholder = "__DUCKGRES_COPY_PATH__"

// CopyFromStdinSizePlaceholder is replaced by the worker with the exact
// number of bytes in its completed spool file. The postgres binary table
// function needs this because its COPY-function wrapper currently ignores
// buffer_size and cannot read files larger than its 32 MiB default.
const CopyFromStdinSizePlaceholder = "__DUCKGRES_COPY_SIZE__"

// CopyFromStdinChunkSize is the byte size of each FlightData frame sent
// from the control plane to the worker during a COPY upload. Large
// enough to amortise per-frame overhead, small enough to keep memory
// bounded if the worker is slow to drain.
const CopyFromStdinChunkSize = 1 << 20 // 1 MiB

const copyFromStdinErrorDomain = "duckgres.copy"

// copyFromStdinRPCError is the privacy boundary between a worker and the
// control plane. It deliberately retains only a gRPC code and fixed text, not
// the original status description or cause, because legacy workers may echo
// generated SQL, copied values, or worker-local paths in that description.
type copyFromStdinRPCError struct {
	code             codes.Code
	message          string
	canceled         bool
	deadlineExceeded bool
}

func (e *copyFromStdinRPCError) Error() string { return e.message }

func (e *copyFromStdinRPCError) GRPCStatus() *status.Status {
	return status.New(e.code, e.message)
}

func (e *copyFromStdinRPCError) Is(target error) bool {
	return (e.canceled && target == context.Canceled) ||
		(e.deadlineExceeded && target == context.DeadlineExceeded)
}

func sanitizeCopyFromStdinRPCError(err error) error {
	if err == nil {
		return nil
	}

	code := status.Code(err)
	reason := ""
	switch {
	case errors.Is(err, context.Canceled):
		reason = "CANCELED"
		if code == codes.Unknown {
			code = codes.Canceled
		}
	case errors.Is(err, context.DeadlineExceeded):
		reason = "DEADLINE_EXCEEDED"
		if code == codes.Unknown {
			code = codes.DeadlineExceeded
		}
	}
	if rpcStatus, ok := status.FromError(err); ok {
		for _, detail := range rpcStatus.Details() {
			info, ok := detail.(*errdetails.ErrorInfo)
			if !ok || info.GetDomain() != copyFromStdinErrorDomain {
				continue
			}
			if copyFromStdinReasonMatchesCode(info.GetReason(), code) {
				reason = info.GetReason()
			}
			break
		}
	}

	return &copyFromStdinRPCError{
		code:             code,
		message:          copyFromStdinSafeMessage(code, reason),
		canceled:         code == codes.Canceled || reason == "CANCELED",
		deadlineExceeded: code == codes.DeadlineExceeded || reason == "DEADLINE_EXCEEDED",
	}
}

func copyFromStdinReasonMatchesCode(reason string, code codes.Code) bool {
	switch reason {
	case "INVALID_REQUEST", "INVALID_INPUT", "LOAD_REJECTED":
		return code == codes.InvalidArgument
	case "CANCELED", "DEADLINE_EXCEEDED":
		return code == codes.Aborted ||
			(reason == "CANCELED" && code == codes.Canceled) ||
			(reason == "DEADLINE_EXCEEDED" && code == codes.DeadlineExceeded)
	case "UNAVAILABLE":
		return code == codes.Aborted || code == codes.FailedPrecondition || code == codes.NotFound ||
			code == codes.ResourceExhausted || code == codes.Unavailable
	case "INTERNAL":
		return code == codes.Internal || code == codes.Unknown
	default:
		return false
	}
}

func copyFromStdinSafeMessage(code codes.Code, reason string) string {
	switch reason {
	case "INVALID_REQUEST":
		return "COPY request is invalid"
	case "INVALID_INPUT":
		return "COPY input is invalid"
	case "LOAD_REJECTED":
		return "COPY input was rejected by the destination"
	case "CANCELED":
		return "COPY load canceled"
	case "DEADLINE_EXCEEDED":
		return "COPY load deadline exceeded"
	case "UNAVAILABLE":
		return "COPY worker is unavailable"
	case "INTERNAL":
		return "COPY worker failed"
	}

	switch code {
	case codes.Canceled:
		return "COPY load canceled"
	case codes.DeadlineExceeded:
		return "COPY load deadline exceeded"
	case codes.InvalidArgument:
		return "COPY input is invalid"
	case codes.NotFound, codes.Unavailable:
		return "COPY worker is unavailable"
	default:
		return "COPY worker failed"
	}
}

// CopyFromStdin streams COPY input bytes from r to a remote worker, then runs
// request.SQLTemplate against a worker-local spool file. The SQL template must
// contain CopyFromStdinPathPlaceholder where the file path should appear. It
// returns the number of rows the worker reports COPY-affected.
//
// Wire layout:
//
//	frame 0: FlightDescriptor{Type=PATH, Path=[CopyFromStdinDescriptorPath, optional version], Cmd=SQL or structured request}
//	frame 1..N: DataBody=<chunk of COPY input bytes>
//	(client closes send)
//	server: PutResult{AppMetadata=DoPutUpdateResult{RecordCount=N}}
func (e *FlightExecutor) CopyFromStdin(ctx context.Context, request sqlcore.CopyFromStdinRequest, r io.Reader) (int64, error) {
	if e.dead.Load() {
		return 0, ErrWorkerDead
	}

	reqCtx, cancel := e.mergedContext(ctx)
	defer cancel()
	reqCtx = e.withSession(reqCtx)

	stream, err := e.client.Client.DoPut(reqCtx)
	if err != nil {
		return 0, sanitizeCopyFromStdinRPCError(err)
	}

	// Frame 0: descriptor + COPY SQL.
	path := []string{CopyFromStdinDescriptorPath}
	cmd := []byte(request.SQLTemplate)
	if len(request.PostgresBinaryDatabaseTypeNames) > 0 {
		path = append(path, CopyFromStdinPostgresBinaryPathVersion)
		cmd, err = json.Marshal(request)
		if err != nil {
			return 0, fmt.Errorf("marshal binary COPY request: %w", err)
		}
	}
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: path,
		Cmd:  cmd,
	}
	if err := stream.Send(&flight.FlightData{FlightDescriptor: desc}); err != nil {
		return 0, sanitizeCopyFromStdinRPCError(err)
	}

	// Frames 1..N: COPY input byte chunks. We re-use one buffer; gRPC marshals the
	// proto synchronously inside Send, so reusing is safe across iterations.
	//
	// On a non-EOF read error we return immediately WITHOUT calling
	// CloseSend. The deferred cancel() then aborts the gRPC stream, so the
	// worker's Recv fails with Canceled and the worker bails before
	// running COPY on a partial spool file. This is what makes wire-level
	// COPY cancellation correct end-to-end.
	buf := make([]byte, CopyFromStdinChunkSize)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&flight.FlightData{DataBody: buf[:n]}); sendErr != nil {
				return 0, sanitizeCopyFromStdinRPCError(sendErr)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read COPY stream: %w", readErr)
		}
	}

	if err := stream.CloseSend(); err != nil {
		return 0, sanitizeCopyFromStdinRPCError(err)
	}

	res, err := stream.Recv()
	if err != nil {
		return 0, sanitizeCopyFromStdinRPCError(err)
	}

	// Drain to EOF so any trailing metadata is materialised.
	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		return 0, sanitizeCopyFromStdinRPCError(err)
	}

	var updateResult pb.DoPutUpdateResult
	if err := proto.Unmarshal(res.GetAppMetadata(), &updateResult); err != nil {
		return 0, fmt.Errorf("unmarshal DoPutUpdateResult: %w", err)
	}
	return updateResult.GetRecordCount(), nil
}
