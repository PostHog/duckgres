package flightclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/protobuf/proto"
)

// CopyFromStdinDescriptorPath is the FlightDescriptor path that marks a
// DoPut stream as a spool-and-COPY upload rather than a standard
// Flight SQL CommandStatementUpdate. The duckdbservice worker matches
// on this path and routes to its CopyFromStdin handler.
const CopyFromStdinDescriptorPath = "duckgres-copy-from-stdin"

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

// CopyFromStdin streams COPY input bytes from r to a remote worker, then runs
// copySQL on the worker against a worker-local spool file. copySQL must
// contain CopyFromStdinPathPlaceholder where the file path should appear.
// Returns the number of rows the worker reports COPY-affected.
//
// Wire layout:
//
//	frame 0: FlightDescriptor{Type=PATH, Path=[CopyFromStdinDescriptorPath], Cmd=copySQL}
//	frame 1..N: DataBody=<chunk of COPY input bytes>
//	(client closes send)
//	server: PutResult{AppMetadata=DoPutUpdateResult{RecordCount=N}}
func (e *FlightExecutor) CopyFromStdin(ctx context.Context, copySQL string, r io.Reader) (int64, error) {
	if e.dead.Load() {
		return 0, ErrWorkerDead
	}

	reqCtx, cancel := e.mergedContext(ctx)
	defer cancel()
	reqCtx = e.withSession(reqCtx)

	stream, err := e.client.Client.DoPut(reqCtx)
	if err != nil {
		return 0, fmt.Errorf("flight doput: %w", err)
	}

	// Frame 0: descriptor + COPY SQL.
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{CopyFromStdinDescriptorPath},
		Cmd:  []byte(copySQL),
	}
	if err := stream.Send(&flight.FlightData{FlightDescriptor: desc}); err != nil {
		return 0, fmt.Errorf("flight doput send descriptor: %w", err)
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
				return 0, fmt.Errorf("flight doput send chunk: %w", sendErr)
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
		return 0, fmt.Errorf("flight doput close-send: %w", err)
	}

	res, err := stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("flight doput recv: %w", err)
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
		return 0, err
	}

	var updateResult pb.DoPutUpdateResult
	if err := proto.Unmarshal(res.GetAppMetadata(), &updateResult); err != nil {
		return 0, fmt.Errorf("unmarshal DoPutUpdateResult: %w", err)
	}
	return updateResult.GetRecordCount(), nil
}
