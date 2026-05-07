package duckdbservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/flightclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// IsCopyFromStdinDescriptor reports whether desc is the custom DoPut
// descriptor used by the control plane to stream a CSV spool file for
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

// doCopyFromStdin handles a CSV spool DoPut from the control plane.
// It writes streamed bytes to a worker-local tempfile, executes the
// COPY SQL with the placeholder substituted, and returns the row count
// in the standard DoPutUpdateResult AppMetadata so the existing client
// drain path continues to work.
func (h *FlightSQLHandler) doCopyFromStdin(
	ctx context.Context,
	first *flight.FlightData,
	stream flight.FlightService_DoPutServer,
) error {
	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	desc := first.GetFlightDescriptor()
	if desc == nil || len(desc.Cmd) == 0 {
		return status.Error(codes.InvalidArgument, "copy-from-stdin: missing COPY SQL in descriptor.Cmd")
	}
	copySQL := string(desc.Cmd)
	if !strings.Contains(copySQL, flightclient.CopyFromStdinPathPlaceholder) {
		return status.Errorf(codes.InvalidArgument,
			"copy-from-stdin: COPY SQL missing %q placeholder", flightclient.CopyFromStdinPathPlaceholder)
	}

	tmpFile, err := os.CreateTemp("", "duckgres-worker-copy-*.csv")
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
			data, err = stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return status.Errorf(codes.Aborted, "copy-from-stdin: recv: %v", err)
			}
		}
		body := data.GetDataBody()
		if len(body) == 0 {
			continue
		}
		n, werr := tmpFile.Write(body)
		if werr != nil {
			return status.Errorf(codes.Internal, "copy-from-stdin: write tempfile: %v", werr)
		}
		bytesWritten += int64(n)
		frames++
	}
	closeOnce()

	finalSQL := strings.ReplaceAll(copySQL, flightclient.CopyFromStdinPathPlaceholder, tmpPath)
	slog.Debug("copy-from-stdin: executing COPY",
		"frames", frames, "bytes", bytesWritten, "tmp", tmpPath)

	res, execErr := session.Conn.ExecContext(ctx, finalSQL)
	if execErr != nil {
		return status.Errorf(codes.InvalidArgument, "failed to execute update: %v", execErr)
	}
	var rowCount int64
	if res != nil {
		rowCount, _ = res.RowsAffected()
	}

	updateResult := &pb.DoPutUpdateResult{RecordCount: rowCount}
	app, mErr := proto.Marshal(updateResult)
	if mErr != nil {
		return status.Errorf(codes.Internal, "marshal DoPutUpdateResult: %v", mErr)
	}
	if err := stream.Send(&flight.PutResult{AppMetadata: app}); err != nil {
		return fmt.Errorf("copy-from-stdin: send PutResult: %w", err)
	}
	return nil
}
