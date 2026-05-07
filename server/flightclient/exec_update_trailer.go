package flightclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// executeUpdateWithTrailer is a hand-rolled equivalent of
// flightsql.Client.ExecuteUpdate that exposes the gRPC trailer.
//
// ExecuteUpdate is a bidirectional-streaming RPC (DoPut). Passing
// grpc.Trailer(&md) as a CallOption is documented to work for unary RPCs;
// for streams the trailer is only reachable via stream.Trailer() *after*
// stream.Recv has returned a non-nil error (typically io.EOF). The
// standard ExecuteUpdate hides the stream so that's not possible from
// outside, which means the worker's per-query profiling JSON (sent as a
// trailer) was silently dropped on the control-plane side for every
// non-result-returning query.
//
// This re-implements the same wire interaction (descriptor + CommandStatementUpdate),
// then drains the stream to io.EOF and pulls the trailer.
func executeUpdateWithTrailer(ctx context.Context, c *flightsql.Client, query string) (int64, metadata.MD, error) {
	cmd := &pb.CommandStatementUpdate{Query: query}

	var anyMsg anypb.Any
	if err := anyMsg.MarshalFrom(cmd); err != nil {
		return 0, nil, fmt.Errorf("marshal CommandStatementUpdate: %w", err)
	}
	descBytes, err := proto.Marshal(&anyMsg)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal Any: %w", err)
	}
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  descBytes,
	}

	stream, err := c.Client.DoPut(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("flight doput: %w", err)
	}

	if err := stream.Send(&flight.FlightData{FlightDescriptor: desc}); err != nil {
		return 0, stream.Trailer(), fmt.Errorf("flight doput send: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return 0, stream.Trailer(), fmt.Errorf("flight doput close-send: %w", err)
	}

	res, err := stream.Recv()
	if err != nil {
		return 0, stream.Trailer(), err
	}

	// Drain to io.EOF so the trailer is materialised. The worker sends a
	// single PutResult and closes the stream, so this Recv should return
	// io.EOF immediately.
	if drainErr := drainToEOF(stream); drainErr != nil {
		// Surface the error, but the trailer may still be valid.
		return 0, stream.Trailer(), drainErr
	}

	trailer := stream.Trailer()

	var updateResult pb.DoPutUpdateResult
	if err := proto.Unmarshal(res.GetAppMetadata(), &updateResult); err != nil {
		return 0, trailer, fmt.Errorf("unmarshal DoPutUpdateResult: %w", err)
	}
	return updateResult.GetRecordCount(), trailer, nil
}

// drainToEOF reads from the stream until io.EOF or another non-nil error.
// Per the gRPC contract, the trailer is only reachable after Recv returns
// a non-nil error — io.EOF is the expected terminator for a clean
// DoPut/ExecuteUpdate exchange.
func drainToEOF(stream pb.FlightService_DoPutClient) error {
	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}
