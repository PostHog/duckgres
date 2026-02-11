package duckdbservice

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// BearerTokenUnaryInterceptor returns a gRPC unary interceptor that validates
// the bearer token from the "authorization" metadata header.
// If expectedToken is empty, no authentication is enforced.
func BearerTokenUnaryInterceptor(expectedToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if expectedToken == "" {
			return handler(ctx, req)
		}
		if err := validateBearerToken(ctx, expectedToken); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// BearerTokenStreamInterceptor returns a gRPC stream interceptor that validates
// the bearer token from the "authorization" metadata header.
// If expectedToken is empty, no authentication is enforced.
func BearerTokenStreamInterceptor(expectedToken string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if expectedToken == "" {
			return handler(srv, ss)
		}
		if err := validateBearerToken(ss.Context(), expectedToken); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func validateBearerToken(ctx context.Context, expectedToken string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization header")
	}

	auth := authHeaders[0]
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return status.Error(codes.Unauthenticated, "expected Bearer authorization")
	}

	if subtle.ConstantTimeCompare([]byte(parts[1]), []byte(expectedToken)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	return nil
}
