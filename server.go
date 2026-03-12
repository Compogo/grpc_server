package grpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server wraps a gRPC server with Compogo lifecycle support.
// It implements runner.Process and io.Closer for seamless integration
// with the application's lifecycle.
type Server struct {
	grpcServer *grpc.Server
	config     *Config
}

// NewServer creates a new gRPC server with the given configuration.
// The server is pre-configured with production-ready defaults:
//   - Max concurrent streams limit
//   - Keepalive enforcement policy
//   - Keepalive parameters
//
// All settings can be overridden via command-line flags.
func NewServer(config *Config) *Server {
	return &Server{
		grpcServer: grpc.NewServer(
			grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             config.MinTime,
				PermitWithoutStream: config.PermitWithoutStream,
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    config.KeepaliveTime,
				Timeout: config.KeepaliveTimeout,
			}),
		),
		config: config,
	}
}

// GetGRPC returns the underlying *grpc.Server.
// This is used to register service implementations before starting the server.
//
// Example:
//
//	pb.RegisterUserServiceServer(server.GetGRPC(), &userServiceImpl{})
func (server *Server) GetGRPC() *grpc.Server {
	return server.grpcServer
}

// Process implements runner.Process.
// It starts the gRPC server and blocks until the server stops or an error occurs.
// The server is automatically stopped when the provided context is canceled.
//
// The method:
//   - Creates a TCP listener on the configured interface and port
//   - Starts the gRPC server
//   - Closes the listener when the context is done
//   - Returns nil on normal shutdown, error on failure
func (server *Server) Process(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", server.config.Interface, server.config.Port))
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	return server.grpcServer.Serve(listener)
}

// Close implements io.Closer for graceful shutdown.
// It calls GracefulStop on the underlying gRPC server, allowing
// in-flight requests to complete before shutting down.
func (server *Server) Close() error {
	server.grpcServer.GracefulStop()
	return nil
}
