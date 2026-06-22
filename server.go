package grpc_server

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server представляет GRPC-сервер с поддержкой graceful shutdown.
type Server struct {
	grpcServer *grpc.Server
	config     *Config
}

// NewServer создаёт новый GRPC-сервер с настройками из конфигурации.
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

// GetGRPC возвращает внутренний GRPC-сервер для регистрации сервисов.
func (server *Server) GetGRPC() *grpc.Server {
	return server.grpcServer
}

// Process запускает GRPC-сервер.
// Реализует интерфейс runner.Process.
func (server *Server) Process(_ context.Context) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", server.config.Interface, server.config.Port))
	if err != nil {
		return err
	}

	return server.grpcServer.Serve(listener)
}

func (server *Server) Close() error {
	server.grpcServer.GracefulStop()
	return nil
}

func (server *Server) Name() string {
	return "server.grpc"
}
