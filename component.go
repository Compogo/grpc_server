package grpc_server

import (
	"github.com/Compogo/compogo"
	"github.com/Compogo/compogo/flag"
	"github.com/Compogo/runner"
)

// Component — компонент GRPC-сервера для Compogo.
// Регистрирует сервер в DI-контейнере и запускает его через Runner.
//
// Пример:
//
//	app.AddComponents(&grpc_server.Component)
//
//	var s *grpc_server.Server
//	container.Invoke(func(server *grpc_server.Server) { s = server })
//	service.RegisterMyServiceServer(s.GetGRPC(), &MyService{})
var Component = compogo.Component{
	Dependencies: compogo.Components{
		&runner.Component,
	},
	Init: compogo.StepFunc(func(container compogo.Container) error {
		return container.Provides(
			NewConfig,
			NewServer,
		)
	}),
	BindFlags: compogo.BindFlags(func(flagSet flag.FlagSet, container compogo.Container) error {
		return container.Invoke(func(config *Config) {
			flagSet.BoolVar(&config.PermitWithoutStream, PermitWithoutStreamFieldName, PermitWithoutStreamDefault, "")
			flagSet.Uint16Var(&config.Port, PortFieldName, PortDefault, "")
			flagSet.Uint32Var(&config.MaxConcurrentStreams, MaxConcurrentStreamsFieldName, MaxConcurrentStreamsDefault, "")
			flagSet.StringVar(&config.Interface, InterfaceFieldName, InterfaceDefault, "")
			flagSet.DurationVar(&config.MinTime, MinTimeFieldName, MinTimeDefault, "")
			flagSet.DurationVar(&config.KeepaliveTime, KeepaliveTimeFieldName, KeepaliveTimeDefault, "")
			flagSet.DurationVar(&config.KeepaliveTimeout, KeepaliveTimeoutFieldName, KeepaliveTimeoutDefault, "")
		})
	}),
	Configuration: compogo.StepFunc(func(container compogo.Container) error {
		return container.Invoke(Configuration)
	}),
	Execute: compogo.StepFunc(func(container compogo.Container) error {
		return container.Invoke(func(r runner.Runner, server *Server) error {
			return r.RunProcess(server)
		})
	}),
	Stop: compogo.StepFunc(func(container compogo.Container) error {
		return container.Invoke(func(server *Server) error {
			return server.Close()
		})
	}),
}
