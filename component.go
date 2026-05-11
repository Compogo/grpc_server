package grpc

import (
	"github.com/Compogo/compogo/component"
	"github.com/Compogo/compogo/container"
	"github.com/Compogo/compogo/flag"
	"github.com/Compogo/runner"
)

// Component is a ready-to-use Compogo component that provides a gRPC server.
// It automatically:
//   - Registers Config and Server in the DI container
//   - Adds command-line flags for server configuration
//   - Configures the server during Configuration phase
//   - Starts the server as a runner task during Execute phase
//   - Performs graceful shutdown during Stop phase
//
// Usage:
//
//	compogo.WithComponents(
//	    runner.Component,
//	    grpc.Component,
//	    // ... your service components
//	)
//
// Service registration should happen in PreExecute phase:
//
//	var userServiceComponent = &component.Component{
//	    Dependencies: []*component.Component{grpc.Component},
//	    PreExecute: component.StepFunc(func(c container.Container) error {
//	        return c.Invoke(func(server *grpc.Server, svc *UserService) {
//	            pb.RegisterUserServiceServer(server.GetGRPC(), svc)
//	        })
//	    }),
//	}
var Component = &component.Component{
	Dependencies: component.Components{
		runner.Component,
	},
	Init: component.StepFunc(func(container container.Container) error {
		return container.Provides(
			NewConfig,
			NewServer,
		)
	}),
	BindFlags: component.BindFlags(func(flagSet flag.FlagSet, container container.Container) error {
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
	Configuration: component.StepFunc(func(container container.Container) error {
		return container.Invoke(Configuration)
	}),
	Execute: component.StepFunc(func(container container.Container) error {
		return container.Invoke(func(r runner.Runner, server *Server) error {
			return r.RunProcess(server)
		})
	}),
	Stop: component.StepFunc(func(container container.Container) error {
		return container.Invoke(func(server *Server) error {
			return server.Close()
		})
	}),
}
