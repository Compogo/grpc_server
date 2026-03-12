package grpc

import (
	"time"

	"github.com/Compogo/compogo/configurator"
)

const (
	MaxConcurrentStreamsFieldName = "server.grpc.max_concurrent_streams"
	MinTimeFieldName              = "server.grpc.min_time"
	PermitWithoutStreamFieldName  = "server.grpc.permit_without_stream"
	KeepaliveTimeFieldName        = "server.grpc.keepalive.time"
	KeepaliveTimeoutFieldName     = "server.grpc.keepalive.timeout"
	InterfaceFieldName            = "server.grpc.interface"
	PortFieldName                 = "server.grpc.port"

	MaxConcurrentStreamsDefault = uint32(1000)
	MinTimeDefault              = time.Second
	PermitWithoutStreamDefault  = true
	KeepaliveTimeDefault        = time.Second * 10
	KeepaliveTimeoutDefault     = time.Second * 20
	InterfaceDefault            = "0.0.0.0"
	PortDefault                 = uint16(9090)
)

type Config struct {
	PermitWithoutStream  bool
	Port                 uint16
	MaxConcurrentStreams uint32
	Interface            string
	MinTime              time.Duration
	KeepaliveTime        time.Duration
	KeepaliveTimeout     time.Duration
}

func NewConfig() *Config {
	return &Config{}
}

// Configuration applies configuration values to the Config struct.
// It reads from the provided configurator and sets defaults if values are not present.
// This function is designed to be used with container.Invoke in the Configuration phase.
func Configuration(config *Config, configurator configurator.Configurator) *Config {
	if config.Interface == "" || config.Interface == InterfaceDefault {
		configurator.SetDefault(InterfaceFieldName, InterfaceDefault)
		config.Interface = configurator.GetString(InterfaceFieldName)
	}

	if config.Port == 0 || config.Port == PortDefault {
		configurator.SetDefault(PortFieldName, PortDefault)
		config.Port = configurator.GetUint16(PortFieldName)
	}

	if config.MaxConcurrentStreams == 0 || config.MaxConcurrentStreams == MaxConcurrentStreamsDefault {
		configurator.SetDefault(MaxConcurrentStreamsFieldName, MaxConcurrentStreamsDefault)
		config.MaxConcurrentStreams = configurator.GetUint32(MaxConcurrentStreamsFieldName)
	}

	if !config.PermitWithoutStream || config.PermitWithoutStream == PermitWithoutStreamDefault {
		configurator.SetDefault(PermitWithoutStreamFieldName, PermitWithoutStreamDefault)
		config.PermitWithoutStream = configurator.GetBool(PermitWithoutStreamFieldName)
	}

	if config.MinTime == 0 || config.MinTime == MinTimeDefault {
		configurator.SetDefault(MinTimeFieldName, MinTimeDefault)
		config.MinTime = configurator.GetDuration(MinTimeFieldName)
	}

	if config.KeepaliveTime == 0 || config.KeepaliveTime == KeepaliveTimeDefault {
		configurator.SetDefault(KeepaliveTimeFieldName, KeepaliveTimeDefault)
		config.KeepaliveTime = configurator.GetDuration(KeepaliveTimeFieldName)
	}

	if config.KeepaliveTimeout == 0 || config.KeepaliveTimeout == KeepaliveTimeoutDefault {
		configurator.SetDefault(KeepaliveTimeoutFieldName, KeepaliveTimeoutDefault)
		config.KeepaliveTimeout = configurator.GetDuration(KeepaliveTimeoutFieldName)
	}

	return config
}
