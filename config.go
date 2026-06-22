package grpc_server

import (
	"time"

	"github.com/Compogo/compogo"
)

const (
	// MaxConcurrentStreamsFieldName максимальное количество одновременных стримов
	MaxConcurrentStreamsFieldName = "server.grpc.max_concurrent_streams"

	// MinTimeFieldName минимальное время между keepalive пингами
	MinTimeFieldName = "server.grpc.min_time"

	// PermitWithoutStreamFieldName разрешать keepalive без активных стримов
	PermitWithoutStreamFieldName = "server.grpc.permit_without_stream"

	// KeepaliveTimeFieldName интервал keepalive пингов
	KeepaliveTimeFieldName = "server.grpc.keepalive.time"

	// KeepaliveTimeoutFieldName таймаут keepalive
	KeepaliveTimeoutFieldName = "server.grpc.keepalive.timeout"

	// InterfaceFieldName сетевой интерфейс
	InterfaceFieldName = "server.grpc.interface"

	// PortFieldName порт
	PortFieldName = "server.grpc.port"
)

var (
	MaxConcurrentStreamsDefault = uint32(1000)
	MinTimeDefault              = time.Second
	PermitWithoutStreamDefault  = true
	KeepaliveTimeDefault        = time.Second * 10
	KeepaliveTimeoutDefault     = time.Second * 20
	InterfaceDefault            = "0.0.0.0"
	PortDefault                 = uint16(9090)
)

// Config содержит конфигурацию GRPC-сервера.
type Config struct {
	PermitWithoutStream  bool
	Port                 uint16
	MaxConcurrentStreams uint32
	Interface            string
	MinTime              time.Duration
	KeepaliveTime        time.Duration
	KeepaliveTimeout     time.Duration
}

// NewConfig создаёт новую конфигурацию.
func NewConfig() *Config {
	return &Config{}
}

// Configuration загружает конфигурацию из Configurator.
func Configuration(config *Config, configurator compogo.Configurator) *Config {
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
