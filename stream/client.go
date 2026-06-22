package stream

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Compogo/compogo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReqCallback — функция обратного вызова для обработки входящих сообщений.
type ReqCallback[Req any] func(ctx context.Context, req *Req) error

// ClientServerStreamingServer — серверный стрим для отправки сообщений клиенту.
// Использует буферизированный канал для асинхронной отправки.
type ClientServerStreamingServer[Res any] struct {
	stream  grpc.ServerStreamingServer[Res]
	msgChan chan *Res
}

// NewClientServerStreamingServer создаёт новый серверный стрим.
func NewClientServerStreamingServer[Res any](stream grpc.ServerStreamingServer[Res], bufferSize uint32) *ClientServerStreamingServer[Res] {
	return &ClientServerStreamingServer[Res]{
		stream:  stream,
		msgChan: make(chan *Res, bufferSize),
	}
}

// Send отправляет сообщение клиенту.
func (client *ClientServerStreamingServer[Res]) Send(message *Res) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

// Process обрабатывает отправку сообщений.
func (client *ClientServerStreamingServer[Res]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-client.stream.Context().Done():
			return nil
		case msg, ok := <-client.msgChan:
			if !ok {
				return MessageChanClosedError
			}

			if err := client.stream.Send(msg); err != nil {
				return fmt.Errorf("message %+v; %w, %w", msg, SendMessageError, err)
			}
		}
	}
}

// ClientServerStreamingClient — клиентский стрим для получения сообщений от сервера.
type ClientServerStreamingClient[Res any] struct {
	stream      grpc.ServerStreamingClient[Res]
	reqCallback ReqCallback[Res]

	logger compogo.Logger
}

// NewClientServerStreamingClient создаёт новый клиентский стрим.
func NewClientServerStreamingClient[Res any](
	stream grpc.ServerStreamingClient[Res],
	reqCallback ReqCallback[Res],
	logger compogo.Logger,
) *ClientServerStreamingClient[Res] {
	return &ClientServerStreamingClient[Res]{
		stream:      stream,
		reqCallback: reqCallback,
		logger:      logger,
	}
}

// Process обрабатывает получение сообщений.
func (client *ClientServerStreamingClient[Res]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	for {
		req, err := client.stream.Recv()
		if err != nil && errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if err = client.reqCallback(ctx, req); err != nil {
			client.logger.Error(err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-client.stream.Context().Done():
			return nil
		default:
			continue
		}
	}
}

// BidiStreaming — интерфейс для двунаправленного стрима.
type BidiStreaming[Req any, Res any] interface {
	Recv() (*Req, error)
	Send(*Res) error
	Context() context.Context
}

// ClientBidiStreamingServer — двунаправленный стрим.
type ClientBidiStreamingServer[Req any, Res any] struct {
	stream      BidiStreaming[Req, Res]
	msgChan     chan *Res
	reqCallback ReqCallback[Req]

	logger compogo.Logger
}

// NewClientBidiStreamingServer создаёт новый двунаправленный стрим.
func NewClientBidiStreamingServer[Req any, Res any](
	stream BidiStreaming[Req, Res],
	reqCallback ReqCallback[Req],
	logger compogo.Logger,
	bufferSize uint32,
) *ClientBidiStreamingServer[Req, Res] {
	return &ClientBidiStreamingServer[Req, Res]{
		stream:      stream,
		reqCallback: reqCallback,
		logger:      logger,
		msgChan:     make(chan *Res, bufferSize),
	}
}

// Send отправляет сообщение.
func (client *ClientBidiStreamingServer[Req, Res]) Send(message *Res) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

// Process обрабатывает двунаправленный стрим.
func (client *ClientBidiStreamingServer[Req, Res]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	chanErr := make(chan error, 1)
	go func() {
		defer cancelFunc()
		recvCtx, recvCancelFunc := context.WithCancel(ctx)
		defer recvCancelFunc()
		defer close(chanErr)

		for {
			req, err := client.stream.Recv()
			if err = CheckError(err); err != nil {
				chanErr <- err
				return
			}

			if err = client.reqCallback(recvCtx, req); err != nil {
				chanErr <- err
				return
			}

			select {
			case <-recvCtx.Done():
				return
			default:
				continue
			}
		}
	}()

	for {
		select {
		case err, ok := <-chanErr:
			if !ok {
				return nil
			}

			return err
		case m := <-client.msgChan:
			if err := CheckError(client.stream.Send(m)); err != nil {
				return err
			}
		case <-client.stream.Context().Done():
			return CheckError(client.stream.Context().Err())
		case <-ctx.Done():
			return nil
		}
	}
}

// CheckError проверяет, является ли ошибка результатом отмены контекста.
func CheckError(err error) error {
	st, ok := status.FromError(err)
	if (ok && st.Code() == codes.Canceled) || errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
