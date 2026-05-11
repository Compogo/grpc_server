package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Compogo/compogo/logger"
	"google.golang.org/grpc"
)

type ReqCallback[Req any] func(ctx context.Context, req *Req) error

type ClientServerStreamingServer[Res any] struct {
	stream  grpc.ServerStreamingServer[Res]
	msgChan chan *Res
}

func NewClientServerStreamingServer[Res any](stream grpc.ServerStreamingServer[Res], bufferSize uint32) *ClientServerStreamingServer[Res] {
	return &ClientServerStreamingServer[Res]{
		stream:  stream,
		msgChan: make(chan *Res, bufferSize),
	}
}

func (client *ClientServerStreamingServer[Res]) Send(message *Res) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

func (client *ClientServerStreamingServer[Res]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	defer close(client.msgChan)

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

type ClientServerStreamingClient[Res any] struct {
	stream      grpc.ServerStreamingClient[Res]
	reqCallback ReqCallback[Res]

	logger logger.Logger
}

func NewClientServerStreamingClient[Res any](
	stream grpc.ServerStreamingClient[Res],
	reqCallback ReqCallback[Res],
	logger logger.Logger,
) *ClientServerStreamingClient[Res] {
	return &ClientServerStreamingClient[Res]{
		stream:      stream,
		reqCallback: reqCallback,
		logger:      logger,
	}
}

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

type ClientBidiStreamingServer[Req any, Res any] struct {
	stream      grpc.BidiStreamingServer[Req, Res]
	msgChan     chan *Res
	reqCallback ReqCallback[Req]

	logger logger.Logger
}

func NewClientBidiStreamingServer[Req any, Res any](
	stream grpc.BidiStreamingServer[Req, Res],
	reqCallback ReqCallback[Req],
	logger logger.Logger,
	bufferSize uint32,
) *ClientBidiStreamingServer[Req, Res] {
	return &ClientBidiStreamingServer[Req, Res]{
		stream:      stream,
		reqCallback: reqCallback,
		logger:      logger,
		msgChan:     make(chan *Res, bufferSize),
	}
}

func (client *ClientBidiStreamingServer[Req, Res]) Send(message *Res) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

func (client *ClientBidiStreamingServer[Req, Res]) Process(ctx context.Context) error {
	mainCtx, mainCancelFunc := context.WithCancel(ctx)
	defer mainCancelFunc()

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		recvCtx, recvCancelFunc := context.WithCancel(mainCtx)
		defer recvCancelFunc()

		for {
			req, err := client.stream.Recv()
			if err != nil && errors.Is(err, io.EOF) {
				mainCancelFunc()
				return
			}

			if err != nil {
				client.logger.Error(err)
				mainCancelFunc()
				return
			}

			if err = client.reqCallback(recvCtx, req); err != nil {
				client.logger.Error(err)
			}

			select {
			case <-recvCtx.Done():
				return
			case <-client.stream.Context().Done():
				mainCancelFunc()
				return
			default:
				continue
			}
		}
	})

	wg.Go(func() {
		sendCtx, sendCancelFunc := context.WithCancel(mainCtx)
		defer sendCancelFunc()

		for {
			select {
			case m := <-client.msgChan:
				err := client.stream.Send(m)
				if err != nil && errors.Is(err, io.EOF) {
					mainCancelFunc()
					return
				}

				if err != nil {
					client.logger.Error(err)
					mainCancelFunc()
					return
				}
			case <-sendCtx.Done():
				return
			case <-client.stream.Context().Done():
				mainCancelFunc()
				return
			}
		}
	})

	wg.Wait()
	return nil
}
