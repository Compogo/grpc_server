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

type ClientServerStreaming[Res any] struct {
	grpcClient grpc.ServerStreamingServer[Res]
	msgChan    chan *Res
}

func NewClientServerStreaming[Res any](grpcClient grpc.ServerStreamingServer[Res], bufferSize uint32) *ClientServerStreaming[Res] {
	return &ClientServerStreaming[Res]{
		grpcClient: grpcClient,
		msgChan:    make(chan *Res, bufferSize),
	}
}

func (client *ClientServerStreaming[Res]) Send(message *Res) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

func (client *ClientServerStreaming[Res]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	defer close(client.msgChan)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-client.grpcClient.Context().Done():
			return nil
		case msg, ok := <-client.msgChan:
			if !ok {
				return MessageChanClosedError
			}

			if err := client.grpcClient.Send(msg); err != nil {
				return fmt.Errorf("message %+v; %w, %w", msg, SendMessageError, err)
			}
		}
	}
}

type ReqCallback[Req any] func(ctx context.Context, req *Req) error

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
			}
		}
	})

	wg.Wait()
	return nil
}
