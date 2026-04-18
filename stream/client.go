package stream

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

type Client[T any] struct {
	grpcClient grpc.ServerStreamingServer[T]
	msgChan    chan *T
}

func NewClient[T any](grpcClient grpc.ServerStreamingServer[T], bufferSize uint32) *Client[T] {
	return &Client[T]{
		grpcClient: grpcClient,
		msgChan:    make(chan *T, bufferSize),
	}
}

func (client *Client[T]) Send(message *T) error {
	select {
	case client.msgChan <- message:
		return nil
	default:
		return MessageChanFullError
	}
}

func (client *Client[T]) Process(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	defer func() {
		close(client.msgChan)
	}()

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
