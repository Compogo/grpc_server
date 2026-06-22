# Compogo gRPC Server

[![Go Reference](https://pkg.go.dev/badge/github.com/Compogo/grpc_server.svg)](https://pkg.go.dev/github.com/Compogo/grpc_server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

GRPC-сервер и утилиты для работы со стримами в фреймворке [Compogo](https://github.com/Compogo/compogo).

Предоставляет:

* GRPC-сервер с настройками keepalive
* Интеграцию с Runner для управления жизненным циклом
* Утилиты для работы с серверными, клиентскими и двунаправленными стримами

## Установка

```shell
go get github.com/Compogo/grpc_server
```

## Быстрый старт

```go
package main

import (
    "github.com/Compogo/compogo"
    "github.com/Compogo/grpc_server"
)

func main() {
    app := compogo.NewApp("myapp",
        compogo.WithComponents(&grpc_server.Component),
    )

    app.AddComponents(&compogo.Component{
        Name: "my_service",
        Init: compogo.StepFunc(func(container compogo.Container) error {
            return container.Invoke(func(server *grpc_server.Server) error {
                // Регистрация вашего GRPC-сервиса
                service.RegisterMyServiceServer(server.GetGRPC(), &MyService{})
                return nil
            })
        }),
    })

    if err := app.Serve(); err != nil {
        panic(err)
    }
}
```

## Конфигурация

### Флаги командной строки

```shell
# Сетевой интерфейс
--server.grpc.interface=0.0.0.0

# Порт
--server.grpc.port=9090

# Максимальное количество одновременных стримов
--server.grpc.max_concurrent_streams=1000

# Keepalive настройки
--server.grpc.min_time=1s
--server.grpc.keepalive.time=10s
--server.grpc.keepalive.timeout=20s
--server.grpc.permit_without_stream=true
```

## Использование

### Серверный стрим (Server Streaming)

Отправка сообщений клиенту:

```go
type MyService struct {
    service.UnimplementedMyServiceServer
}

func (s *MyService) Subscribe(req *emptypb.Empty, stream grpc.ServerStreamingServer[MyResponse]) error {
    client := stream.NewClientServerStreamingServer[MyResponse](stream, 10)

    // Отправка сообщений
    for i := 0; i < 10; i++ {
        if err := client.Send(&MyResponse{Data: "message"}); err != nil {
            return err
        }
    }

    return client.Process(ctx)
}
```

### Клиентский стрим (Client Streaming)

Получение сообщений от сервера:

```go
func (s *MyService) Subscribe(ctx context.Context, callback func(ctx context.Context, *MyResponse) error) error {
    stream, err := s.client.Subscribe(ctx, &emptypb.Empty{})
    if err != nil {
        return err
    }

    client := stream.NewClientServerStreamingClient[MyResponse](stream, callback, logger)

    return client.Process(ctx)
}
```

### Двунаправленный стрим (Bidirectional Streaming)

Одновременная отправка и получение сообщений:

```go
// На сервере
func (s *MyService) Chat(stream grpc.BidiStreamingServer[ChatRequest, ChatResponse]) error {
    server := stream.NewClientBidiStreamingServer[ChatRequest, ChatResponse](
        stream,
        func(ctx context.Context, req *ChatRequest) error {
            // Обработка входящих сообщений
            return nil
        },
        logger,
        10,
    )

    // Отправка сообщений
    server.Send(&ChatResponse{Message: "Hello"})

    return server.Process(ctx)
}

// На клиенте
func (c *Client) Chat(ctx context.Context, callback func(ctx context.Context, *ChatResponse) error) error {
    stream, err := c.client.Chat(ctx)
    if err != nil {
        return err
    }

    client := stream.NewClientBidiStreamingServer[ChatRequest, ChatResponse](
        stream,
        callback,
        logger,
        10,
    )

    // Отправка сообщений
    client.Send(&ChatRequest{Message: "Hello"})

    return client.Process(ctx)
}
```

## Зависимости

* [Compogo](https://github.com/Compogo/compogo) — основной фреймворк
* [Compogo Runner](https://github.com/Compogo/runner) — управление процессами
* [google.golang.org/grpc](https://github.com/grpc/grpc-go) — GRPC библиотека

## Лицензия

```text
MIT License

Copyright (c) 2026 Compogo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

```
