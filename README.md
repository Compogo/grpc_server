# Compogo gRPC 🔌

**Compogo gRPC** — это production-ready gRPC-сервер для Compogo, построенный поверх стандартной библиотеки `google.golang.org/grpc`. Полностью интегрируется с жизненным циклом Compogo, поддерживает тонкую настройку через флаги и graceful shutdown из коробки.

## 🚀 Установка

```bash
go get github.com/Compogo/grpc
```

### 📦 Быстрый старт

```go
package main

import (
    "github.com/Compogo/compogo"
    "github.com/Compogo/runner"
    "github.com/Compogo/grpc"
)

func main() {
    app := compogo.NewApp("myapp",
        compogo.WithOsSignalCloser(),
        runner.WithRunner(),
        grpc.Component,  // добавляем gRPC-сервер
        compogo.WithComponents(
            userServiceComponent,
        ),
    )

    if err := app.Serve(); err != nil {
        panic(err)
    }
}

// Компонент с бизнес-логикой
var userServiceComponent = &component.Component{
    Dependencies: component.Components{grpc.Component},
    Init: component.StepFunc(func(c container.Container) error {
        return c.Provide(NewUserService)
    }),
    PreExecute: component.StepFunc(func(c container.Container) error {
        return c.Invoke(func(server *grpc.Server, svc *UserService) {
            // Регистрируем сервис ДО запуска сервера
            pb.RegisterUserServiceServer(server.GetGRPC(), svc)
        })
    }),
}
```

### ✨ Возможности

#### 🎯 Production-ready конфигурация

Сервер настраивается через флаги — всё, что нужно для продакшена:

```bash
./myapp \
    --server.grpc.interface=0.0.0.0 \
    --server.grpc.port=9090 \
    --server.grpc.max_concurrent_streams=1000 \
    --server.grpc.min_time=1s \
    --server.grpc.permit_without_stream=true \
    --server.grpc.keepalive.time=10s \
    --server.grpc.keepalive.timeout=20s
```

#### 🔄 Жизненный цикл

```plantuml
Init         → создаём конфиг и сервер
BindFlags    → добавляем флаги
Configuration→ применяем конфиг
PreExecute   → РЕГИСТРИРУЕМ СЕРВИСЫ (важно!)
Execute      → запускаем сервер через Runner
Stop         → graceful shutdown (GracefulStop)
```

**Критический** момент: сервисы должны регистрироваться в `PreExecute` — ДО того, как сервер запустится в `Execute`.

### 🔌 Доступ к чистому grpc.Server

```go
server.GetGRPC()  // для регистрации сервисов
```

#### 🧩 Пример с несколькими сервисами

```go
var GrpcServicesComponent = &component.Component{
    Dependencies: component.Components{grpc.Component},
    Init: component.StepFunc(func(c container.Container) error {
        return c.Provides(
            NewUserService,
            NewOrderService,
            NewProductService,
        )
    }),
    PreExecute: component.StepFunc(func(c container.Container) error {
        return c.Invoke(func(
            server *grpc.Server,
            users *UserService,
            orders *OrderService,
            products *ProductService,
        ) {
            pb.RegisterUserServiceServer(server.GetGRPC(), users)
            pb.RegisterOrderServiceServer(server.GetGRPC(), orders)
            pb.RegisterProductServiceServer(server.GetGRPC(), products)
        })
    }),
}
```
