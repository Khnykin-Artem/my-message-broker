# Сборка
FROM golang:1.21 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o broker ./cmd/broker

# Финальный образ
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/broker .
COPY --from=builder /app/configs/config.yaml ./configs/
EXPOSE 5555 8080
CMD ["./broker", "--config", "configs/config.yaml"]