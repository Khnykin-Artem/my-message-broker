package main

import (
    "log"
    "os"

    "my-message-broker/pkg/brokerclient"
)

func main() {
    addr := os.Getenv("BROKER_ADDR")
    if addr == "" {
        addr = "broker:5555"
    }

    client, err := brokerclient.Dial(addr)
    if err != nil {
        log.Fatalf("Failed to connect to broker: %v", err)
    }
    defer client.Close()

    // Подписываемся на DLQ (очередь tasks_dlq)
    // Брокер создаст её автоматически при первом перемещении сообщения
    sub, msgCh, err := client.Subscribe("tasks_dlq")
    if err != nil {
        log.Fatalf("Failed to subscribe to DLQ: %v", err)
    }
    defer sub.Close()

    log.Println("Monitor started. Watching Dead Letter Queue (tasks_dlq)...")

    for msg := range msgCh {
        log.Printf("[DLQ MONITOR] Dead message received: ID=%s, payload=%s", msg.ID, msg.Payload)
        // Здесь можно реализовать дополнительные действия: алерт, запись в БД и т.п.
        // Чтобы не засорять DLQ, подтверждаем сообщение (удаляем из DLQ)
        if err := sub.Ack(msg.ID); err != nil {
            log.Printf("[DLQ MONITOR] ACK error: %v", err)
        } else {
            log.Printf("[DLQ MONITOR] Acknowledged and removed from DLQ: %s", msg.ID)
        }
    }
}