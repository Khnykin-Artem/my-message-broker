package main

import (
    "log"
    "os"
    "time"

    "my-message-broker/pkg/brokerclient"
)

func main() {
    addr := os.Getenv("BROKER_ADDR")
    if addr == "" {
        addr = "broker:5555"
    }
    consumerID := os.Getenv("CONSUMER_ID")
    if consumerID == "" {
        consumerID = "default"
    }

    client, err := brokerclient.Dial(addr)
    if err != nil {
        log.Fatalf("Failed to connect to broker: %v", err)
    }
    defer client.Close()

    // Подписка на очередь tasks (обычная)
    sub, msgCh, err := client.Subscribe("tasks")
    if err != nil {
        log.Fatalf("Subscribe error: %v", err)
    }
    defer sub.Close()

    log.Printf("Consumer %s started, waiting for messages...", consumerID)

    for msg := range msgCh {
        log.Printf("[%s] Received message ID=%s, payload=%s", consumerID, msg.ID, msg.Payload)

        // Имитируем ошибку для Task 3
        if msg.Payload == "Task 3" {
            log.Printf("[%s] Simulating failure, NOT acknowledging (message will go to DLQ after retries)", consumerID)
            continue // пропускаем ACK
        }

        time.Sleep(1 * time.Second) // имитация обработки
        if err := sub.Ack(msg.ID); err != nil {
            log.Printf("[%s] ACK error: %v", consumerID, err)
        } else {
            log.Printf("[%s] Acknowledged %s", consumerID, msg.ID)
        }
    }
}