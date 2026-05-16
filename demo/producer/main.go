package main

import (
    "fmt"
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

    client, err := brokerclient.Dial(addr)
    if err != nil {
        log.Fatalf("Failed to connect to broker: %v", err)
    }
    defer client.Close()

    // Создаём очередь (если не существует)
    _ = client.CreateQueue("tasks")

    log.Println("Producer started. Sending tasks every 5 seconds...")
    var i int = 1
    for {
        payload := fmt.Sprintf("Task %d", i)
        if err := client.Publish("tasks", payload); err != nil {
            log.Printf("Failed to publish: %v", err)
        } else {
            log.Printf("Published: %s", payload)
        }
        i++
        time.Sleep(5 * time.Second)
    }
}