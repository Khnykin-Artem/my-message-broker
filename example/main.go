package main

import (
    "fmt"
    "log"
    "time"

    "my-message-broker/pkg/brokerclient"
)

func main() {
    // 1. Подключаемся к брокеру
    client, err := brokerclient.Dial("localhost:5555")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // if err := client.Auth("admin-token"); err != nil {
    //     log.Fatal(err)
    // }

    // 2. Создаём топик
    if err := client.CreateTopic("test_sdk"); err != nil {
        log.Printf("CreateTopic (maybe already exists): %v", err)
    }

    // 3. Подписываемся на топик (durable для проверки)
    sub, msgCh, err := client.SubscribeDurable("test_sdk", "myclient1")
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Close()

    // 4. Горутина для вывода полученных сообщений
    go func() {
        for msg := range msgCh {
            fmt.Printf("Получено сообщение: ID=%s, Payload=%s\n", msg.ID, msg.Payload)
        }
    }()

    // 5. Публикуем сообщение
    err = client.Publish("test_sdk", "Hello from SDK!")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Сообщение опубликовано")

    // // 6. Проверка административной команды LIST
    // list, _ := client.List()
    // fmt.Println("Список destination:\n", list)

    // Подождём, чтобы сообщение успело прийти
    time.Sleep(2 * time.Second)
}