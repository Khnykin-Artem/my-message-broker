package broker

import (
    "time"
    "github.com/google/uuid"
)

type MessageStatus string

const (
    StatusPending   MessageStatus = "pending"
    StatusDelivered MessageStatus = "delivered"
    StatusAcked     MessageStatus = "acked"
)

type Message struct {
    ID              string
    Destination     string
    Payload         string
    Timestamp       int64
    DeliveryAttempt int
    Status          MessageStatus
}

func NewMessage(destination, payload string) *Message {
    return &Message{
        ID:              uuid.New().String(),
        Destination:     destination,
        Payload:         payload,
        Timestamp:       time.Now().Unix(),
        DeliveryAttempt: 0,
        Status:          StatusPending,
    }
}