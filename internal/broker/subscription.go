package broker

import (
    "net"
    "time"
)

type Subscription struct {
    ID          string
    Destination string
    Conn        net.Conn
    CreatedAt   int64
}

func NewSubscription(id, dest string, conn net.Conn) *Subscription {
    return &Subscription{
        ID:          id,
        Destination: dest,
        Conn:        conn,
        CreatedAt:   time.Now().Unix(),
    }
}