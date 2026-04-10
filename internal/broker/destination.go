package broker

import "time"

type DestinationType string

const (
    Queue DestinationType = "queue"
    Topic DestinationType = "topic"
)

type Destination struct {
    Name      string
    Type      DestinationType
    CreatedAt int64
}

func NewDestination(name string, typ DestinationType) *Destination {
    return &Destination{
        Name:      name,
        Type:      typ,
        CreatedAt: time.Now().Unix(),
    }
}