package broker

import (
    "sync"
)

type Storage struct {
    mu            sync.RWMutex
    Destinations  map[string]*Destination
    Queues        map[string][]*Message          // имя очереди -> список сообщений
    PendingAcks   map[string]*PendingMessage     // messageID -> pending info
    Subscriptions map[string][]*Subscription     // destination -> list
    RoundRobinIdx map[string]int                 // для очередей: индекс последнего отправителя
}

type PendingMessage struct {
    Message   *Message
    Sub       *Subscription
    SentAt    int64
    Attempts  int
}

func NewStorage() *Storage {
    return &Storage{
        Destinations:  make(map[string]*Destination),
        Queues:        make(map[string][]*Message),
        PendingAcks:   make(map[string]*PendingMessage),
        Subscriptions: make(map[string][]*Subscription),
        RoundRobinIdx: make(map[string]int),
    }
}