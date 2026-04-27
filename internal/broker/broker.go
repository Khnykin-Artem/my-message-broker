package broker

import (
    "fmt"
    "log"
    "net"
    "strings"
    "time"

    "github.com/google/uuid"
    "my-message-broker/internal/protocol"
    "my-message-broker/internal/storage"
)

type Broker struct {
    storage              *Storage
    visibilityTimeoutSec int
    maxDeliveryAttempts  int
    wal                  *storage.Wal
}

func NewBroker(visibilityTimeoutSec, maxDeliveryAttempts int, walPath string, useWal bool) (*Broker, error) {
    b := &Broker{
        storage:              NewStorage(),
        visibilityTimeoutSec: visibilityTimeoutSec,
        maxDeliveryAttempts:  maxDeliveryAttempts,
    }
    if useWal && walPath != "" {
        wal, err := storage.NewWal(walPath)
        if err != nil {
            return nil, err
        }
        b.wal = wal
        if err := b.recoverFromWal(); err != nil {
            log.Printf("recovery error: %v", err)
        }
    }
    go b.retryTimeoutChecker()
    return b, nil
}

func (b *Broker) recoverFromWal() error {
    return b.wal.Replay(func(entry storage.WalEntry) {
        switch entry.Type {
        case storage.EntryCreate:
            if data, ok := entry.Data.(map[string]interface{}); ok {
                name, _ := data["name"].(string)
                typ, _ := data["type"].(string)
                var destType DestinationType
                if strings.ToUpper(typ) == "QUEUE" {
                    destType = Queue
                } else {
                    destType = Topic
                }
                b.storage.mu.Lock()
                if _, exists := b.storage.Destinations[name]; !exists {
                    b.storage.Destinations[name] = NewDestination(name, destType)
                    if destType == Queue {
                        b.storage.Queues[name] = []*Message{}
                    }
                }
                b.storage.mu.Unlock()
            }
        case storage.EntryPublish:
            if data, ok := entry.Data.(map[string]interface{}); ok {
                msg := &Message{
                    ID:              data["id"].(string),
                    Destination:     data["destination"].(string),
                    Payload:         data["payload"].(string),
                    Timestamp:       int64(data["timestamp"].(float64)),
                    DeliveryAttempt: int(data["deliveryAttempt"].(float64)),
                    Status:          MessageStatus(data["status"].(string)),
                }
                b.storage.mu.Lock()
                if dest, exists := b.storage.Destinations[msg.Destination]; exists && dest.Type == Queue {
                    b.storage.Queues[msg.Destination] = append(b.storage.Queues[msg.Destination], msg)
                }
                b.storage.mu.Unlock()
            }
        case storage.EntryAck:
            if data, ok := entry.Data.(map[string]interface{}); ok {
                msgID := data["messageId"].(string)
                b.storage.mu.Lock()
                if pending, ok := b.storage.PendingAcks[msgID]; ok {
                    queueName := pending.Message.Destination
                    queue := b.storage.Queues[queueName]
                    newQueue := []*Message{}
                    for _, m := range queue {
                        if m.ID != msgID {
                            newQueue = append(newQueue, m)
                        }
                    }
                    b.storage.Queues[queueName] = newQueue
                    delete(b.storage.PendingAcks, msgID)
                }
                b.storage.mu.Unlock()
            }
        case storage.EntryDelete:
            if data, ok := entry.Data.(map[string]interface{}); ok {
                name := data["name"].(string)
                b.storage.mu.Lock()
                delete(b.storage.Destinations, name)
                delete(b.storage.Queues, name)
                delete(b.storage.Subscriptions, name)
                delete(b.storage.RoundRobinIdx, name)
                b.storage.mu.Unlock()
            }
        }
    })
}

func (b *Broker) logToWal(entry storage.WalEntry) {
    if b.wal != nil {
        if err := b.wal.Append(entry); err != nil {
            log.Printf("WAL append error: %v", err)
        }
    }
}

func (b *Broker) ProcessCommand(cmd *protocol.Command, conn net.Conn) string {
    if cmd == nil {
        return protocol.RespErr + " empty command"
    }
    switch cmd.Name {
    case protocol.CmdCreate:
        return b.handleCreate(cmd)
    case protocol.CmdDelete:
        return b.handleDelete(cmd)
    case protocol.CmdPublish:
        return b.handlePublish(cmd)
    case protocol.CmdSubscribe:
        return b.handleSubscribe(cmd, conn)
    case protocol.CmdUnsubscribe:
        return b.handleUnsubscribe(cmd, conn)
    case protocol.CmdAck:
        return b.handleAck(cmd)
    case protocol.CmdPing:
        return protocol.RespOk
    case "LIST":
        return b.handleList()
    case "STATS":
        if len(cmd.Args) < 1 {
            return protocol.RespErr + " usage: STATS <name>"
        }
        return b.handleStats(cmd.Args[0])
    case "PURGE":
        if len(cmd.Args) < 1 {
            return protocol.RespErr + " usage: PURGE <queue>"
        }
        return b.handlePurge(cmd.Args[0])
    default:
        return protocol.RespErr + " unknown command"
    }
}

func (b *Broker) handleCreate(cmd *protocol.Command) string {
    if len(cmd.Args) < 2 {
        return protocol.RespErr + " usage: CREATE QUEUE|TOPIC name"
    }
    typStr := strings.ToUpper(cmd.Args[0])
    name := cmd.Args[1]
    var destType DestinationType
    switch typStr {
    case protocol.TypeQueue:
        destType = Queue
    case protocol.TypeTopic:
        destType = Topic
    default:
        return protocol.RespErr + " invalid type, use QUEUE or TOPIC"
    }
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    if _, exists := b.storage.Destinations[name]; exists {
        return protocol.RespErr + " destination already exists"
    }
    b.storage.Destinations[name] = NewDestination(name, destType)
    if destType == Queue {
        if _, ok := b.storage.Queues[name]; !ok {
            b.storage.Queues[name] = []*Message{}
        }
    }
    b.logToWal(storage.WalEntry{
        Type: storage.EntryCreate,
        Data: map[string]interface{}{
            "name": name,
            "type": typStr,
        },
    })
    return protocol.RespOk
}

func (b *Broker) handleDelete(cmd *protocol.Command) string {
    if len(cmd.Args) < 1 {
        return protocol.RespErr + " usage: DELETE name"
    }
    name := cmd.Args[0]
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    dest, exists := b.storage.Destinations[name]
    if !exists {
        return protocol.RespErr + " destination not found"
    }
    if subs, ok := b.storage.Subscriptions[name]; ok && len(subs) > 0 {
        return protocol.RespErr + " destination has active subscriptions"
    }
    if dest.Type == Queue {
        if msgs, ok := b.storage.Queues[name]; ok && len(msgs) > 0 {
            return protocol.RespErr + " queue has pending messages"
        }
        delete(b.storage.Queues, name)
    }
    delete(b.storage.Destinations, name)
    delete(b.storage.Subscriptions, name)
    delete(b.storage.RoundRobinIdx, name)
    b.logToWal(storage.WalEntry{
        Type: storage.EntryDelete,
        Data: map[string]interface{}{
            "name": name,
        },
    })
    return protocol.RespOk
}

func (b *Broker) handlePublish(cmd *protocol.Command) string {
    if len(cmd.Args) < 2 {
        return protocol.RespErr + " usage: PUBLISH name payload"
    }
    name := cmd.Args[0]
    payload := cmd.Args[1]
    b.storage.mu.RLock()
    dest, exists := b.storage.Destinations[name]
    b.storage.mu.RUnlock()
    if !exists {
        return protocol.RespErr + " destination not found"
    }
    msg := NewMessage(name, payload)
    b.logToWal(storage.WalEntry{
        Type: storage.EntryPublish,
        Data: map[string]interface{}{
            "id":              msg.ID,
            "destination":     msg.Destination,
            "payload":         msg.Payload,
            "timestamp":       msg.Timestamp,
            "deliveryAttempt": msg.DeliveryAttempt,
            "status":          string(msg.Status),
        },
    })
    if dest.Type == Topic {
        b.storage.mu.RLock()
        subs := b.storage.Subscriptions[name]
        b.storage.mu.RUnlock()
        if len(subs) == 0 {
            return protocol.RespOk
        }
        for _, sub := range subs {
            go b.sendToSubscriber(sub, msg)
        }
        return protocol.RespOk
    } else {
        b.storage.mu.Lock()
        b.storage.Queues[name] = append(b.storage.Queues[name], msg)
        b.storage.mu.Unlock()
        b.tryDeliverQueue(name)
        return protocol.RespOk
    }
}

func (b *Broker) handleSubscribe(cmd *protocol.Command, conn net.Conn) string {
    if len(cmd.Args) < 1 {
        return protocol.RespErr + " usage: SUBSCRIBE name"
    }
    name := cmd.Args[0]
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    dest, exists := b.storage.Destinations[name]
    if !exists {
        return protocol.RespErr + " destination not found"
    }
    sub := NewSubscription(uuid.New().String(), name, conn)
    b.storage.Subscriptions[name] = append(b.storage.Subscriptions[name], sub)
    if dest.Type == Queue {
        go b.tryDeliverQueue(name)
    }
    return protocol.RespOk
}

func (b *Broker) handleUnsubscribe(cmd *protocol.Command, conn net.Conn) string {
    if len(cmd.Args) < 1 {
        return protocol.RespErr + " usage: UNSUBSCRIBE name"
    }
    name := cmd.Args[0]
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    subs := b.storage.Subscriptions[name]
    newSubs := []*Subscription{}
    for _, sub := range subs {
        if sub.Conn != conn {
            newSubs = append(newSubs, sub)
        }
    }
    if len(newSubs) == len(subs) {
        return protocol.RespErr + " not subscribed"
    }
    b.storage.Subscriptions[name] = newSubs
    return protocol.RespOk
}

func (b *Broker) handleAck(cmd *protocol.Command) string {
    if len(cmd.Args) < 1 {
        return protocol.RespErr + " usage: ACK messageID"
    }
    msgID := cmd.Args[0]
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    pending, ok := b.storage.PendingAcks[msgID]
    if !ok {
        return protocol.RespErr + " unknown or already acked message"
    }
    queueName := pending.Message.Destination
    queue := b.storage.Queues[queueName]
    newQueue := []*Message{}
    for _, m := range queue {
        if m.ID != msgID {
            newQueue = append(newQueue, m)
        }
    }
    b.storage.Queues[queueName] = newQueue
    delete(b.storage.PendingAcks, msgID)
    b.logToWal(storage.WalEntry{
        Type: storage.EntryAck,
        Data: map[string]interface{}{
            "messageId": msgID,
        },
    })
    return protocol.RespOk
}

func (b *Broker) sendToSubscriber(sub *Subscription, msg *Message) error {
    line := fmt.Sprintf("MSG %s %s %s\n", msg.ID, msg.Destination, msg.Payload)
    _, err := sub.Conn.Write([]byte(line))
    if err != nil {
        b.storage.mu.Lock()
        subs := b.storage.Subscriptions[msg.Destination]
        newSubs := []*Subscription{}
        for _, s := range subs {
            if s.Conn != sub.Conn {
                newSubs = append(newSubs, s)
            }
        }
        b.storage.Subscriptions[msg.Destination] = newSubs
        b.storage.mu.Unlock()
        log.Printf("removed subscription due to send error: %v", err)
        return err
    }
    if dest, _ := b.storage.Destinations[msg.Destination]; dest != nil && dest.Type == Queue {
        b.storage.mu.Lock()
        b.storage.PendingAcks[msg.ID] = &PendingMessage{
            Message:  msg,
            Sub:      sub,
            SentAt:   time.Now().Unix(),
            Attempts: msg.DeliveryAttempt + 1,
        }
        b.storage.mu.Unlock()
    }
    return nil
}

func (b *Broker) tryDeliverQueue(name string) {
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    dest, exists := b.storage.Destinations[name]
    if !exists || dest.Type != Queue {
        return
    }
    subs := b.storage.Subscriptions[name]
    if len(subs) == 0 {
        return
    }
    queue := b.storage.Queues[name]
    if len(queue) == 0 {
        return
    }
    idx := b.storage.RoundRobinIdx[name] % len(subs)
    sub := subs[idx]
    msg := queue[0]

    if msg.DeliveryAttempt >= b.maxDeliveryAttempts {
        b.storage.Queues[name] = queue[1:]
        log.Printf("message %s exceeded max delivery attempts, dropped", msg.ID)
        b.tryDeliverQueue(name)
        return
    }

    go func() {
        err := b.sendToSubscriber(sub, msg)
        if err == nil {
            msg.DeliveryAttempt++
        } else {
            b.storage.mu.Lock()
            b.storage.RoundRobinIdx[name] = idx + 1
            b.storage.mu.Unlock()
            b.tryDeliverQueue(name)
        }
    }()
    b.storage.RoundRobinIdx[name] = idx + 1
}

func (b *Broker) retryTimeoutChecker() {
    ticker := time.NewTicker(time.Duration(b.visibilityTimeoutSec) * time.Second)
    for range ticker.C {
        b.storage.mu.Lock()
        now := time.Now().Unix()
        for msgID, pending := range b.storage.PendingAcks {
            if now-pending.SentAt >= int64(b.visibilityTimeoutSec) {
                queueName := pending.Message.Destination
                delete(b.storage.PendingAcks, msgID)
                queue := b.storage.Queues[queueName]
                newQueue := append([]*Message{pending.Message}, queue...)
                b.storage.Queues[queueName] = newQueue
                log.Printf("message %s timed out, redelivering", msgID)
                go b.tryDeliverQueue(queueName)
            }
        }
        b.storage.mu.Unlock()
    }
}

func (b *Broker) RemoveSubscriptionsByConn(conn net.Conn) {
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    for dest, subs := range b.storage.Subscriptions {
        newSubs := []*Subscription{}
        for _, sub := range subs {
            if sub.Conn != conn {
                newSubs = append(newSubs, sub)
            }
        }
        if len(newSubs) != len(subs) {
            b.storage.Subscriptions[dest] = newSubs
        }
    }
}

func (b *Broker) handleList() string {
    b.storage.mu.RLock()
    defer b.storage.mu.RUnlock()
    var sb strings.Builder
    sb.WriteString("Destinations:\n")
    for name, dest := range b.storage.Destinations {
        sb.WriteString(fmt.Sprintf("  %s (%s)\n", name, dest.Type))
    }
    return sb.String()
}

func (b *Broker) handleStats(name string) string {
    b.storage.mu.RLock()
    defer b.storage.mu.RUnlock()
    dest, ok := b.storage.Destinations[name]
    if !ok {
        return protocol.RespErr + " destination not found"
    }
    stats := fmt.Sprintf("Name: %s\nType: %s\n", name, dest.Type)
    if dest.Type == Queue {
        depth := len(b.storage.Queues[name])
        stats += fmt.Sprintf("Queue depth: %d\nPending acks: %d\n",
            depth, len(b.storage.PendingAcks))
    } else {
        subs := len(b.storage.Subscriptions[name])
        stats += fmt.Sprintf("Subscribers: %d\n", subs)
    }
    return stats
}

func (b *Broker) handlePurge(queueName string) string {
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    dest, ok := b.storage.Destinations[queueName]
    if !ok || dest.Type != Queue {
        return protocol.RespErr + " not a queue"
    }
    b.storage.Queues[queueName] = []*Message{}
    for id, pm := range b.storage.PendingAcks {
        if pm.Message.Destination == queueName {
            delete(b.storage.PendingAcks, id)
        }
    }
    return protocol.RespOk
}