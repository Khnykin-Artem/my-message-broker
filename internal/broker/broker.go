package broker

import (
    "fmt"
    "log"
    "net"
    "strings"
    "sync"
    "time"

    "github.com/google/uuid"
    "my-message-broker/internal/protocol"
    "my-message-broker/internal/storage"
)

// ---------- Структуры ----------
type Broker struct {
    storage              *Storage
    visibilityTimeoutSec int
    maxDeliveryAttempts  int
    wal                  *storage.Wal

    // durable subscriptions: topic -> consumerID -> DurableSubInfo
    durableSubs   map[string]map[string]*DurableSubInfo
    muDurable     sync.RWMutex
    // хранение сообщений топиков (ограниченной глубины)
    topicMessages map[string][]*Message
    topicMsgMu    sync.RWMutex
    maxTopicMsgs  int
}

type DurableSubInfo struct {
    ConsumerID    string
    Topic         string
    LastMsgOffset int // индекс последнего полученного сообщения в slice topicMessages[topic]
    // для восстановления offset из WAL
}

// ---------- Конструктор ----------
func NewBroker(visibilityTimeoutSec, maxDeliveryAttempts int, walPath string, useWal bool) (*Broker, error) {
    b := &Broker{
        storage:              NewStorage(),
        visibilityTimeoutSec: visibilityTimeoutSec,
        maxDeliveryAttempts:  maxDeliveryAttempts,
        durableSubs:          make(map[string]map[string]*DurableSubInfo),
        topicMessages:        make(map[string][]*Message),
        maxTopicMsgs:         1000, // хранить последние 1000 сообщений в топике
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

// -------------------- Восстановление из WAL --------------------
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
                    DeliveryAttempt: 0,
                    Status:          StatusPending,
                }
                b.storage.mu.Lock()
                if dest, exists := b.storage.Destinations[msg.Destination]; exists && dest.Type == Queue {
                    b.storage.Queues[msg.Destination] = append(b.storage.Queues[msg.Destination], msg)
                } else if dest != nil && dest.Type == Topic {
                    b.topicMsgMu.Lock()
                    b.topicMessages[msg.Destination] = append(b.topicMessages[msg.Destination], msg)
                    if len(b.topicMessages[msg.Destination]) > b.maxTopicMsgs {
                        b.topicMessages[msg.Destination] = b.topicMessages[msg.Destination][1:]
                    }
                    b.topicMsgMu.Unlock()
                }
                b.storage.mu.Unlock()
            }
        case storage.EntryDurableSub:
            if data, ok := entry.Data.(map[string]interface{}); ok {
                topic := data["topic"].(string)
                consumerID := data["consumerID"].(string)
                offset := int(data["offset"].(float64))
                b.muDurable.Lock()
                if _, ok := b.durableSubs[topic]; !ok {
                    b.durableSubs[topic] = make(map[string]*DurableSubInfo)
                }
                b.durableSubs[topic][consumerID] = &DurableSubInfo{
                    ConsumerID:    consumerID,
                    Topic:         topic,
                    LastMsgOffset: offset,
                }
                b.muDurable.Unlock()
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
                // очистить durable подписки на этот топик
                b.muDurable.Lock()
                delete(b.durableSubs, name)
                b.muDurable.Unlock()
                b.topicMsgMu.Lock()
                delete(b.topicMessages, name)
                b.topicMsgMu.Unlock()
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

// -------------------- Публичные методы для HTTP API --------------------
func (b *Broker) GetQueuesStats() map[string]int {
    b.storage.mu.RLock()
    defer b.storage.mu.RUnlock()
    stats := make(map[string]int)
    for name, dest := range b.storage.Destinations {
        if dest.Type == Queue {
            stats[name] = len(b.storage.Queues[name])
        }
    }
    return stats
}

func (b *Broker) DeleteQueue(name string) error {
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
    dest, exists := b.storage.Destinations[name]
    if !exists {
        return fmt.Errorf("queue not found")
    }
    if dest.Type != Queue {
        return fmt.Errorf("destination is not a queue")
    }
    delete(b.storage.Destinations, name)
    delete(b.storage.Queues, name)
    delete(b.storage.Subscriptions, name)
    delete(b.storage.RoundRobinIdx, name)
    for id, pending := range b.storage.PendingAcks {
        if pending.Message.Destination == name {
            delete(b.storage.PendingAcks, id)
        }
    }
    if b.wal != nil {
        b.logToWal(storage.WalEntry{
            Type: storage.EntryDelete,
            Data: map[string]interface{}{"name": name},
        })
    }
    return nil
}

// -------------------- Durable subscription logic --------------------
func (b *Broker) durableSubscribe(topic, consumerID string, conn net.Conn) string {
    b.muDurable.Lock()
    defer b.muDurable.Unlock()
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()

    dest, exists := b.storage.Destinations[topic]
    if !exists || dest.Type != Topic {
        return protocol.RespErr + " topic not found"
    }

    // Восстанавливаем или создаём info
    if _, ok := b.durableSubs[topic]; !ok {
        b.durableSubs[topic] = make(map[string]*DurableSubInfo)
    }
    info, ok := b.durableSubs[topic][consumerID]
    if !ok {
        info = &DurableSubInfo{
            ConsumerID:    consumerID,
            Topic:         topic,
            LastMsgOffset: -1,
        }
        b.durableSubs[topic][consumerID] = info
    }

    // Записываем в WAL факт durable подписки
    b.logToWal(storage.WalEntry{
        Type: storage.EntryDurableSub,
        Data: map[string]interface{}{
            "topic":      topic,
            "consumerID": consumerID,
            "offset":     info.LastMsgOffset,
        },
    })

    // Отправляем все сообщения, которые были накоплены с момента последнего offset
    b.topicMsgMu.RLock()
    msgs := b.topicMessages[topic]
    b.topicMsgMu.RUnlock()
    startIdx := info.LastMsgOffset + 1
    if startIdx < 0 {
        startIdx = 0
    }
    for i := startIdx; i < len(msgs); i++ {
        go b.sendToSubscriberTopic(conn, msgs[i])
    }
    if len(msgs) > 0 {
        info.LastMsgOffset = len(msgs) - 1
    } else {
        info.LastMsgOffset = -1
    }

    // Также добавляем обычную подписку для мгновенной доставки новых сообщений
    sub := NewSubscription(uuid.New().String(), topic, conn)
    b.storage.Subscriptions[topic] = append(b.storage.Subscriptions[topic], sub)
    return protocol.RespOk
}

// отправка сообщения durable-подписчику (пишем прямо в сокет)
func (b *Broker) sendToSubscriberTopic(conn net.Conn, msg *Message) {
    line := fmt.Sprintf("MSG %s %s %s\n", msg.ID, msg.Destination, msg.Payload)
    _, err := conn.Write([]byte(line))
    if err != nil {
        log.Printf("failed to send durable message to %s: %v", conn.RemoteAddr(), err)
    }
}

// -------------------- Обработка команд --------------------
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

// ---------- CREATE / DELETE ----------
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
    } else {
        b.topicMsgMu.Lock()
        if _, ok := b.topicMessages[name]; !ok {
            b.topicMessages[name] = []*Message{}
        }
        b.topicMsgMu.Unlock()
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
    } else {
        b.topicMsgMu.Lock()
        delete(b.topicMessages, name)
        b.topicMsgMu.Unlock()
        b.muDurable.Lock()
        delete(b.durableSubs, name)
        b.muDurable.Unlock()
    }
    delete(b.storage.Destinations, name)
    delete(b.storage.Subscriptions, name)
    delete(b.storage.RoundRobinIdx, name)
    b.logToWal(storage.WalEntry{
        Type: storage.EntryDelete,
        Data: map[string]interface{}{"name": name},
    })
    return protocol.RespOk
}

// ---------- PUBLISH ----------
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
            "id":          msg.ID,
            "destination": msg.Destination,
            "payload":     msg.Payload,
            "timestamp":   msg.Timestamp,
        },
    })
    if dest.Type == Topic {
        // сохраняем сообщение топика
        b.topicMsgMu.Lock()
        b.topicMessages[name] = append(b.topicMessages[name], msg)
        if len(b.topicMessages[name]) > b.maxTopicMsgs {
            b.topicMessages[name] = b.topicMessages[name][1:]
        }
        b.topicMsgMu.Unlock()
        // рассылка активным подписчикам
        b.storage.mu.RLock()
        subs := b.storage.Subscriptions[name]
        b.storage.mu.RUnlock()
        for _, sub := range subs {
            go b.sendToSubscriber(sub, msg)
        }
        return protocol.RespOk
    } else {
        // очередь
        b.storage.mu.Lock()
        b.storage.Queues[name] = append(b.storage.Queues[name], msg)
        b.storage.mu.Unlock()
        b.tryDeliverQueue(name)
        return protocol.RespOk
    }
}

// ---------- SUBSCRIBE / UNSUBSCRIBE ----------
func (b *Broker) handleSubscribe(cmd *protocol.Command, conn net.Conn) string {
    if len(cmd.Args) < 1 {
        return protocol.RespErr + " usage: SUBSCRIBE destination [consumer_id]"
    }
    name := cmd.Args[0]
    consumerID := ""
    if len(cmd.Args) >= 2 {
        consumerID = cmd.Args[1]
    }
    b.storage.mu.Lock()
    dest, exists := b.storage.Destinations[name]
    b.storage.mu.Unlock()
    if !exists {
        return protocol.RespErr + " destination not found"
    }
    // durable подписка на топик
    if dest.Type == Topic && consumerID != "" {
        return b.durableSubscribe(name, consumerID, conn)
    }
    // обычная подписка (временная)
    b.storage.mu.Lock()
    defer b.storage.mu.Unlock()
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
    // Проверяем, может быть это durable подписка? по conn удаляем только обычные
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

// ---------- ACK ----------
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

// ---------- Доставка для очередей (не durable) ----------
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

// ---------- LIST / STATS / PURGE ----------
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
        stats += fmt.Sprintf("Active subscribers: %d\n", subs)
        // дополнительно durable subscribers
        b.muDurable.RLock()
        durableCount := 0
        if m, ok := b.durableSubs[name]; ok {
            durableCount = len(m)
        }
        b.muDurable.RUnlock()
        stats += fmt.Sprintf("Durable subscribers: %d\n", durableCount)
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