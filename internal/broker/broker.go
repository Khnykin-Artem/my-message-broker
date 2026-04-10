package broker

import (
    "fmt"
    "log"
    "net"
    "strings"
    "time"

    "github.com/google/uuid"
    "my-message-broker/internal/protocol"
)

type Broker struct {
    storage              *Storage
    visibilityTimeoutSec int
    maxDeliveryAttempts  int
}

func NewBroker(visibilityTimeoutSec, maxDeliveryAttempts int) *Broker {
    b := &Broker{
        storage:              NewStorage(),
        visibilityTimeoutSec: visibilityTimeoutSec,
        maxDeliveryAttempts:  maxDeliveryAttempts,
    }
    // запускаем фоновую проверку таймаутов
    go b.retryTimeoutChecker()
    return b
}

// ProcessCommand обрабатывает команду от клиента
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
    default:
        return protocol.RespErr + " unknown command"
    }
}

// CREATE QUEUE|TOPIC name
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
    return protocol.RespOk
}

// DELETE name
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
    // проверяем, есть ли подписки
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
    return protocol.RespOk
}

// PUBLISH name payload
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
    if dest.Type == Topic {
        // топик: рассылаем всем подписчикам
        b.storage.mu.RLock()
        subs := b.storage.Subscriptions[name]
        b.storage.mu.RUnlock()
        if len(subs) == 0 {
            return protocol.RespOk // нет подписчиков – просто успех
        }
        for _, sub := range subs {
            go b.sendToSubscriber(sub, msg)
        }
        return protocol.RespOk
    } else { // очередь
        b.storage.mu.Lock()
        b.storage.Queues[name] = append(b.storage.Queues[name], msg)
        b.storage.mu.Unlock()
        b.tryDeliverQueue(name)
        return protocol.RespOk
    }
}

// SUBSCRIBE name
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
    // создаём подписку
    sub := NewSubscription(uuid.New().String(), name, conn)
    b.storage.Subscriptions[name] = append(b.storage.Subscriptions[name], sub)
    // если очередь – пытаемся сразу доставить ожидающие сообщения
    if dest.Type == Queue {
        go b.tryDeliverQueue(name)
    }
    return protocol.RespOk
}

// UNSUBSCRIBE name
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

// ACK messageID
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
    // удаляем из очереди
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
    return protocol.RespOk
}

// sendToSubscriber отправляет сообщение подписчику и возвращает ошибку
func (b *Broker) sendToSubscriber(sub *Subscription, msg *Message) error {
    line := fmt.Sprintf("MSG %s %s %s\n", msg.ID, msg.Destination, msg.Payload)
    _, err := sub.Conn.Write([]byte(line))
    if err != nil {
        // ошибка отправки – удаляем подписку
        b.storage.mu.Lock()
        defer b.storage.mu.Unlock()
        subs := b.storage.Subscriptions[msg.Destination]
        newSubs := []*Subscription{}
        for _, s := range subs {
            if s.Conn != sub.Conn {
                newSubs = append(newSubs, s)
            }
        }
        b.storage.Subscriptions[msg.Destination] = newSubs
        log.Printf("removed subscription due to send error: %v", err)
        return err
    }
    // для очереди – помещаем в pending и запускаем таймер (таймер обрабатывается в retryTimeoutChecker)
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

// tryDeliverQueue пытается доставить следующее сообщение из очереди
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
    // round-robin
    idx := b.storage.RoundRobinIdx[name] % len(subs)
    sub := subs[idx]
    msg := queue[0]

    if msg.DeliveryAttempt >= b.maxDeliveryAttempts {
        // превышено количество попыток – удаляем (dead-letter)
        b.storage.Queues[name] = queue[1:]
        log.Printf("message %s exceeded max delivery attempts, dropped", msg.ID)
        b.tryDeliverQueue(name) // рекурсивно пробуем следующее
        return
    }

    // отправляем асинхронно
    go func() {
        err := b.sendToSubscriber(sub, msg)
        if err == nil {
            // увеличиваем счётчик попыток
            msg.DeliveryAttempt++
        } else {
            // при ошибке отправки пробуем другого подписчика
            b.storage.mu.Lock()
            b.storage.RoundRobinIdx[name] = idx + 1
            b.storage.mu.Unlock()
            b.tryDeliverQueue(name)
        }
    }()
    b.storage.RoundRobinIdx[name] = idx + 1
}

// retryTimeoutChecker фоновый процесс для повторной отправки неподтверждённых сообщений
func (b *Broker) retryTimeoutChecker() {
    ticker := time.NewTicker(time.Duration(b.visibilityTimeoutSec) * time.Second)
    for range ticker.C {
        b.storage.mu.Lock()
        now := time.Now().Unix()
        for msgID, pending := range b.storage.PendingAcks {
            if now-pending.SentAt >= int64(b.visibilityTimeoutSec) {
                // таймаут – возвращаем сообщение в начало очереди
                queueName := pending.Message.Destination
                // удаляем из pending
                delete(b.storage.PendingAcks, msgID)
                // добавляем обратно в очередь (в начало)
                queue := b.storage.Queues[queueName]
                newQueue := append([]*Message{pending.Message}, queue...)
                b.storage.Queues[queueName] = newQueue
                log.Printf("message %s timed out, redelivering", msgID)
                // пробуем доставить заново
                go b.tryDeliverQueue(queueName)
            }
        }
        b.storage.mu.Unlock()
    }
}

// RemoveSubscriptionsByConn удаляет все подписки, связанные с соединением
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