package brokerclient

import (
    "bufio"
    "fmt"
    "net"
    "strings"
    "sync"
)

// Subscriber представляет активную подписку.
type Subscriber struct {
    client     *Client
    destName   string
    consumerID string
    conn       net.Conn
    reader     *bufio.Reader
    msgCh      chan Message
    closeCh    chan struct{}
    closeOnce  sync.Once
}

// Subscribe создаёт новую подписку (обычную) на destination и возвращает канал сообщений.
func (c *Client) Subscribe(destination string) (*Subscriber, <-chan Message, error) {
    return c.subscribe(destination, "")
}

// SubscribeDurable создаёт durable подписку на топик с указанным consumerID.
func (c *Client) SubscribeDurable(topic, consumerID string) (*Subscriber, <-chan Message, error) {
    return c.subscribe(topic, consumerID)
}

func (c *Client) subscribe(destination, consumerID string) (*Subscriber, <-chan Message, error) {
    conn, err := net.Dial("tcp", c.conn.RemoteAddr().String())
    if err != nil {
        return nil, nil, err
    }
    sub := &Subscriber{
        client:     c,
        destName:   destination,
        consumerID: consumerID,
        conn:       conn,
        reader:     bufio.NewReader(conn),
        msgCh:      make(chan Message, 100),
        closeCh:    make(chan struct{}),
    }
    cmd := fmt.Sprintf("SUBSCRIBE %s", destination)
    if consumerID != "" {
        cmd = fmt.Sprintf("SUBSCRIBE %s %s", destination, consumerID)
    }
    _, err = conn.Write([]byte(cmd + "\n"))
    if err != nil {
        conn.Close()
        return nil, nil, err
    }
    resp, err := bufio.NewReader(conn).ReadString('\n')
    if err != nil {
        conn.Close()
        return nil, nil, err
    }
    if strings.HasPrefix(resp, "-ERR") {
        conn.Close()
        return nil, nil, fmt.Errorf("subscribe failed: %s", resp)
    }
    go sub.readLoop()
    return sub, sub.msgCh, nil
}

func (s *Subscriber) readLoop() {
    for {
        select {
        case <-s.closeCh:
            return
        default:
            line, err := s.reader.ReadString('\n')
            if err != nil {
                s.close()
                return
            }
            line = strings.TrimSpace(line)
            if strings.HasPrefix(line, "MSG ") {
                parts := strings.SplitN(line, " ", 4)
                if len(parts) >= 4 {
                    msg := Message{
                        ID:          parts[1],
                        Destination: parts[2],
                        Payload:     parts[3],
                    }
                    select {
                    case s.msgCh <- msg:
                    case <-s.closeCh:
                        return
                    }
                }
            }
        }
    }
}

// Close закрывает подписку.
func (s *Subscriber) Close() error {
    s.closeOnce.Do(func() {
        close(s.closeCh)
        s.conn.Close()
        close(s.msgCh)
    })
    return nil
}

// Ack отправляет подтверждение для сообщения (для очередей).
func (s *Subscriber) Ack(msgID string) error {
    return s.client.Ack(msgID)
}

func (s *Subscriber) close() {
    s.Close()
}