package brokerclient

import (
    "bufio"
    "fmt"
    "net"
    "strings"
    "time"
)

// Client представляет соединение с брокером для отправки команд и управления.
type Client struct {
    conn     net.Conn
    reader   *bufio.Reader
    authDone bool
    timeout  time.Duration
}

// Dial устанавливает TCP-соединение с брокером.
func Dial(addr string) (*Client, error) {
    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
    if err != nil {
        return nil, err
    }
    return &Client{
        conn:    conn,
        reader:  bufio.NewReader(conn),
        timeout: 10 * time.Second,
    }, nil
}

// Close закрывает соединение.
func (c *Client) Close() error {
    return c.conn.Close()
}

// SetTimeout устанавливает таймаут на операции чтения/записи.
func (c *Client) SetTimeout(d time.Duration) {
    c.timeout = d
}

// sendCommand отправляет команду и возвращает ответ (первую строку).
// Ответ должен быть +OK или -ERR ...
func (c *Client) sendCommand(cmd string) (string, error) {
    if c.conn == nil {
        return "", ErrNotConnected
    }
    c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
    _, err := c.conn.Write([]byte(cmd + "\n"))
    if err != nil {
        return "", err
    }
    c.conn.SetReadDeadline(time.Now().Add(c.timeout))
    resp, err := c.reader.ReadString('\n')
    if err != nil {
        return "", err
    }
    resp = strings.TrimSpace(resp)
    if strings.HasPrefix(resp, "-ERR") {
        return "", fmt.Errorf("%s", resp)
    }
    if resp != "+OK" {
        return resp, nil // для команд, возвращающих не только +OK (например LIST)
    }
    return resp, nil
}

// Auth отправляет токен аутентификации.
func (c *Client) Auth(token string) error {
    _, err := c.sendCommand("AUTH " + token)
    if err != nil {
        return err
    }
    c.authDone = true
    return nil
}

// CreateQueue создаёт очередь.
func (c *Client) CreateQueue(name string) error {
    _, err := c.sendCommand(fmt.Sprintf("CREATE QUEUE %s", name))
    return err
}

// CreateTopic создаёт топик.
func (c *Client) CreateTopic(name string) error {
    _, err := c.sendCommand(fmt.Sprintf("CREATE TOPIC %s", name))
    return err
}

// DeleteDestination удаляет очередь или топик.
func (c *Client) DeleteDestination(name string) error {
    _, err := c.sendCommand(fmt.Sprintf("DELETE %s", name))
    return err
}

// Publish отправляет сообщение в destination (очередь или топик).
func (c *Client) Publish(destination, payload string) error {
    // экранирование перевода строки в payload? Пока что предполагаем, что payload не содержит \n
    _, err := c.sendCommand(fmt.Sprintf("PUBLISH %s %s", destination, payload))
    return err
}

// List возвращает список всех destination (очередей и топиков).
func (c *Client) List() (string, error) {
    return c.sendCommand("LIST")
}

// Stats возвращает статистику по destination.
func (c *Client) Stats(name string) (string, error) {
    return c.sendCommand(fmt.Sprintf("STATS %s", name))
}

// Purge очищает очередь.
func (c *Client) Purge(queueName string) error {
    _, err := c.sendCommand(fmt.Sprintf("PURGE %s", queueName))
    return err
}

// Ack подтверждает обработку сообщения (для очередей).
func (c *Client) Ack(messageID string) error {
    _, err := c.sendCommand(fmt.Sprintf("ACK %s", messageID))
    return err
}

// Ping проверяет соединение.
func (c *Client) Ping() error {
    _, err := c.sendCommand("PING")
    return err
}