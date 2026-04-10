package server

import (
    "bufio"
    "log"
    "net"
    "my-message-broker/internal/broker"
    "my-message-broker/internal/protocol"
)

type TCPServer struct {
    host   string
    port   string
    broker *broker.Broker
}

func NewTCPServer(host, port string, b *broker.Broker) *TCPServer {
    return &TCPServer{
        host:   host,
        port:   port,
        broker: b,
    }
}

func (s *TCPServer) Run() error {
    addr := net.JoinHostPort(s.host, s.port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    defer listener.Close()
    log.Printf("TCP server listening on %s", addr)
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("accept error: %v", err)
            continue
        }
        go s.handleConnection(conn)
    }
}

func (s *TCPServer) handleConnection(conn net.Conn) {
    defer conn.Close()
    log.Printf("new connection from %s", conn.RemoteAddr())
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        line := scanner.Text()
        cmd, err := protocol.ParseCommand(line)
        if err != nil {
            conn.Write([]byte(protocol.RespErr + " parse error\n"))
            continue
        }
        resp := s.broker.ProcessCommand(cmd, conn)
        conn.Write([]byte(resp + "\n"))
        // если команда SUBSCRIBE, то соединение остаётся открытым для получения сообщений
        // (они придут асинхронно через sendToSubscriber)
    }
    if err := scanner.Err(); err != nil {
        log.Printf("connection error: %v", err)
    }
    // при закрытии соединения нужно отписать все подписки этого клиента
    s.broker.RemoveSubscriptionsByConn(conn)
}