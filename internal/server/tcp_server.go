package server

import (
    "bufio"
    "crypto/tls"
    "log"
    "net"
    "my-message-broker/internal/auth"
    "my-message-broker/internal/broker"
    "my-message-broker/internal/protocol"
)

type TCPServer struct {
    host          string
    port          string
    tlsConfig     *tls.Config
    broker        *broker.Broker
    authenticator *auth.Authenticator
    authEnabled   bool
}

func NewTCPServer(host, port string, tlsCfg *tls.Config, b *broker.Broker, auth *auth.Authenticator, authEnabled bool) *TCPServer {
    return &TCPServer{
        host:          host,
        port:          port,
        tlsConfig:     tlsCfg,
        broker:        b,
        authenticator: auth,
        authEnabled:   authEnabled,
    }
}

func (s *TCPServer) Run() error {
    addr := net.JoinHostPort(s.host, s.port)
    var listener net.Listener
    var err error
    if s.tlsConfig != nil {
        listener, err = tls.Listen("tcp", addr, s.tlsConfig)
    } else {
        listener, err = net.Listen("tcp", addr)
    }
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

type clientState struct {
    authenticated bool
}

func (s *TCPServer) handleConnection(conn net.Conn) {
    defer conn.Close()
    state := &clientState{authenticated: false}
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        line := scanner.Text()
        // Если аутентификация включена и клиент ещё не аутентифицировался, разрешаем только AUTH
        if s.authEnabled && !state.authenticated {
            if cmd, _ := protocol.ParseCommand(line); cmd != nil && cmd.Name == "AUTH" && len(cmd.Args) >= 1 {
                if s.authenticator.Authenticate(cmd.Args[0]) {
                    state.authenticated = true
                    conn.Write([]byte(protocol.RespOk + "\n"))
                } else {
                    conn.Write([]byte(protocol.RespErr + " invalid token\n"))
                    return
                }
                continue
            } else {
                conn.Write([]byte(protocol.RespErr + " authentication required\n"))
                continue
            }
        }
        cmd, err := protocol.ParseCommand(line)
        if err != nil {
            conn.Write([]byte(protocol.RespErr + " parse error\n"))
            continue
        }
        resp := s.broker.ProcessCommand(cmd, conn)
        conn.Write([]byte(resp + "\n"))
    }
    if err := scanner.Err(); err != nil {
        log.Printf("connection error: %v", err)
    }
    s.broker.RemoveSubscriptionsByConn(conn)
}