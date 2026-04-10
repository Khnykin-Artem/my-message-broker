package main

import (
    "flag"
    "log"
    "my-message-broker/internal/broker"
    "my-message-broker/internal/server"
)

func main() {
    host := flag.String("host", "0.0.0.0", "host to bind")
    port := flag.String("port", "5555", "port to bind")
    visibilityTimeout := flag.Int("visibility-timeout", 30, "visibility timeout in seconds")
    maxAttempts := flag.Int("max-delivery-attempts", 3, "max delivery attempts")
    flag.Parse()

    b := broker.NewBroker(*visibilityTimeout, *maxAttempts)
    srv := server.NewTCPServer(*host, *port, b)
    log.Fatal(srv.Run())
}