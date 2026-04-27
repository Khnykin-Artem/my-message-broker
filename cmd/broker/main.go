package main

import (
    "crypto/tls"
    "flag"
    "fmt"
    "log"
    "os"

    "gopkg.in/yaml.v3"
    "my-message-broker/internal/auth"
    "my-message-broker/internal/broker"
    "my-message-broker/internal/metrics"
    "my-message-broker/internal/server"
)

type Config struct {
    Server struct {
        Host string `yaml:"host"`
        Port int    `yaml:"port"`
        TLS  struct {
            Enabled  bool   `yaml:"enabled"`
            CertFile string `yaml:"cert_file"`
            KeyFile  string `yaml:"key_file"`
        } `yaml:"tls"`
    } `yaml:"server"`
    Storage struct {
        Type    string `yaml:"type"`
        DataDir string `yaml:"data_dir"`
    } `yaml:"storage"`
    Auth struct {
        Enabled bool     `yaml:"enabled"`
        Tokens  []string `yaml:"tokens"`
    } `yaml:"auth"`
    Metrics struct {
        Enabled bool   `yaml:"enabled"`
        Port    int    `yaml:"port"`
        Path    string `yaml:"path"`
    } `yaml:"metrics"`
    Broker struct {
        VisibilityTimeoutSec int `yaml:"visibility_timeout_sec"`
        MaxDeliveryAttempts  int `yaml:"max_delivery_attempts"`
    } `yaml:"broker"`
}

func main() {
    configPath := flag.String("config", "configs/config.yaml", "path to config file")
    flag.Parse()

    data, err := os.ReadFile(*configPath)
    if err != nil {
        log.Fatalf("read config: %v", err)
    }
    var cfg Config
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        log.Fatalf("parse config: %v", err)
    }

    useWal := cfg.Storage.Type == "wal"
    walPath := ""
    if useWal {
        walPath = cfg.Storage.DataDir + "/wal.log"
    }
    b, err := broker.NewBroker(
        cfg.Broker.VisibilityTimeoutSec,
        cfg.Broker.MaxDeliveryAttempts,
        walPath,
        useWal,
    )
    if err != nil {
        log.Fatalf("create broker: %v", err)
    }

    var authenticator *auth.Authenticator
    if cfg.Auth.Enabled {
        authenticator = auth.NewAuthenticator(cfg.Auth.Tokens)
    }

    var tlsConfig *tls.Config
    if cfg.Server.TLS.Enabled {
        cert, err := tls.LoadX509KeyPair(cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
        if err != nil {
            log.Fatalf("load TLS cert: %v", err)
        }
        tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
    }

    tcpServer := server.NewTCPServer(
        cfg.Server.Host,
        fmt.Sprintf("%d", cfg.Server.Port),
        tlsConfig,
        b,
        authenticator,
        cfg.Auth.Enabled,
    )

    // Запуск сервера метрик
    if cfg.Metrics.Enabled {
        metricsAddr := fmt.Sprintf(":%d", cfg.Metrics.Port)
        metricsServer := metrics.NewMetricsServer(b, metricsAddr, cfg.Metrics.Path)
        go func() {
            if err := metricsServer.Run(); err != nil {
                log.Printf("metrics server error: %v", err)
            }
        }()
    }

    log.Fatal(tcpServer.Run())
}