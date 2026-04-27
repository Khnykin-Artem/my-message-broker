package metrics

import (
    "net/http"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "my-message-broker/internal/broker"
)

var (
    MessagesPublished = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "broker_messages_published_total",
            Help: "Total number of published messages",
        },
        []string{"destination"},
    )
    QueueDepth = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "broker_queue_depth",
            Help: "Current depth of a queue",
        },
        []string{"queue"},
    )
    ActiveSubscribers = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "broker_active_subscribers",
            Help: "Number of active subscribers per destination",
        },
        []string{"destination"},
    )
)

func init() {
    prometheus.MustRegister(MessagesPublished)
    prometheus.MustRegister(QueueDepth)
    prometheus.MustRegister(ActiveSubscribers)
}

type MetricsServer struct {
    broker *broker.Broker
    addr   string
    path   string
}

func NewMetricsServer(b *broker.Broker, addr, path string) *MetricsServer {
    return &MetricsServer{
        broker: b,
        addr:   addr,
        path:   path,
    }
}

func (ms *MetricsServer) Run() error {
    http.Handle(ms.path, promhttp.Handler())
    // дополнительный административный API
    http.HandleFunc("/api/v1/queues", ms.handleQueues)
    http.HandleFunc("/api/v1/queues/", ms.handleQueue)
    return http.ListenAndServe(ms.addr, nil)
}

func (ms *MetricsServer) handleQueues(w http.ResponseWriter, r *http.Request) {
    // выводит список очередей в JSON
}

func (ms *MetricsServer) handleQueue(w http.ResponseWriter, r *http.Request) {
    // обработка DELETE /api/v1/queues/{name}
}