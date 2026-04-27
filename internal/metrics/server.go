package metrics

import (
    "encoding/json"
    "log"
    "net/http"
    "strings"

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
    http.HandleFunc("/api/v1/queues", ms.handleQueues)
    http.HandleFunc("/api/v1/queues/", ms.handleQueue)
    return http.ListenAndServe(ms.addr, nil)
}

// handleQueues возвращает список всех очередей с их глубиной
func (ms *MetricsServer) handleQueues(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "only GET allowed", http.StatusMethodNotAllowed)
        return
    }
    // Получаем данные из брокера
    queues := ms.broker.GetQueuesStats() // возвращает map[string]int (имя -> глубина)
    resp := make([]map[string]interface{}, 0, len(queues))
    for name, depth := range queues {
        resp = append(resp, map[string]interface{}{
            "name":  name,
            "depth": depth,
        })
    }
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(resp); err != nil {
        log.Printf("failed to encode queues response: %v", err)
    }
}

// handleQueue обрабатывает DELETE /api/v1/queues/{name}
func (ms *MetricsServer) handleQueue(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodDelete {
        http.Error(w, "only DELETE allowed", http.StatusMethodNotAllowed)
        return
    }
    // Извлекаем имя очереди из пути
    path := strings.TrimPrefix(r.URL.Path, "/api/v1/queues/")
    if path == "" {
        http.Error(w, "queue name required", http.StatusBadRequest)
        return
    }
    // Удаляем очередь через публичный метод брокера
    err := ms.broker.DeleteQueue(path)
    if err != nil {
        if strings.Contains(err.Error(), "not found") {
            http.Error(w, err.Error(), http.StatusNotFound)
        } else {
            http.Error(w, err.Error(), http.StatusBadRequest)
        }
        return
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "status":  "ok",
        "message": "queue " + path + " deleted",
    })
}