package brokerclient

// Message представляет полученное сообщение (для подписчика)
type Message struct {
    ID          string
    Destination string
    Payload     string
}