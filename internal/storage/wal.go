package storage

import (
    "encoding/json"
    "os"
    "sync"
)

type EntryType string

const (
    EntryPublish EntryType = "PUBLISH"
    EntryAck     EntryType = "ACK"
    EntryCreate  EntryType = "CREATE"
    EntryDelete  EntryType = "DELETE"
)

type WalEntry struct {
    Type    EntryType   `json:"type"`
    Data    interface{} `json:"data"`
}

type Wal struct {
    file *os.File
    mu   sync.Mutex
}

func NewWal(path string) (*Wal, error) {
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, err
    }
    return &Wal{file: f}, nil
}

func (w *Wal) Append(entry WalEntry) error {
    w.mu.Lock()
    defer w.mu.Unlock()
    data, err := json.Marshal(entry)
    if err != nil {
        return err
    }
    data = append(data, '\n')
    _, err = w.file.Write(data)
    return err
}

func (w *Wal) Replay(callback func(WalEntry)) error {
    w.mu.Lock()
    defer w.mu.Unlock()
    f, err := os.Open(w.file.Name())
    if err != nil {
        return err
    }
    defer f.Close()
    dec := json.NewDecoder(f)
    for {
        var entry WalEntry
        if err := dec.Decode(&entry); err != nil {
            break
        }
        callback(entry)
    }
    return nil
}

func (w *Wal) Close() error {
    return w.file.Close()
}