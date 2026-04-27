package storage

import (
    "encoding/json"
    "os"
    "sync"
)

type Index struct {
    mu      sync.RWMutex
    data    map[string]map[string]int64 // queueName -> messageID -> offset
    file    *os.File
}

func NewIndex(path string) (*Index, error) {
    idx := &Index{
        data: make(map[string]map[string]int64),
    }
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return nil, err
    }
    idx.file = f
    // загружаем существующий индекс
    dec := json.NewDecoder(f)
    if err := dec.Decode(&idx.data); err != nil && err.Error() != "EOF" {
        // не критично
    }
    return idx, nil
}

func (idx *Index) Set(queue, msgID string, offset int64) {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    if _, ok := idx.data[queue]; !ok {
        idx.data[queue] = make(map[string]int64)
    }
    idx.data[queue][msgID] = offset
}

func (idx *Index) Delete(queue, msgID string) {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    if m, ok := idx.data[queue]; ok {
        delete(m, msgID)
    }
}

func (idx *Index) Get(queue, msgID string) (int64, bool) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    if m, ok := idx.data[queue]; ok {
        off, ok := m[msgID]
        return off, ok
    }
    return 0, false
}

func (idx *Index) Save() error {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    idx.file.Truncate(0)
    idx.file.Seek(0, 0)
    enc := json.NewEncoder(idx.file)
    return enc.Encode(idx.data)
}