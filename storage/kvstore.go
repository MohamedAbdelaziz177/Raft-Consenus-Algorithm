package storage

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/raft"
)

type Command struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type KVStore struct {
	mu   *sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
		mu:   &sync.RWMutex{},
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, ok := kv.data[key]
	return value, ok
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KVStore) Delete(key string) {
	kv.mu.Lock()
	delete(kv.data, key)
	kv.mu.Unlock()
}

func (kv *KVStore) ListenToApplyChannel(chann <-chan raft.ApplyMsg) {
	go func() {
		for msg := range chann {
			if !msg.CommandValid {
				continue
			}

			var cmd *Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				fmt.Printf("invalid command: %v", err)
				continue
			}

			switch cmd.Operation {
			case "SET":
				kv.Set(cmd.Key, cmd.Value)
			case "DELETE":
				kv.Delete(cmd.Key)
			}
		}
	}()
}
