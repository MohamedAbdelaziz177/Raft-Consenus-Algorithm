package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/raft"
	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/storage"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	node  *raft.RaftNode
	store *storage.KVStore
}

func NewHandler(node *raft.RaftNode, store *storage.KVStore) Handler {
	return Handler{
		node:  node,
		store: store,
	}
}

func (handler *Handler) Set(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd := storage.Command{
		Key:       req.Key,
		Value:     req.Value,
		Operation: "SET",
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idx, succ := handler.node.Execute(cmdBytes)

	if !succ {
		writeNotLeader(w, handler.node.LeaderID())
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": fmt.Sprintf("log entry appended at index %d", idx),
	})
}

func (handler *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	key := chi.URLParam(r, "key")

	cmd := storage.Command{
		Key:       key,
		Operation: "DELETE",
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idx, succ := handler.node.Execute(cmdBytes)

	if !succ {
		writeNotLeader(w, handler.node.LeaderID())
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": fmt.Sprintf("log entry appended at index %d", idx),
	})

}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	key := chi.URLParam(r, "key")

	value, ok := h.store.Get(key)

	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": value,
	})
}

func (handler *Handler) debugState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	handler.node.DebugNodeState(w)
}

func writeNotLeader(w http.ResponseWriter, leaderID int32) {
	response := map[string]any{
		"success": false,
		"message": "requested replica is not the leader",
	}
	if leaderID >= 0 {
		response["leaderId"] = leaderID
	}

	w.WriteHeader(http.StatusConflict)
	json.NewEncoder(w).Encode(response)
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
