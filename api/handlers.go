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

func NewHandler(node *raft.RaftNode) Handler {
	return Handler{
		node: node,
	}
}

func (handler *Handler) Set(w http.ResponseWriter, r *http.Request) {

	var req *SetRequest
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	cmd := storage.Command{
		Key:       req.key,
		Value:     req.val,
		Operation: "SET",
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idx, succ := handler.node.Execute(cmdBytes)

	if !succ {
		w.Write([]byte("Request failed since requested replica is not the leader"))
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": fmt.Sprintf("log entry appended at index %d", idx),
	})
}

func (handler *Handler) Delete(w http.ResponseWriter, r *http.Request) {
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
		w.Write([]byte("Request failed since requested replica is not the leader"))
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": fmt.Sprintf("log entry appended at index %d", idx),
	})

}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
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

type SetRequest struct {
	key string `json:"key"`
	val string `json:"value"`
}
