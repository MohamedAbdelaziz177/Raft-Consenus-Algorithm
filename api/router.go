package api

import (
	"github.com/go-chi/chi/v5"
)

func NewRouter(handler *Handler) *chi.Mux {
	r := chi.NewRouter()

	r.Route("/raft", func(r chi.Router) {
		r.Get("/{key}", handler.Get)
		r.Post("/set", handler.Set)
		r.Delete("/{key}", handler.Delete)
	})

	return r
}
