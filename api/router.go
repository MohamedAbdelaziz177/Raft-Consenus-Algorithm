package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
)

func NewRouter(handler *Handler) *chi.Mux {
	r := chi.NewRouter()

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			"GET",
			"POST",
			"PUT",
			"DELETE",
			"OPTIONS",
		},
		AllowedHeaders: []string{
			"Accept",
			"Authorization",
			"Content-Type",
		},
	}))

	r.Route("/raft", func(r chi.Router) {
		r.Get("/{key}", handler.Get)
		r.Post("/set", handler.Set)
		r.Delete("/{key}", handler.Delete)
		r.Get("/debug/state", handler.debugState)
	})

	return r
}
