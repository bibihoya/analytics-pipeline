package movie

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"cinema/internal/event"
)

func (p *Producer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := p.client.Ping(ctx); err != nil {
			http.Error(w, "Kafka unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		decoder.DisallowUnknownFields()
		var input struct {
			event.Event
			MovieID         *string         `json:"movie_id"`
			ProgressSeconds json.RawMessage `json:"progress_seconds"`
		}
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			http.Error(w, "expected exactly one JSON event", http.StatusBadRequest)
			return
		}
		if input.MovieID == nil {
			http.Error(w, "movie_id must be a string", http.StatusBadRequest)
			return
		}
		value := input.Event
		value.MovieID = *input.MovieID
		if len(input.ProgressSeconds) > 0 {
			if bytes.Equal(bytes.TrimSpace(input.ProgressSeconds), []byte("null")) || json.Unmarshal(input.ProgressSeconds, &value.ProgressSeconds) != nil {
				http.Error(w, "progress_seconds must be an integer", http.StatusBadRequest)
				return
			}
		}
		if err := value.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
		defer cancel()
		if err := p.Publish(ctx, value); err != nil {
			http.Error(w, "event publication failed", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"event_id": value.EventID})
	})
	return mux
}
