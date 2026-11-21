package aggregation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.pg.Ping(ctx); err != nil {
			http.Error(w, "PostgreSQL unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := s.ch.query(ctx, "SELECT 1", nil, nil); err != nil {
			http.Error(w, "ClickHouse unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	for _, path := range []string{"POST /aggregate", "POST /export"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			date, err := time.Parse(time.DateOnly, r.URL.Query().Get("date"))
			if err != nil || date.After(time.Now().UTC().Truncate(24*time.Hour)) || date.Year() < 1970 {
				http.Error(w, "date must be YYYY-MM-DD in UTC, between 1970-01-01 and today", http.StatusBadRequest)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
			defer cancel()
			var result any
			if r.URL.Path == "/aggregate" {
				result, err = s.Run(ctx, date)
			} else {
				var key string
				key, err = s.Export(ctx, date)
				result = map[string]string{"bucket": s.cfg.S3Bucket, "key": key}
			}
			if err != nil {
				status := http.StatusServiceUnavailable
				if errors.Is(err, ErrBusy) {
					status = http.StatusConflict
				}
				http.Error(w, err.Error(), status)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
		})
	}
	return mux
}
