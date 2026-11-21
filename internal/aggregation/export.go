package aggregation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
)

func (s *Service) Export(ctx context.Context, date time.Time) (key string, err error) {
	if !s.mu.TryLock() {
		return "", ErrBusy
	}
	defer s.mu.Unlock()
	started := time.Now()
	key = "daily/" + date.Format(time.DateOnly) + "/aggregates.json"
	defer func() {
		if err != nil {
			s.logger.Error("export_failed", "date", date.Format(time.DateOnly), "key", key, "error", err)
		} else {
			s.logger.Info("export_completed", "date", date.Format(time.DateOnly), "key", key, "duration_ms", time.Since(started).Milliseconds())
		}
	}()
	var metrics []struct {
		Metric
		ComputedAt time.Time `json:"computed_at"`
	}
	err = s.retry(ctx, "postgres_read_export", func() error {
		rows, queryErr := s.pg.Query(ctx, "SELECT metric, dimension, value, computed_at FROM daily_metrics WHERE date = $1 ORDER BY metric, dimension", date)
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()
		metrics = nil
		for rows.Next() {
			var metric struct {
				Metric
				ComputedAt time.Time `json:"computed_at"`
			}
			if queryErr = rows.Scan(&metric.Name, &metric.Dimension, &metric.Value, &metric.ComputedAt); queryErr != nil {
				return queryErr
			}
			metrics = append(metrics, metric)
		}
		return rows.Err()
	})
	if err != nil {
		return key, err
	}
	if len(metrics) == 0 {
		return key, fmt.Errorf("no aggregates for %s; run aggregation first", date.Format(time.DateOnly))
	}
	payload, err := json.MarshalIndent(struct {
		Date    string `json:"date"`
		Metrics any    `json:"metrics"`
	}{Date: date.Format(time.DateOnly), Metrics: metrics}, "", "  ")
	if err != nil {
		return key, err
	}
	err = s.retry(ctx, "s3_put", func() error {
		_, uploadErr := s.s3.PutObject(ctx, s.cfg.S3Bucket, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{ContentType: "application/json"})
		return uploadErr
	})
	return key, err
}
