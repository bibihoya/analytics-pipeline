package aggregation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrBusy = errors.New("another aggregation or export is running")

type Metric struct {
	Name      string   `json:"metric"`
	Dimension string   `json:"dimension"`
	Value     *float64 `json:"value"`
}

type Result struct {
	Date       string `json:"date"`
	Processed  uint64 `json:"processed"`
	Metrics    int    `json:"metrics"`
	DurationMS int64  `json:"duration_ms"`
}

type Service struct {
	ch     *ClickHouse
	pg     *pgxpool.Pool
	s3     *minio.Client
	cfg    Config
	logger *slog.Logger
	mu     sync.Mutex
}

func NewService(ctx context.Context, cfg Config, logger *slog.Logger) (*Service, error) {
	pg, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	s3, err := minio.New(cfg.S3Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""), Secure: cfg.S3Secure})
	if err != nil {
		pg.Close()
		return nil, err
	}
	s := &Service{
		ch: &ClickHouse{url: cfg.ClickHouseURL, user: cfg.ClickHouseUser, password: cfg.ClickHousePassword, client: &http.Client{Timeout: 90 * time.Second}},
		pg: pg, s3: s3, cfg: cfg, logger: logger,
	}
	if err := pg.Ping(ctx); err != nil {
		pg.Close()
		return nil, err
	}
	exists, err := s3.BucketExists(ctx, cfg.S3Bucket)
	if err == nil && !exists {
		err = s3.MakeBucket(ctx, cfg.S3Bucket, minio.MakeBucketOptions{})
	}
	if err != nil {
		pg.Close()
		return nil, fmt.Errorf("prepare S3 bucket: %w", err)
	}
	return s, nil
}

func (s *Service) Close() { s.pg.Close() }

func (s *Service) Run(ctx context.Context, date time.Time) (result Result, err error) {
	if !s.mu.TryLock() {
		return result, ErrBusy
	}
	defer s.mu.Unlock()
	started := time.Now()
	result.Date = date.Format(time.DateOnly)
	s.logger.Info("aggregation_started", "date", result.Date)
	defer func() {
		result.DurationMS = time.Since(started).Milliseconds()
		if err != nil {
			s.logger.Error("aggregation_failed", "date", result.Date, "processed", result.Processed, "duration_ms", result.DurationMS, "error", err)
		} else {
			s.logger.Info("aggregation_completed", "date", result.Date, "processed", result.Processed, "metrics", result.Metrics, "duration_ms", result.DurationMS)
		}
	}()
	params := url.Values{"param_day": {result.Date}}
	var daily struct {
		Processed, DAU, Started, Finished uint64
		TopMovieIDs                       []string `json:"top_movie_ids"`
		TopMovieViews                     []uint64 `json:"top_movie_views"`
		Average                           *float64
	}
	if err = s.ch.query(ctx, dailySQL, params, &daily); err != nil {
		return result, err
	}
	result.Processed = daily.Processed
	metrics := []Metric{
		{Name: "dau", Value: number(daily.DAU)}, {Name: "average_watch_seconds", Value: daily.Average},
		{Name: "views_started", Value: number(daily.Started)}, {Name: "views_finished", Value: number(daily.Finished)},
		{Name: "view_conversion", Value: ratio(daily.Finished, daily.Started)},
	}
	for index, movieID := range daily.TopMovieIDs {
		metrics = append(metrics, Metric{Name: "top_movies", Dimension: movieID, Value: number(daily.TopMovieViews[index])})
	}
	var cohort struct {
		ViewSize                                                     uint64 `json:"view_size"`
		D1, D7, Size, Day0, Day1, Day2, Day3, Day4, Day5, Day6, Day7 uint64
	}
	if err = s.ch.query(ctx, cohortSQL, params, &cohort); err != nil {
		return result, err
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	metrics = append(metrics, Metric{Name: "cohort_size", Dimension: "first_view", Value: number(cohort.ViewSize)})
	for _, item := range []struct {
		day      int
		returned uint64
	}{{1, cohort.D1}, {7, cohort.D7}} {
		var value *float64
		if !date.AddDate(0, 0, item.day).After(today) {
			value = ratio(item.returned, cohort.ViewSize)
		}
		metrics = append(metrics, Metric{Name: "retention_d" + strconv.Itoa(item.day), Value: value})
	}
	metrics = append(metrics, Metric{Name: "cohort_size", Dimension: "first_activity", Value: number(cohort.Size)})
	for day, returned := range []uint64{cohort.Day0, cohort.Day1, cohort.Day2, cohort.Day3, cohort.Day4, cohort.Day5, cohort.Day6, cohort.Day7} {
		var value *float64
		if !date.AddDate(0, 0, day).After(today) {
			value = ratio(returned, cohort.Size)
		}
		metrics = append(metrics, Metric{Name: "cohort_retention", Dimension: strconv.Itoa(day), Value: value})
	}
	computedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err = s.ch.save(ctx, date, metrics, computedAt); err != nil {
		return result, err
	}
	if err = s.retry(ctx, "postgres_write", func() error { return s.savePostgres(ctx, date, metrics, computedAt) }); err != nil {
		return result, err
	}
	result.Metrics = len(metrics)
	return result, nil
}

func number(value uint64) *float64 {
	result := float64(value)
	return &result
}

func ratio(numerator, denominator uint64) *float64 {
	if denominator == 0 {
		return nil
	}
	value := float64(numerator) / float64(denominator)
	return &value
}

func (s *Service) retry(ctx context.Context, operation string, fn func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		s.logger.Warn("operation_retry", "operation", operation, "attempt", attempt+1, "error", err)
		if attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond * time.Duration(1<<attempt)):
		}
	}
	return err
}
