package aggregation

import (
	"context"
	"time"
)

func (s *Service) savePostgres(ctx context.Context, date time.Time, metrics []Metric, computedAt time.Time) error {
	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, metric := range metrics {
		_, err = tx.Exec(ctx, `INSERT INTO daily_metrics (date, metric, dimension, value, computed_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (date, metric, dimension) DO UPDATE
SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at`, date, metric.Name, metric.Dimension, metric.Value, computedAt)
		if err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "DELETE FROM daily_metrics WHERE date = $1 AND computed_at <> $2", date, computedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
