package aggregation

import (
	"context"
	"time"
)

func (s *Service) Schedule(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		s.scheduledCycle(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) scheduledCycle(ctx context.Context) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for daysAgo := 8; daysAgo >= 0; daysAgo-- {
		if ctx.Err() != nil {
			return
		}
		date := today.AddDate(0, 0, -daysAgo)
		cycle, cancel := context.WithTimeout(ctx, 3*time.Minute)
		_, err := s.Run(cycle, date)
		if err == nil && daysAgo > 0 {
			_, err = s.Export(cycle, date)
		}
		cancel()
		if err != nil {
			s.logger.Error("scheduled_cycle_failed", "date", date.Format(time.DateOnly), "error", err, "retry_on_next_cycle", true)
		}
	}
}
