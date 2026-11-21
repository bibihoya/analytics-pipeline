package movie

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"cinema/internal/event"
	"github.com/google/uuid"
)

func (p *Producer) Generate(ctx context.Context, cfg Config) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for daysAgo := cfg.HistoryDays; daysAgo >= 1; daysAgo-- {
		for user := 0; user < 40; user++ {
			if user%cfg.HistoryDays < daysAgo-1 && user%3 == 0 {
				continue
			}
			at := today.AddDate(0, 0, -daysAgo).Add(10*time.Hour + time.Duration(user)*time.Minute)
			if err := p.session(ctx, fmt.Sprintf("demo-user-%02d", user), at, true); err != nil {
				p.logger.Error("generator_history_failed", "error", err)
				return
			}
		}
	}
	p.logger.Info("generator_history_completed", "days", cfg.HistoryDays)
	ticker := time.NewTicker(cfg.GeneratorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.session(ctx, fmt.Sprintf("live-user-%02d", rand.IntN(50)), time.Now().UTC(), false); err != nil {
				p.logger.Error("generator_session_failed", "error", err)
			}
		}
	}
}

func (p *Producer) session(ctx context.Context, user string, at time.Time, historical bool) error {
	devices := []string{"MOBILE", "DESKTOP", "TV", "TABLET"}
	session := event.Event{UserID: user, MovieID: fmt.Sprintf("movie-%02d", rand.IntN(10)+1), DeviceType: devices[rand.IntN(len(devices))], SessionID: uuid.NewString()}
	types := []string{"SEARCHED", "VIEW_STARTED", "VIEW_PAUSED", "VIEW_RESUMED", "VIEW_FINISHED", "LIKED"}
	progress := []int32{0, 0, 600, 600, 5400, 5400}
	if rand.IntN(4) == 0 {
		types, progress = types[:3], progress[:3]
	}
	for index, kind := range types {
		if !historical && index > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
		session.EventID = uuid.NewString()
		session.EventType, session.ProgressSeconds = kind, progress[index]
		if historical {
			session.Timestamp = at.Add(time.Duration(index)*time.Second + time.Duration(progress[index])*time.Second)
		} else {
			session.Timestamp = time.Now().UTC()
		}
		for {
			if err := p.Publish(ctx, session); err == nil {
				break
			} else {
				p.logger.Warn("generator_retry", "event_id", session.EventID, "error", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	return nil
}
