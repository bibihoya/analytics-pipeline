package event

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	EventID         string    `json:"event_id"`
	UserID          string    `json:"user_id"`
	MovieID         string    `json:"movie_id"`
	EventType       string    `json:"event_type"`
	Timestamp       time.Time `json:"timestamp"`
	DeviceType      string    `json:"device_type"`
	SessionID       string    `json:"session_id"`
	ProgressSeconds int32     `json:"progress_seconds"`
}

func (e Event) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil || len(e.EventID) != 36 {
		return fmt.Errorf("event_id must be a UUID")
	}
	if strings.TrimSpace(e.UserID) == "" || strings.TrimSpace(e.SessionID) == "" {
		return fmt.Errorf("user_id and session_id are required")
	}
	switch e.EventType {
	case "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED":
		if strings.TrimSpace(e.MovieID) == "" {
			return fmt.Errorf("movie_id is required for %s", e.EventType)
		}
	case "SEARCHED":
	default:
		return fmt.Errorf("unsupported event_type")
	}
	switch e.DeviceType {
	case "MOBILE", "DESKTOP", "TV", "TABLET":
	default:
		return fmt.Errorf("unsupported device_type")
	}
	if e.Timestamp.IsZero() || e.Timestamp.Year() < 1970 || e.Timestamp.Year() > 2200 {
		return fmt.Errorf("timestamp must be a valid datetime between 1970 and 2200")
	}
	if e.ProgressSeconds < 0 {
		return fmt.Errorf("progress_seconds must be non-negative")
	}
	return nil
}
