package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventReachesClickHouse(t *testing.T) {
	producerURL := os.Getenv("TEST_PRODUCER_URL")
	clickhouseURL := os.Getenv("TEST_CLICKHOUSE_URL")
	if producerURL == "" || clickhouseURL == "" {
		t.Skip("run with docker compose --profile test run --build --rm integration-test")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	now := time.Now().UTC().Truncate(time.Millisecond)
	id := uuid.NewString()
	want := map[string]any{
		"event_id": id, "user_id": "integration-" + id, "movie_id": "test-movie",
		"event_type": "VIEW_PAUSED", "timestamp": now.Format(time.RFC3339Nano),
		"device_type": "TABLET", "session_id": "test-session-" + id, "progress_seconds": 123,
	}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Post(producerURL+"/events", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("publish through producer: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("publish: HTTP %d: %s", response.StatusCode, body)
	}
	var published struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(body, &published); err != nil || published.EventID != id {
		t.Fatalf("unexpected publication response: %s, error: %v", body, err)
	}
	query := fmt.Sprintf(`SELECT event_id, user_id, movie_id, event_type,
toUnixTimestamp64Milli(timestamp) AS timestamp_ms,
device_type, session_id, progress_seconds FROM cinema.raw_events FINAL
WHERE event_id = '%s' FORMAT JSONEachRow`, id)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodPost, clickhouseURL, bytes.NewBufferString(query))
		if err != nil {
			t.Fatal(err)
		}
		request.SetBasicAuth("default", os.Getenv("TEST_CLICKHOUSE_PASSWORD"))
		result, err := client.Do(request)
		if err != nil {
			t.Logf("waiting for ClickHouse: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		data, readErr := io.ReadAll(result.Body)
		result.Body.Close()
		if readErr != nil || result.StatusCode != http.StatusOK {
			t.Fatalf("ClickHouse: HTTP %d: %s, error: %v", result.StatusCode, data, readErr)
		}
		if len(bytes.TrimSpace(data)) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var got struct {
			EventID         string `json:"event_id"`
			UserID          string `json:"user_id"`
			MovieID         string `json:"movie_id"`
			EventType       string `json:"event_type"`
			TimestampMS     int64  `json:"timestamp_ms,string"`
			DeviceType      string `json:"device_type"`
			SessionID       string `json:"session_id"`
			ProgressSeconds int    `json:"progress_seconds"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(data), &got); err != nil {
			t.Fatalf("decode stored event: %v: %s", err, data)
		}
		if got.EventID != id || got.UserID != want["user_id"] || got.MovieID != want["movie_id"] ||
			got.EventType != want["event_type"] || got.TimestampMS != now.UnixMilli() ||
			got.DeviceType != want["device_type"] || got.SessionID != want["session_id"] || got.ProgressSeconds != 123 {
			t.Fatalf("stored event differs: %+v; expected: %+v", got, want)
		}
		t.Logf("event_id=%s reached ClickHouse with all fields intact", id)
		return
	}
	t.Fatalf("event_id=%s did not reach ClickHouse within 60s", id)
}
