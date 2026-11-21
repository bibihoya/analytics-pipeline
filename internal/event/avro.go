package event

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cinema/schemas"
	"github.com/linkedin/goavro/v2"
)

type Encoder struct {
	codec    *goavro.Codec
	schemaID uint32
	Version  int
}

func NewEncoder(ctx context.Context, registryURL, subject string) (*Encoder, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	call := func(method, path string, value, result any) error {
		body, err := json.Marshal(value)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(registryURL, "/")+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return fmt.Errorf("schema registry HTTP %d: %s", response.StatusCode, message)
		}
		if result != nil {
			return json.NewDecoder(response.Body).Decode(result)
		}
		return nil
	}
	if err := call(http.MethodPut, "/config/"+subject, map[string]string{"compatibility": "BACKWARD_TRANSITIVE"}, nil); err != nil {
		return nil, err
	}
	var registered struct {
		ID uint32 `json:"id"`
	}
	payload := map[string]string{"schemaType": "AVRO", "schema": schemas.MovieEvent}
	if err := call(http.MethodPost, "/subjects/"+subject+"/versions", payload, &registered); err != nil {
		return nil, err
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := call(http.MethodPost, "/subjects/"+subject, payload, &version); err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schemas.MovieEvent)
	if err != nil {
		return nil, err
	}
	return &Encoder{codec: codec, schemaID: registered.ID, Version: version.Version}, nil
}

func (e *Encoder) Encode(value Event) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:], e.schemaID)
	return e.codec.BinaryFromNative(header, map[string]any{
		"event_id": value.EventID, "user_id": value.UserID, "movie_id": value.MovieID,
		"event_type": value.EventType, "timestamp": value.Timestamp.UTC(), "device_type": value.DeviceType,
		"session_id": value.SessionID, "progress_seconds": value.ProgressSeconds,
	})
}
