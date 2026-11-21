package aggregation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type ClickHouse struct {
	url, user, password string
	client              *http.Client
}

func (c *ClickHouse) query(ctx context.Context, sql string, params url.Values, result any) error {
	endpoint, err := url.Parse(c.url)
	if err != nil {
		return err
	}
	query := endpoint.Query()
	query.Set("database", "cinema")
	query.Set("output_format_json_quote_64bit_integers", "0")
	query.Set("date_time_input_format", "best_effort")
	query.Set("max_execution_time", "60")
	query.Set("wait_end_of_query", "1")
	for key, values := range params {
		query[key] = values
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(sql))
	if err != nil {
		return err
	}
	request.SetBasicAuth(c.user, c.password)
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8192))
		return fmt.Errorf("ClickHouse HTTP %d: %s", response.StatusCode, body)
	}
	if result != nil {
		if err := json.NewDecoder(response.Body).Decode(result); err != nil {
			return err
		}
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}

func (c *ClickHouse) save(ctx context.Context, date time.Time, metrics []Metric, computedAt time.Time) error {
	version := uint64(computedAt.UnixMicro())
	var body bytes.Buffer
	body.WriteString("INSERT INTO cinema.aggregate_values FORMAT JSONEachRow\n")
	encoder := json.NewEncoder(&body)
	for _, metric := range metrics {
		if err := encoder.Encode(struct {
			Date       string   `json:"date"`
			Metric     string   `json:"metric"`
			Dimension  string   `json:"dimension"`
			Value      *float64 `json:"value"`
			ComputedAt string   `json:"computed_at"`
			Version    uint64   `json:"version"`
		}{date.Format(time.DateOnly), metric.Name, metric.Dimension, metric.Value, computedAt.Format(time.RFC3339Nano), version}); err != nil {
			return err
		}
	}
	if err := c.query(ctx, body.String(), nil, nil); err != nil {
		return err
	}
	params := url.Values{"param_day": {date.Format(time.DateOnly)}, "param_version": {fmt.Sprint(version)}}
	return c.query(ctx, "INSERT INTO cinema.aggregate_runs (date, version) VALUES ({day:Date}, {version:UInt64})", params, nil)
}
