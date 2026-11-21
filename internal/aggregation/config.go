package aggregation

import (
	"fmt"
	"strconv"
	"time"

	"cinema/internal/config"
)

type Config struct {
	HTTPAddr, ClickHouseURL, ClickHouseUser, ClickHousePassword string
	PostgresDSN, S3Endpoint, S3AccessKey, S3SecretKey, S3Bucket string
	S3Secure                                                    bool
	Interval                                                    time.Duration
}

func LoadConfig() (Config, error) {
	cfg := Config{
		HTTPAddr: config.Env("HTTP_ADDR", ":8081"), ClickHouseURL: config.Env("CLICKHOUSE_URL", "http://clickhouse:8123"),
		ClickHouseUser: config.Env("CLICKHOUSE_USER", "default"), ClickHousePassword: config.Env("CLICKHOUSE_PASSWORD", "cinema"),
		PostgresDSN: config.Env("POSTGRES_DSN", "postgres://cinema:cinema@postgres:5432/cinema?sslmode=disable"),
		S3Endpoint:  config.Env("S3_ENDPOINT", "minio:9000"), S3AccessKey: config.Env("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey: config.Env("S3_SECRET_KEY", "minioadmin"), S3Bucket: config.Env("S3_BUCKET", "movie-analytics"),
	}
	var err error
	cfg.Interval, err = config.Duration("AGGREGATION_INTERVAL", "1m")
	if err != nil {
		return cfg, err
	}
	cfg.S3Secure, err = strconv.ParseBool(config.Env("S3_SECURE", "false"))
	if err != nil {
		return cfg, fmt.Errorf("S3_SECURE: %w", err)
	}
	return cfg, nil
}
