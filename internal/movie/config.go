package movie

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cinema/internal/config"
)

type Config struct {
	HTTPAddr, RegistryURL, Topic string
	Brokers                      []string
	GeneratorEnabled             bool
	GeneratorInterval            time.Duration
	HistoryDays                  int
}

func LoadConfig() (Config, error) {
	result := Config{
		HTTPAddr: config.Env("HTTP_ADDR", ":8080"), RegistryURL: config.Env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		Topic: config.Env("KAFKA_TOPIC", "movie-events"), Brokers: strings.Split(config.Env("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ","),
	}
	var err error
	result.GeneratorEnabled, err = strconv.ParseBool(config.Env("GENERATOR_ENABLED", "true"))
	if err != nil {
		return result, fmt.Errorf("GENERATOR_ENABLED: %w", err)
	}
	result.GeneratorInterval, err = config.Duration("GENERATOR_INTERVAL", "5s")
	if err != nil {
		return result, err
	}
	result.HistoryDays, err = config.Int("GENERATOR_HISTORY_DAYS", 8)
	if err != nil || result.HistoryDays > 30 {
		return result, fmt.Errorf("GENERATOR_HISTORY_DAYS must be between 0 and 30")
	}
	return result, nil
}
