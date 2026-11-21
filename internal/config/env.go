package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func Env(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func Duration(name, fallback string) (time.Duration, error) {
	value, err := time.ParseDuration(Env(name, fallback))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func Int(name string, fallback int) (int, error) {
	value, err := strconv.Atoi(Env(name, strconv.Itoa(fallback)))
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}
