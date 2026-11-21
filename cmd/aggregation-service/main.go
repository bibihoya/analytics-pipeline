package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cinema/internal/aggregation"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service_failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := aggregation.LoadConfig()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stopStartup := context.WithTimeout(ctx, 45*time.Second)
	service, err := aggregation.NewService(startup, cfg, logger)
	stopStartup()
	if err != nil {
		return err
	}
	defer service.Close()
	var workers sync.WaitGroup
	workers.Go(func() { service.Schedule(ctx) })
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: service.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 190 * time.Second, IdleTimeout: 60 * time.Second}
	workers.Go(func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		server.Shutdown(shutdown)
	})
	logger.Info("service_started", "service", "aggregation", "addr", cfg.HTTPAddr, "interval", cfg.Interval.String())
	err = server.ListenAndServe()
	cancel()
	workers.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
