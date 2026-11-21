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

	"cinema/internal/movie"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service_failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := movie.LoadConfig()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stopStartup := context.WithTimeout(ctx, 60*time.Second)
	producer, err := movie.NewProducer(startup, cfg, logger)
	stopStartup()
	if err != nil {
		return err
	}
	defer producer.Close()
	var workers sync.WaitGroup
	if cfg.GeneratorEnabled {
		workers.Go(func() { producer.Generate(ctx, cfg) })
	}
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: producer.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 40 * time.Second, IdleTimeout: 60 * time.Second}
	workers.Go(func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		server.Shutdown(shutdown)
	})
	logger.Info("service_started", "service", "movie", "addr", cfg.HTTPAddr)
	err = server.ListenAndServe()
	cancel()
	workers.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
