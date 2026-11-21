package movie

import (
	"context"
	"log/slog"
	"time"

	"cinema/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client  *kgo.Client
	encoder *event.Encoder
	topic   string
	logger  *slog.Logger
}

func NewProducer(ctx context.Context, cfg Config, logger *slog.Logger) (*Producer, error) {
	encoder, err := event.NewEncoder(ctx, cfg.RegistryURL, cfg.Topic+"-value")
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...), kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.RecordDeliveryTimeout(30*time.Second),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			delay := 200 * time.Millisecond * time.Duration(1<<min(attempt, 5))
			logger.Warn("kafka_retry", "attempt", attempt+1, "backoff", delay.String())
			return delay
		}),
	)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, err
	}
	logger.Info("schema_registered", "subject", cfg.Topic+"-value", "version", encoder.Version)
	return &Producer{client: client, encoder: encoder, topic: cfg.Topic, logger: logger}, nil
}

func (p *Producer) Publish(ctx context.Context, value event.Event) error {
	payload, err := p.encoder.Encode(value)
	if err != nil {
		return err
	}
	err = p.client.ProduceSync(ctx, &kgo.Record{Topic: p.topic, Key: []byte(value.UserID), Value: payload}).FirstErr()
	if err != nil {
		p.logger.Error("publication_failed", "event_id", value.EventID, "error", err)
		return err
	}
	p.logger.Info("event_published", "event_id", value.EventID, "event_type", value.EventType, "timestamp", value.Timestamp.UTC())
	return nil
}

func (p *Producer) Close() { p.client.Close() }
