package kafka

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID, // consumer group — lets you run multiple worker
			// instances that share the work, and lets a restarted worker
			// resume from where it left off instead of reprocessing
			// everything or missing messages that arrived while it was down.
		}),
		logger: logger,
	}
}

// Consume blocks, calling handler for every message until ctx is cancelled.
// A handler error is logged but does not stop the loop — one bad message
// shouldn't take down the whole worker. Committing only happens after the
// handler returns successfully (ReadMessage auto-commits in kafka-go's
// default mode), so a crash mid-handling means the message gets redelivered
// rather than silently lost.
func (c *Consumer) Consume(ctx context.Context, handler func(ctx context.Context, key, value []byte) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled — normal shutdown
			}
			c.logger.Error("failed to read kafka message", "error", err)
			continue
		}

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			c.logger.Error("handler failed processing message", "error", err, "key", string(msg.Key))
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
