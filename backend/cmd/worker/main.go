package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"omnistream/config"
	"omnistream/internal/domain"
	"omnistream/internal/infra/kafka"
	"omnistream/internal/infra/logging"
	"omnistream/internal/infra/postgres"
	"omnistream/internal/usecase"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	cfg, err := config.Load()
	if err != nil {
		panic("config error: " + err.Error())
	}

	logger := logging.New(cfg.LogLevel, cfg.IsProduction())
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.PostgresDSN())
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		panic(err)
	}
	defer pool.Close()

	fileRepo := postgres.NewFileRepository(pool)
	fileService := usecase.NewFileService(fileRepo, nil, 0)

	// groupID matters: multiple worker instances sharing this same group ID
	// will split the work between them automatically (Kafka's consumer
	// group rebalancing), rather than each one reprocessing every message.
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopicFileEvents, "dropvault-worker", logger)
	defer consumer.Close()
	readyProducer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicFileReadyEvents)
	defer readyProducer.Close()
	logger.Info("worker started, consuming", "topic", cfg.KafkaTopicFileEvents)

	consumer.Consume(ctx, func(ctx context.Context, key, value []byte) error {
		var event domain.FileUploadedEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return err
		}

		logger.Info("processing uploaded file",
			"file_id", event.FileID,
			"room_id", event.RoomID,
			"file_name", event.FileName,
			"size", event.FileSize,
			"storage_key", event.StorageKey,
		)

		if err := fileService.MarkReady(ctx, event.FileID); err != nil {
			logger.Error("failed to mark file ready", "error", err, "file_id", event.FileID)
			return err
		}

		readyEvent := domain.FileReadyEvent{
			FileID:      event.FileID,
			RoomID:      event.RoomID,
			FileName:    event.FileName,
			FileSize:    event.FileSize,
			ContentType: event.ContentType,
			ReadyAt:     time.Now(),
		}
		if err := readyProducer.Publish(ctx, event.RoomID, readyEvent); err != nil {
			// The file IS ready — this only affects the live-push
			// notification, not correctness. A connected client just
			// won't get instantly notified; they'll still see it on their
			// next GET .../files poll.
			logger.Error("failed to publish file-ready event", "error", err, "file_id", event.FileID)
		}

		return nil
	})

	logger.Info("worker shutting down")
	_ = os.Stdout.Sync()
}
