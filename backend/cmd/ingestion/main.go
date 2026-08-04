package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"time"
	"strconv"

	"omnistream/config"
	"omnistream/internal/domain"
	"omnistream/internal/infra/kafka"
	"omnistream/internal/infra/logging"
	"omnistream/internal/infra/minio"
	"omnistream/internal/infra/postgres"
	"omnistream/internal/usecase"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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

	storage, err := minio.NewStorage(
		cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey,
		cfg.MinioFilesBucket, cfg.MinioUseSSL,
	)
	if err != nil {
		logger.Error("failed to init minio client", "error", err)
		panic(err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.PostgresDSN())
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		panic(err)
	}
	defer pool.Close()

	fileRepo := postgres.NewFileRepository(pool)
	fileService := usecase.NewFileService(fileRepo, storage, cfg.UploadChunkSizeBytes)

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicFileEvents)
	defer producer.Close()

	if cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     cfg.CORSOrigins,
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "X-Room-Id", "X-File-Name"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// POST /upload — streams the raw request body directly to MinIO, then
	// publishes a small "file uploaded" event to Kafka. Deliberately simple
	// for this first milestone: proves the full pipeline connects
	// end-to-end (client -> ingestion -> MinIO -> Kafka -> worker) before
	// layering on the real chunked-upload protocol, resumability, and
	// Postgres metadata persistence in the next pass.
	r.POST("/upload", func(c *gin.Context) {
		roomID := c.GetHeader("X-Room-Id")
		fileName := c.GetHeader("X-File-Name")
		contentType := c.ContentType()

		if roomID == "" || fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing X-Room-Id or X-File-Name header"})
			return
		}

		fileID := uuid.New().String()
		storageKey := roomID + "/" + fileID + "-" + fileName

		size := c.Request.ContentLength
		if size <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Content-Length is required"})
			return
		}

		ctx := c.Request.Context()

		if err := storage.PutObjectStream(ctx, storageKey, c.Request.Body, size, contentType); err != nil {
			logger.Error("failed to store upload", "error", err, "file_id", fileID)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to store file"})
			return
		}

		// Register the file in Postgres now that bytes are safely in MinIO.
		// "uploaderID" is left blank for this milestone — becomes a real
		// participant ID once room-level auth/identity exists.
		if _, err := fileService.RegisterUpload(ctx, fileID, roomID, "", fileName, size, contentType, storageKey); err != nil {
			logger.Error("failed to register upload in postgres", "error", err, "file_id", fileID)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register file"})
			return
		}

		event := domain.FileUploadedEvent{
			FileID:      fileID,
			RoomID:      roomID,
			StorageKey:  storageKey,
			FileName:    fileName,
			FileSize:    size,
			ContentType: contentType,
			UploadedAt:  time.Now(),
		}

		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := producer.Publish(publishCtx, roomID, event); err != nil {
			// The file is safely stored even if this fails — log loudly,
			// but don't fail the upload response for an event-publish issue.
			logger.Error("failed to publish file-uploaded event", "error", err, "file_id", fileID)
		}

		logger.Info("file uploaded", "file_id", fileID, "room_id", roomID, "size", size)

		c.JSON(http.StatusCreated, gin.H{
			"file_id":     fileID,
			"storage_key": storageKey,
		})
	})
	r.POST("/uploads/initiate", func(c *gin.Context) {
		var req struct {
			RoomID      string `json:"room_id"`
			FileName    string `json:"file_name"`
			FileSize    int64  `json:"file_size"`
			ContentType string `json:"content_type"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}

		result, err := fileService.InitUpload(c.Request.Context(), req.RoomID, "", req.FileName, req.FileSize, req.ContentType)
		if err != nil {
			logger.Error("failed to init upload", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to init upload"})
			return
		}
		c.JSON(http.StatusCreated, result)
	})

	r.PUT("/uploads/:fileId/parts/:partNumber", func(c *gin.Context) {
		fileID := c.Param("fileId")
		partNumber, err := strconv.Atoi(c.Param("partNumber"))
		if err != nil || partNumber < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid part number"})
			return
		}

		size := c.Request.ContentLength
		if size <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Content-Length is required"})
			return
		}

		if err := fileService.RecordPart(c.Request.Context(), fileID, partNumber, c.Request.Body, size); err != nil {
			logger.Error("failed to record part", "file_id", fileID, "part", partNumber, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record part"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"part_number": partNumber, "size": size})
	})

	r.GET("/uploads/:fileId/status", func(c *gin.Context) {
		fileID := c.Param("fileId")
		status, err := fileService.GetUploadStatus(c.Request.Context(), fileID)
		if err != nil {
			logger.Error("failed to get upload status", "file_id", fileID, "error", err)
			c.JSON(http.StatusNotFound, gin.H{"error": "upload not found"})
			return
		}
		c.JSON(http.StatusOK, status)
	})

	r.POST("/uploads/:fileId/complete", func(c *gin.Context) {
		fileID := c.Param("fileId")

		f, err := fileService.CompleteUpload(c.Request.Context(), fileID)
		if err != nil {
			logger.Error("failed to complete upload", "file_id", fileID, "error", err)
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}

		event := domain.FileUploadedEvent{
			FileID:      f.ID,
			RoomID:      f.RoomID,
			StorageKey:  f.StorageKey,
			FileName:    f.FileName,
			FileSize:    f.FileSize,
			ContentType: f.ContentType,
			UploadedAt:  time.Now(),
		}
		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := producer.Publish(publishCtx, f.RoomID, event); err != nil {
			logger.Error("failed to publish file-uploaded event", "error", err, "file_id", f.ID)
		}

		c.JSON(http.StatusOK, gin.H{"file_id": f.ID, "storage_key": f.StorageKey})
	})

	r.DELETE("/uploads/:fileId", func(c *gin.Context) {
		fileID := c.Param("fileId")
		if err := fileService.AbortUpload(c.Request.Context(), fileID); err != nil {
			logger.Error("failed to abort upload", "file_id", fileID, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to abort upload"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true})
	})
	logger.Info("ingestion service listening", "port", cfg.IngestionPort)
	if err := r.Run(":" + cfg.IngestionPort); err != nil {
		logger.Error("ingestion service failed", "error", err)
	}
}
