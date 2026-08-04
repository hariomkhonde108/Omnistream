package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"omnistream/config"
	"omnistream/internal/infra/logging"
	"omnistream/internal/infra/minio"
	"omnistream/internal/infra/postgres"
	"omnistream/internal/usecase"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"encoding/json"

	"omnistream/internal/adapter/ws"
	"omnistream/internal/domain"
	"omnistream/internal/infra/kafka"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/webhook"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type createRoomRequest struct {
	Password string `json:"password"`
}

type verifyRoomRequest struct {
	Password string `json:"password"`
}

type joinRoomRequest struct {
	ParticipantID string `json:"participant_id"`
	Name          string `json:"name"`
}

type markDeliveredRequest struct {
	ParticipantID string `json:"participant_id"`
}
type parsedRange struct {
	start, end int64
}

func parseRangeHeader(header string, fileSize int64) (parsedRange, bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return parsedRange{}, false
	}

	spec := strings.TrimPrefix(header, prefix)
	if strings.Contains(spec, ",") {
		return parsedRange{}, false // multi-range not supported
	}

	// Suffix range: "bytes=-N" means "give me the last N bytes" — the
	// start position isn't known by the client at all, only computed here
	// once we know the real file size.
	if strings.HasPrefix(spec, "-") {
		suffixLen, err := strconv.ParseInt(spec[1:], 10, 64)
		if err != nil || suffixLen <= 0 {
			return parsedRange{}, false
		}
		start := fileSize - suffixLen
		if start < 0 {
			start = 0 // asked for more than the file has — just return the whole thing
		}
		return parsedRange{start: start, end: fileSize - 1}, true
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return parsedRange{}, false
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= fileSize {
		return parsedRange{}, false
	}

	if parts[1] == "" {
		return parsedRange{start: start, end: -1}, true // open-ended: "bytes=1000-"
	}

	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return parsedRange{}, false
	}
	if end >= fileSize {
		end = fileSize - 1
	}

	return parsedRange{start: start, end: end}, true
}

// api owns room lifecycle (create/verify/join) and the async-dropbox
// "what's waiting for me" check. It also owns running migrations on
// startup — of the three services, this is the natural one to be first-up
// in a typical deploy, and CREATE TABLE IF NOT EXISTS is safe even if
// ingestion/worker happen to start first in some environments.
func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	cfg, err := config.Load()
	logger := logging.New(cfg.LogLevel, cfg.IsProduction())
	logger.Info("DEBUG livekit config", "api_key", cfg.LiveKitAPIKey, "secret_length", len(cfg.LiveKitAPISecret), "secret_bytes", fmt.Sprintf("%q", cfg.LiveKitAPISecret))
	if err != nil {
		panic("config error: " + err.Error())
	}

	slog.SetDefault(logger)

	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, cfg.PostgresDSN())
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		panic(err)
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool); err != nil {
		logger.Error("failed to run migrations", "error", err)
		panic(err)
	}
	logger.Info("migrations applied")

	roomRepo := postgres.NewRoomRepository(pool)
	roomService := usecase.NewRoomService(roomRepo)
	videoEventRepo := postgres.NewVideoEventRepository(pool)
	//videoService := usecase.NewVideoService(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, videoEventRepo)
	recordingRepo := postgres.NewRecordingRepository(pool)
recordingProducer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicRecordingReadyEvents)
defer recordingProducer.Close()

videoService := usecase.NewVideoService(
	cfg.LiveKitAPIKey, cfg.LiveKitAPISecret,
	videoEventRepo, recordingRepo,
	cfg.LiveKitURL,
	usecase.S3RecordingConfig{
		AccessKey: cfg.MinioAccessKey,
		Secret:    cfg.MinioSecretKey,
		Endpoint:  cfg.MinioInternalEndpoint,
		Bucket:    cfg.MinioRecordingsBucket,
	},
	recordingProducer,
	cfg.KafkaTopicRecordingReadyEvents,
)
	// api needs read access to object storage now too — the download
	// endpoint streams bytes straight from MinIO to the client. Ingestion
	// still owns writes; this is purely a read path.
	storage, err := minio.NewStorage(
		cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey,
		cfg.MinioFilesBucket, cfg.MinioUseSSL,
	)
	if err != nil {
		logger.Error("failed to init minio client", "error", err)
		panic(err)
	}
	fileRepo := postgres.NewFileRepository(pool)
	fileService := usecase.NewFileService(fileRepo, storage, cfg.UploadChunkSizeBytes)

	participantRepo := postgres.NewParticipantRepository(pool)
	participantService := usecase.NewParticipantService(participantRepo)

	hub := ws.NewHub(logger)
	wsHandler := ws.NewHandler(hub, cfg.CORSOrigins, logger)

	// Consumes the events the worker publishes when it marks a file ready
	// (a DIFFERENT process from this one) and fans them out to whichever
	// browsers are currently connected to that room. Note: this only works
	// correctly with a single `api` instance — if this service is ever
	// horizontally scaled, this needs the same Redis Pub/Sub fan-out
	// pattern used in the original P2P project's Hub, since a client
	// connected to instance A won't hear about an event consumed by
	// instance B otherwise.
	readyConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopicFileReadyEvents, "dropvault-api-ws", logger)
	go readyConsumer.Consume(ctx, func(ctx context.Context, key, value []byte) error {
		var event domain.FileReadyEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return err
		}
		hub.Broadcast(event.RoomID, ws.Event{
			Type:    "file_ready",
			RoomID:  event.RoomID,
			Payload: event,
		})
		return nil
	})

	if cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     cfg.CORSOrigins,
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept"},
		AllowCredentials: true,
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/ws/rooms/:id", func(c *gin.Context) {
		wsHandler.ServeWS(c.Writer, c.Request, c.Param("id"))
	})
	rooms := r.Group("/api/rooms")
	{	

		rooms.POST("/:id/recording/start", func(c *gin.Context) {
			roomID := c.Param("id")
			rec, err := videoService.StartRecording(c.Request.Context(), roomID)
			if err != nil {
				logger.Error("failed to start recording", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start recording"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"egress_id": rec.EgressID, "storage_key": rec.StorageKey})
		})
		rooms.POST("/:id/recording/stop", func(c *gin.Context) {
			roomID := c.Param("id")

			// Initialize the LiveKit Egress client using your loaded config
			egressClient := lksdk.NewEgressClient(
				cfg.LiveKitURL,
				cfg.LiveKitAPIKey,
				cfg.LiveKitAPISecret,
			)

			// List all active egresses for this specific room
			req := &livekit.ListEgressRequest{RoomName: roomID}
			res, err := egressClient.ListEgress(c.Request.Context(), req)
			if err != nil {
				logger.Error("failed to list egresses", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list egresses"})
				return
			}

			// Loop through and stop any that are currently running
			for _, egress := range res.Items {
				if egress.Status == livekit.EgressStatus_EGRESS_STARTING || egress.Status == livekit.EgressStatus_EGRESS_ACTIVE {
					_, err = egressClient.StopEgress(c.Request.Context(), &livekit.StopEgressRequest{
						EgressId: egress.EgressId,
					})
					if err != nil {
						logger.Error("failed to stop egress", "egress_id", egress.EgressId, "error", err)
					}
				}
			}

			c.JSON(http.StatusOK, gin.H{"message": "Recording stopped"})
		})
		rooms.POST("", func(c *gin.Context) {
			var req createRoomRequest
			_ = c.ShouldBindJSON(&req) // empty body just means "no password"

			result, err := roomService.CreateRoom(c.Request.Context(), req.Password)
			if err != nil {
				logger.Error("failed to create room", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create room"})
				return
			}

			c.JSON(http.StatusCreated, gin.H{
				"room_id":    result.Room.ID,
				"expires_at": result.Room.ExpiresAt,
				"is_secured": result.IsSecured,
			})
		})

		rooms.POST("/:id/verify", func(c *gin.Context) {
			roomID := c.Param("id")
			var req verifyRoomRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
				return
			}

			err := roomService.VerifyRoom(c.Request.Context(), roomID, req.Password)
			switch {
			case err == nil:
				c.JSON(http.StatusOK, gin.H{"success": true})
			case errors.Is(err, usecase.ErrRoomNotFound):
				c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
			case errors.Is(err, usecase.ErrRoomExpired):
				c.JSON(http.StatusGone, gin.H{"error": "room expired"})
			case errors.Is(err, usecase.ErrInvalidPassword):
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid password"})
			default:
				logger.Error("failed to verify room", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			}
		})

		// POST /api/rooms/:id/join — registers a participant in the room.
		// participant_id is generated CLIENT-SIDE (crypto.randomUUID(), same
		// pattern as the original P2P project's peerId) and sent here, not
		// generated server-side — so a reconnecting client can rejoin with
		// the same identity and keep their delivery history, instead of
		// looking like a brand new person every time they refresh the page.
		rooms.POST("/:id/join", func(c *gin.Context) {
			roomID := c.Param("id")
			var req joinRoomRequest
			if err := c.ShouldBindJSON(&req); err != nil || req.ParticipantID == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "participant_id is required"})
				return
			}

			p, err := participantService.Join(c.Request.Context(), req.ParticipantID, roomID)
			if err != nil {
				logger.Error("failed to join room", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to join room"})
				return
			}

			c.JSON(http.StatusOK, gin.H{"participant_id": p.ID, "room_id": p.RoomID})
		})

		// GET /api/rooms/:id/files?participant_id=... — the async dropbox
		// check, scoped to ONE participant. Every participant in a
		// multi-peer room calls this with their OWN id and gets their own
		// independent answer — someone else already downloading a file has
		// no effect on what shows up here.
		rooms.GET("/:id/files", func(c *gin.Context) {
			roomID := c.Param("id")
			participantID := c.Query("participant_id")
			if participantID == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "participant_id query param is required"})
				return
			}

			files, err := fileService.ListWaitingFilesForParticipant(c.Request.Context(), roomID, participantID)
			if err != nil {
				logger.Error("failed to list waiting files", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list files"})
				return
			}

			c.JSON(http.StatusOK, gin.H{"files": files})
		})

		// POST /api/rooms/:id/video-token — mints a short-lived LiveKit
		// join token for one participant. The participant must already be
		// a registered participant of this room (join happens first) —
		// this endpoint doesn't re-verify the room password itself, since
		// that already happened at /verify; it trusts that a client
		// reaching this point has already been through that gate.
		rooms.POST("/:id/video-token", func(c *gin.Context) {
			roomID := c.Param("id")
			var req joinRoomRequest // reuses the same {participant_id} shape as /join
			if err := c.ShouldBindJSON(&req); err != nil || req.ParticipantID == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "participant_id is required"})
				return
			}

			token, err := videoService.GenerateJoinToken(roomID, req.ParticipantID, req.Name)
			if err != nil {
				logger.Error("failed to generate video token", "room_id", roomID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate video token"})
				return
			}

			c.JSON(http.StatusOK, gin.H{"token": token, "livekit_url": cfg.LiveKitURL})
		})
		// POST /api/rooms/:id/files/:fileId/delivered — a participant calls
		// this once they've actually downloaded the file's bytes. This
		// affects ONLY that participant's record — the file remains fully
		// visible and downloadable to everyone else in the room.
		//
		// In practice this is now called automatically by the download
		// handler below on a verified, complete transfer — kept as its own
		// endpoint too, in case a client needs to mark something delivered
		// without re-downloading (e.g. "I already have this file locally").
		rooms.POST("/:id/files/:fileId/delivered", func(c *gin.Context) {
			fileID := c.Param("fileId")
			var req markDeliveredRequest
			if err := c.ShouldBindJSON(&req); err != nil || req.ParticipantID == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "participant_id is required"})
				return
			}

			if err := fileService.MarkDeliveredToParticipant(c.Request.Context(), fileID, req.ParticipantID); err != nil {
				logger.Error("failed to mark file delivered", "file_id", fileID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to mark delivered"})
				return
			}

			c.JSON(http.StatusOK, gin.H{"success": true})
		})

		// GET /api/rooms/:id/files/:fileId/download?participant_id=... —
		// the endpoint that was actually missing: streams the real file
		// bytes from MinIO to the client, and — only once the FULL transfer
		// is confirmed complete — marks it delivered to that participant.
		//
		// Marking delivery is deliberately NOT optimistic (i.e. not done
		// before streaming starts): if the connection drops mid-download,
		// the file should still show up as "waiting" on that participant's
		// next check, so they can simply retry rather than having silently
		// lost access to something they never actually finished receiving.
		rooms.GET("/:id/files/:fileId/download", func(c *gin.Context) {
			roomID := c.Param("id")
			fileID := c.Param("fileId")
			participantID := c.Query("participant_id")

			if participantID == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "participant_id query param is required"})
				return
			}

			ctx := c.Request.Context()

			f, err := fileService.GetForDownload(ctx, fileID, roomID)
			switch {
			case err == nil:
				// continue below
			case errors.Is(err, usecase.ErrFileNotFound):
				c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
				return
			case errors.Is(err, usecase.ErrFileWrongRoom):
				c.JSON(http.StatusForbidden, gin.H{"error": "file does not belong to this room"})
				return
			case errors.Is(err, usecase.ErrFileNotReady):
				c.JSON(http.StatusConflict, gin.H{"error": "file is not ready for download yet"})
				return
			default:
				logger.Error("failed to look up file for download", "file_id", fileID, "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
				return
			}

			c.Header("Accept-Ranges", "bytes")
			c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, f.FileName))
			c.Header("Content-Type", f.ContentType)

			rangeHeader := c.GetHeader("Range")
			rng, hasRange := parseRangeHeader(rangeHeader, f.FileSize)

			var reader io.ReadCloser
			var start, end int64
			var status int

			if hasRange {
				//logger.Info("RANGE REQUEST DETECTED", "raw_header", rangeHeader, "parsed_start", rng.start, "parsed_end", rng.end)
				start = rng.start
				if rng.end < 0 {
					end = f.FileSize - 1
				} else {
					end = rng.end
				}
				status = http.StatusPartialContent

				reader, err = storage.GetObjectStreamRange(ctx, f.StorageKey, start, end)
				if err != nil {
					logger.Error("failed to open ranged object stream", "file_id", fileID, "error", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read file"})
					return
				}

				c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, f.FileSize))
			} else {
				start = 0
				end = f.FileSize - 1
				status = http.StatusOK

				reader, err = storage.GetObjectStream(ctx, f.StorageKey)
				if err != nil {
					logger.Error("failed to open object stream", "file_id", fileID, "error", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read file"})
					return
				}
			}
			defer reader.Close()

			expectedBytes := end - start + 1
			c.Header("Content-Length", strconv.FormatInt(expectedBytes, 10))
			c.Status(status)

			written, copyErr := io.Copy(c.Writer, reader)
			if copyErr != nil {
				logger.Warn("download stream interrupted", "file_id", fileID, "participant_id", participantID, "bytes_written", written, "error", copyErr)
				return
			}
			if written != expectedBytes {
				logger.Warn("download byte count mismatch for this range, not marking delivered",
					"file_id", fileID, "expected", expectedBytes, "written", written)
				return
			}

			if end == f.FileSize-1 {
				if err := fileService.MarkDeliveredToParticipant(ctx, fileID, participantID); err != nil {
					logger.Error("download succeeded but failed to record delivery", "file_id", fileID, "participant_id", participantID, "error", err)
				}
			}
		})
	}
	// POST /webhooks/livekit — LiveKit calls this itself whenever something
	// happens in any room (participant joined/left, room started/ended,
	// etc.). The signature verification here is what stops anyone else
	// from forging fake video events — only requests signed with the same
	// api_key/secret pair configured in livekit.yaml are accepted.
	r.POST("/webhooks/livekit", func(c *gin.Context) {
		keyProvider := auth.NewSimpleKeyProvider(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret)

		event, err := webhook.ReceiveWebhookEvent(c.Request, keyProvider)
		if err != nil {
			logger.Warn("failed to verify livekit webhook", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid webhook signature"})
			return
		}

		if err := videoService.HandleWebhookEvent(c.Request.Context(), event); err != nil {
			logger.Error("failed to handle livekit webhook event", "error", err, "event_type", event.Event)
			// Still return 200 — LiveKit will retry on non-2xx, and a
			// logging/persistence failure here shouldn't cause LiveKit to
			// keep hammering this endpoint with retries.
		}

		c.Status(http.StatusOK)
	})

	logger.Info("api service listening", "port", cfg.APIPort)
	if err := r.Run(":" + cfg.APIPort); err != nil {
		logger.Error("api service failed", "error", err)
	}
}
