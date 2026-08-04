package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"omnistream/config"
	"omnistream/internal/domain"
	"omnistream/internal/infra/kafka"
	"omnistream/internal/infra/logging"
	"omnistream/internal/infra/minio"
	"omnistream/internal/infra/postgres"
	"omnistream/internal/usecase"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type RecordingFinishedEvent struct {
	RoomID      string `json:"room_id"`
	StorageKey  string `json:"storage_key"`
	MeetingType string `json:"meeting_type"`
}

type WhisperResponse struct {
	Text string `json:"text"`
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	System string `json:"system"`
	Format any    `json:"format"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

type ActionItem struct {
	Task     string `json:"task"`
	Assignee string `json:"assignee"`
	Deadline string `json:"deadline"`
}

type CorporateNotes struct {
	Summary     string       `json:"summary"`
	ActionItems []ActionItem `json:"action_items"`
	Blockers    []string     `json:"blockers"`
	Decisions   []string     `json:"decisions"`
}

type Concept struct {
	Term       string `json:"term"`
	Definition string `json:"definition"`
}

type LectureNotes struct {
	Topic       string    `json:"topic"`
	Summary     string    `json:"summary"`
	KeyConcepts []Concept `json:"key_concepts"`
	Assignments []string  `json:"assignments"`
}

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

	filesStorage, err := minio.NewStorage(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucketFiles, cfg.MinioUseSSL)
	if err != nil {
		logger.Error("failed to connect to minio files bucket", "error", err)
		panic(err)
	}

	recordingsStorage, err := minio.NewStorage(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucketRecordings, cfg.MinioUseSSL)
	if err != nil {
		logger.Error("failed to connect to minio recordings bucket", "error", err)
		panic(err)
	}

	fileService := usecase.NewFileService(fileRepo, filesStorage, 0)

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, "recording-ready-events", "omnistream-ai-worker", logger)
	defer consumer.Close()

	readyProducer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicFileReadyEvents)
	defer readyProducer.Close()

	whisperURL := getEnvOrDefault("WHISPER_URL", "http://whisper:9000")
	ollamaURL := getEnvOrDefault("OLLAMA_URL", "http://ollama:11434")

	logger.Info("ai-worker started, consuming topic: recording.events")

	consumer.Consume(ctx, func(ctx context.Context, key, value []byte) error {
		var event RecordingFinishedEvent
		if err := json.Unmarshal(value, &event); err != nil {
			logger.Error("failed to unmarshal recording event", "error", err)
			return err
		}

		logger.Info("processing recording event", "room_id", event.RoomID, "storage_key", event.StorageKey)

		audioStream, err := recordingsStorage.GetObjectStream(ctx, event.StorageKey)
		if err != nil {
			logger.Error("failed to get audio stream from minio", "error", err)
			return err
		}
		defer audioStream.Close()

		audioData, err := io.ReadAll(audioStream)
		if err != nil {
			logger.Error("failed to read audio bytes", "error", err)
			return err
		}

		transcript, err := transcribeAudio(whisperURL, audioData)
		if err != nil {
			logger.Error("whisper transcription failed", "error", err)
			return err
		}

		if event.MeetingType == "" {
			event.MeetingType = "corporate"
		}

		rawJSON, err := generateNotes(ollamaURL, transcript, event.MeetingType)
		if err != nil {
			logger.Error("ollama notes generation failed", "error", err)
			return err
		}

		markdownContent, err := formatMarkdown(rawJSON, event.MeetingType)
		if err != nil {
			logger.Error("markdown formatting failed", "error", err)
			return err
		}

		fileID := uuid.New().String()
		fileName := "AI_Meeting_Notes.md"
		if event.MeetingType == "lecture" {
			fileName = "AI_Lecture_Notes.md"
		}
		storageKey := event.RoomID + "/" + fileID + "-" + fileName
		fileSize := int64(len(markdownContent))

		if err := filesStorage.PutObjectStream(ctx, storageKey, strings.NewReader(markdownContent), fileSize, "text/markdown"); err != nil {
			logger.Error("failed to store markdown file in minio", "error", err)
			return err
		}

		file, err := fileService.RegisterUpload(ctx, fileID, event.RoomID, "system-ai", fileName, fileSize, "text/markdown", storageKey)
		if err != nil {
			logger.Error("failed to register ai file in database", "error", err)
			return err
		}

		if err := fileService.MarkReady(ctx, file.ID); err != nil {
			logger.Error("failed to mark ai file ready", "error", err)
			return err
		}

		readyEvent := domain.FileReadyEvent{
			FileID:      file.ID,
			RoomID:      event.RoomID,
			FileName:    fileName,
			FileSize:    fileSize,
			ContentType: "text/markdown",
			ReadyAt:     time.Now(),
		}

		if err := readyProducer.Publish(ctx, event.RoomID, readyEvent); err != nil {
			logger.Error("failed to publish file ready event for ai notes", "error", err)
		}

		logger.Info("successfully processed recording and generated markdown notes", "file_id", file.ID, "room_id", event.RoomID)
		return nil
	})

	logger.Info("ai-worker shutting down")
	_ = os.Stdout.Sync()
}

func transcribeAudio(whisperURL string, audioData []byte) (string, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("audio_file", "recording.wav")
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(part, bytes.NewReader(audioData)); err != nil {
		return "", err
	}
	writer.Close()

	req, err := http.NewRequest("POST", whisperURL+"/asr?encode=true&task=transcribe&output=json", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("whisper returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result WhisperResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Text, nil
}

func generateNotes(ollamaURL, transcript, meetingType string) (string, error) {
	var systemPrompt string
	var jsonSchema map[string]any

	if meetingType == "lecture" {
		systemPrompt = "You are an expert academic teaching assistant. Analyze the lecture transcript. Summarize the core topic, define technical concepts, and list assignments."
		jsonSchema = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"topic":   map[string]any{"type": "string"},
				"summary": map[string]any{"type": "string"},
				"key_concepts": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"term":       map[string]any{"type": "string"},
							"definition": map[string]any{"type": "string"},
						},
						"required": []string{"term", "definition"},
					},
				},
				"assignments": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
			"required": []string{"topic", "summary", "key_concepts", "assignments"},
		}
	} else {
		systemPrompt = "You are a senior engineering project manager. Analyze the meeting transcript. Extract the core summary, assign action items with deadline and assignee, flag blockers, and record decisions."
		jsonSchema = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"summary": map[string]any{"type": "string"},
				"action_items": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"task":     map[string]any{"type": "string"},
							"assignee": map[string]any{"type": "string"},
							"deadline": map[string]any{"type": "string"},
						},
						"required": []string{"task", "assignee", "deadline"},
					},
				},
				"blockers":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"decisions": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required": []string{"summary", "action_items", "blockers", "decisions"},
		}
	}

	reqPayload := OllamaRequest{
		Model:  "llama3",
		Prompt: transcript,
		System: systemPrompt,
		Format: jsonSchema,
		Stream: false,
	}

	data, err := json.Marshal(reqPayload)
	if err != nil {
		return "", err
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Post(ollamaURL+"/api/generate", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result OllamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Response, nil
}

func formatMarkdown(rawJSON, meetingType string) (string, error) {
	var sb strings.Builder

	if meetingType == "lecture" {
		var notes LectureNotes
		if err := json.Unmarshal([]byte(rawJSON), &notes); err != nil {
			return "", err
		}

		sb.WriteString("# Lecture Notes: " + notes.Topic + "\n\n")
		sb.WriteString("## Executive Summary\n" + notes.Summary + "\n\n")
		sb.WriteString("## Key Concepts\n")
		for _, concept := range notes.KeyConcepts {
			sb.WriteString(fmt.Sprintf("- **%s**: %s\n", concept.Term, concept.Definition))
		}
		sb.WriteString("\n## Assignments & Coursework\n")
		for _, assignment := range notes.Assignments {
			sb.WriteString(fmt.Sprintf("- %s\n", assignment))
		}
	} else {
		var notes CorporateNotes
		if err := json.Unmarshal([]byte(rawJSON), &notes); err != nil {
			return "", err
		}

		sb.WriteString("# Meeting Summary & Action Items\n\n")
		sb.WriteString("## Summary\n" + notes.Summary + "\n\n")
		sb.WriteString("## Action Items\n")
		for _, item := range notes.ActionItems {
			sb.WriteString(fmt.Sprintf("- [ ] **%s** (@%s) - Due: %s\n", item.Task, item.Assignee, item.Deadline))
		}
		sb.WriteString("\n## Operational Blockers\n")
		for _, blocker := range notes.Blockers {
			sb.WriteString(fmt.Sprintf("- ⚠️ %s\n", blocker))
		}
		sb.WriteString("\n## Key Decisions\n")
		for _, decision := range notes.Decisions {
			sb.WriteString(fmt.Sprintf("- ✅ %s\n", decision))
		}
	}

	return sb.String(), nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}
