package config

import (
	"fmt"
	"os"
	"strings"
	"strconv"
)

type Config struct {
	Environment    string
	APIPort        string
	IngestionPort  string

	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string
	PostgresSSLMode  string

	UploadChunkSizeBytes int64

	MinioEndpoint        string
	MinioAccessKey       string
	MinioSecretKey       string
	MinioUseSSL          bool
	MinioFilesBucket     string
	MinioRecordingsBucket string
	MinioInternalEndpoint string
	MinioBucketFiles      string
	MinioBucketRecordings string

	WhisperURL            string
	OllamaURL             string
	
	KafkaBrokers        []string
	KafkaTopicFileEvents string
	KafkaTopicFileReadyEvents string
	KafkaTopicRecordingReadyEvents string

	CORSOrigins []string
	LogLevel    string

	LiveKitURL       string
	LiveKitAPIKey    string
	LiveKitAPISecret string

	

}

func Load() (*Config, error) {
	cfg := &Config{
		Environment:   getEnv("APP_ENV", "development"),
		APIPort:       getEnv("API_PORT", "8080"),
		IngestionPort: getEnv("INGESTION_PORT", "8081"),

		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnv("POSTGRES_PORT", "5432"),
		PostgresUser:     getEnv("POSTGRES_USER", "dropvault"),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", ""),
		PostgresDB:       getEnv("POSTGRES_DB", "dropvault"),
		PostgresSSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),

		MinioEndpoint:         getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey:        getEnv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:        getEnv("MINIO_SECRET_KEY", ""),
		MinioUseSSL:           getEnv("MINIO_USE_SSL", "false") == "true",
		MinioFilesBucket:      getEnv("MINIO_FILES_BUCKET", "dropvault-files"),
		MinioRecordingsBucket: getEnv("MINIO_RECORDINGS_BUCKET", "dropvault-recordings"),
		MinioInternalEndpoint:          getEnv("MINIO_INTERNAL_ENDPOINT", "http://minio:9000"),	
		MinioBucketFiles:      getEnv("MINIO_FILES_BUCKET", "dropvault-files"),
		MinioBucketRecordings: getEnv("MINIO_RECORDINGS_BUCKET", "dropvault-recordings"),
		WhisperURL:            getEnv("WHISPER_URL", "http://localhost:9091"),
		OllamaURL:             getEnv("OLLAMA_URL", "http://localhost:11434"),

		KafkaBrokers:         splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopicFileEvents: getEnv("KAFKA_TOPIC_FILE_EVENTS", "file-events"),
		KafkaTopicFileReadyEvents: getEnv("KAFKA_TOPIC_FILE_READY_EVENTS", "file-ready-events"),
		KafkaTopicRecordingReadyEvents: getEnv("KAFKA_TOPIC_RECORDING_READY_EVENTS", "recording-ready-events"),
	
		CORSOrigins: splitCSV(getEnv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		LiveKitURL:       getEnv("LIVEKIT_URL", ""),
		LiveKitAPIKey:    getEnv("LIVEKIT_API_KEY", ""),
		LiveKitAPISecret: getEnv("LIVEKIT_API_SECRET", ""),
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	chunkSize, err := strconv.ParseInt(getEnv("UPLOAD_CHUNK_SIZE_BYTES", "5242880"), 10, 64) // 5MB default
	if err != nil {
		return nil, fmt.Errorf("invalid UPLOAD_CHUNK_SIZE_BYTES: %w", err)
	}
	cfg.UploadChunkSizeBytes = chunkSize	
	return cfg, nil
}

func (c *Config) validate() error {
	if c.PostgresPassword == "" && c.Environment == "production" {
		return fmt.Errorf("POSTGRES_PASSWORD must be set in production")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	return nil
}

func (c *Config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.PostgresUser, c.PostgresPassword, c.PostgresHost, c.PostgresPort, c.PostgresDB, c.PostgresSSLMode,
	)
}

func (c *Config) IsProduction() bool {
	return c.Environment == "production"
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
