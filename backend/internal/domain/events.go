package domain

import "time"

// FileUploadedEvent is published to Kafka the moment a file finishes landing
// in object storage. This is intentionally tiny — just enough for a worker
// to go fetch the object itself from MinIO using StorageKey. Kafka carries
// pointers to data, never the data itself; see the ingestion service for
// where the actual bytes get written to MinIO before this event fires.
type FileUploadedEvent struct {
	FileID      string    `json:"file_id"`
	RoomID      string    `json:"room_id"`
	StorageKey  string    `json:"storage_key"`
	FileName    string    `json:"file_name"`
	FileSize    int64     `json:"file_size"`
	ContentType string    `json:"content_type"`
	UploadedAt  time.Time `json:"uploaded_at"`
	
}
type FileReadyEvent struct {
	FileID      string    `json:"file_id"`
	RoomID      string    `json:"room_id"`
	FileName    string    `json:"file_name"`
	FileSize    int64     `json:"file_size"`
	ContentType string    `json:"content_type"`
	ReadyAt     time.Time `json:"ready_at"`
}