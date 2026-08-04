package domain

// RecordingReadyEvent is published to Kafka once a recording's egress_ended
// webhook confirms it completed successfully. Same "small pointer, never
// the actual bytes" pattern as FileUploadedEvent — the notes-worker fetches
// the real audio from MinIO using StorageKey.
type RecordingReadyEvent struct {
	EgressID   string `json:"egress_id"`
	RoomID     string `json:"room_id"`
	StorageKey string `json:"storage_key"`
}