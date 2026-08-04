package domain

import "time"

type RecordingStatus string

const (
	RecordingStatusStarting RecordingStatus = "starting"
	RecordingStatusComplete RecordingStatus = "complete"
	RecordingStatusFailed   RecordingStatus = "failed"
)

// Recording tracks one LiveKit Egress session. StorageKey is decided by
// US, before the recording even starts — not parsed out of LiveKit's
// webhook response — specifically so we never depend on the exact shape
// of LiveKit's FileInfo.Location/Filename fields, which weren't confirmed
// precisely enough to trust blindly.
type Recording struct {
	EgressID        string
	RoomID          string
	StorageKey      string
	Status          RecordingStatus
	DurationSeconds int64
	SizeBytes       int64
	CreatedAt       time.Time
	CompletedAt     *time.Time
}

func NewRecording(egressID, roomID, storageKey string) *Recording {
	return &Recording{
		EgressID:   egressID,
		RoomID:     roomID,
		StorageKey: storageKey,
		Status:     RecordingStatusStarting,
		CreatedAt:  time.Now(),
	}
}