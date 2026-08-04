package domain

import "time"

type FileStatus string

const (
	FileStatusUploading FileStatus = "uploading"
	FileStatusStored    FileStatus = "stored"    // bytes fully in object storage
	FileStatusScanning  FileStatus = "scanning"   // a worker is processing it
	FileStatusReady     FileStatus = "ready"      // available for delivery
	FileStatusDelivered FileStatus = "delivered"  // live receiver picked it up
	FileStatusExpired   FileStatus = "expired"
	FileStatusFailed    FileStatus = "failed"
)

// File is the metadata record for an uploaded file. The actual bytes never
// live here — they live in object storage (MinIO/S3) at StorageKey. This
// separation is deliberate: Postgres stays fast for queries over potentially
// millions of small metadata rows, while bulk binary data lives in storage
// built for exactly that.
type File struct {
	ID          string
	RoomID      string
	UploaderID  string
	FileName    string
	FileSize    int64
	ContentType string
	StorageKey  string // path/key within the object storage bucket
	Status      FileStatus
	CreatedAt   time.Time
	ExpiresAt   time.Time
	DeliveredAt *time.Time
}

func NewFile(id, roomID, uploaderID, fileName string, fileSize int64, contentType, storageKey string, ttl time.Duration) *File {
	now := time.Now()
	return &File{
		ID:          id,
		RoomID:      roomID,
		UploaderID:  uploaderID,
		FileName:    fileName,
		FileSize:    fileSize,
		ContentType: contentType,
		StorageKey:  storageKey,
		Status:      FileStatusUploading,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
	}
}

func (f *File) IsExpired() bool {
	return time.Now().After(f.ExpiresAt)
}

func (f *File) IsAwaitingDelivery() bool {
	return f.Status == FileStatusReady && f.DeliveredAt == nil
}
func (f *File) IsUsable() bool {
	return f.Status == FileStatusReady && !f.IsExpired()
}

func (f *File) MarkDelivered() {
	now := time.Now()
	f.DeliveredAt = &now
	f.Status = FileStatusDelivered
}
