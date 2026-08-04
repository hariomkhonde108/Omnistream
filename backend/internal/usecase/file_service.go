package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"omnistream/internal/domain"
	"github.com/google/uuid"
)

// FileRepository is the persistence port — satisfied by
// internal/infra/postgres.FileRepository.
type FileRepository interface {
	Save(ctx context.Context, f *domain.File) error
	Get(ctx context.Context, id string) (*domain.File, error)
	UpdateStatus(ctx context.Context, id string, status domain.FileStatus) error
	MarkDeliveredTo(ctx context.Context, fileID, participantID string) error
	ListWaitingForParticipant(ctx context.Context, roomID, participantID string) ([]*domain.File, error)
	SaveUploadPart(ctx context.Context, p *domain.UploadedPart) error
	ListUploadParts(ctx context.Context, fileID string) ([]*domain.UploadedPart, error)
	DeleteUploadParts(ctx context.Context, fileID string) error
	Delete(ctx context.Context, fileID string) error
}

// ObjectStorage is the persistence port for object storage — satisfied by
// internal/infra/minio.Storage. Kept minimal: only the methods the
// resumable-upload logic actually needs.
type ObjectStorage interface {
	PutObjectStream(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error
	GetObjectStream(ctx context.Context, key string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, key string) error
}

const defaultFileTTL = 7 * 24 * time.Hour // matches typical "dropbox link expires in a week" behavior

var (
	ErrFileNotFound     = errors.New("file not found")
	ErrFileNotReady     = errors.New("file is not ready for download")
	ErrFileWrongRoom    = errors.New("file does not belong to this room")
	ErrUploadNotFound   = errors.New("upload not found")
	ErrUploadIncomplete = errors.New("upload is missing one or more parts")
)

type FileService struct {
	repo      FileRepository
	storage   ObjectStorage // nil for services that never touch upload/download bytes (e.g. worker)
	chunkSize int64         // 0 if storage is nil — the resumable-upload methods aren't usable in that case
}

func NewFileService(repo FileRepository, storage ObjectStorage, chunkSize int64) *FileService {
	return &FileService{repo: repo, storage: storage, chunkSize: chunkSize}
}

// RegisterUpload is called by the ingestion service right after bytes are
// successfully written to object storage in ONE request (the small-file
// path) — this is what turns "a blob exists in MinIO" into "a file the
// rest of the system knows about."
func (s *FileService) RegisterUpload(ctx context.Context, id, roomID, uploaderID, fileName string, fileSize int64, contentType, storageKey string) (*domain.File, error) {
	f := domain.NewFile(id, roomID, uploaderID, fileName, fileSize, contentType, storageKey, defaultFileTTL)
	f.Status = domain.FileStatusStored

	if err := s.repo.Save(ctx, f); err != nil {
		return nil, fmt.Errorf("failed to register upload: %w", err)
	}
	return f, nil
}

func (s *FileService) MarkReady(ctx context.Context, fileID string) error {
	if err := s.repo.UpdateStatus(ctx, fileID, domain.FileStatusReady); err != nil {
		return fmt.Errorf("failed to mark file ready: %w", err)
	}
	return nil
}

func (s *FileService) MarkFailed(ctx context.Context, fileID string) error {
	if err := s.repo.UpdateStatus(ctx, fileID, domain.FileStatusFailed); err != nil {
		return fmt.Errorf("failed to mark file failed: %w", err)
	}
	return nil
}

func (s *FileService) GetForDownload(ctx context.Context, fileID, roomID string) (*domain.File, error) {
	f, err := s.repo.Get(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	if f == nil {
		return nil, ErrFileNotFound
	}
	if f.RoomID != roomID {
		return nil, ErrFileWrongRoom
	}
	if !f.IsUsable() {
		return nil, ErrFileNotReady
	}
	return f, nil
}

func (s *FileService) MarkDeliveredToParticipant(ctx context.Context, fileID, participantID string) error {
	if err := s.repo.MarkDeliveredTo(ctx, fileID, participantID); err != nil {
		return fmt.Errorf("failed to mark file delivered: %w", err)
	}
	return nil
}

func (s *FileService) ListWaitingFilesForParticipant(ctx context.Context, roomID, participantID string) ([]*domain.File, error) {
	files, err := s.repo.ListWaitingForParticipant(ctx, roomID, participantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list waiting files: %w", err)
	}
	return files, nil
}

// --- Resumable upload protocol ---

type InitUploadResult struct {
	FileID     string `json:"file_id"`
	ChunkSize  int64  `json:"chunk_size"`
	TotalParts int    `json:"total_parts"`
}

func (s *FileService) InitUpload(ctx context.Context, roomID, uploaderID, fileName string, fileSize int64, contentType string) (*InitUploadResult, error) {
	fileID := uuid.New().String()
	storageKey := roomID + "/" + fileID + "-" + fileName
	totalParts := int((fileSize + s.chunkSize - 1) / s.chunkSize) // ceiling division

	f := domain.NewFile(fileID, roomID, uploaderID, fileName, fileSize, contentType, storageKey, defaultFileTTL)
	// Status stays FileStatusUploading (domain.NewFile's default) — this
	// record exists but isn't usable for anything (download, dropbox
	// listing) until CompleteUpload flips it to Stored.

	if err := s.repo.Save(ctx, f); err != nil {
		return nil, fmt.Errorf("failed to init upload: %w", err)
	}

	return &InitUploadResult{FileID: fileID, ChunkSize: s.chunkSize, TotalParts: totalParts}, nil
}

// RecordPart stores exactly one chunk as its own small object, separate
// from the file's final object. Re-uploading the same part number (a
// client retry) safely overwrites, rather than erroring or duplicating.
func (s *FileService) RecordPart(ctx context.Context, fileID string, partNumber int, reader io.Reader, size int64) error {
	f, err := s.repo.Get(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to get file for part upload: %w", err)
	}
	if f == nil {
		return ErrUploadNotFound
	}

	partKey := fmt.Sprintf("%s.part%d", f.StorageKey, partNumber)
	if err := s.storage.PutObjectStream(ctx, partKey, reader, size, "application/octet-stream"); err != nil {
		return fmt.Errorf("failed to store part %d: %w", partNumber, err)
	}

	part := &domain.UploadedPart{
		FileID:     fileID,
		PartNumber: partNumber,
		PartKey:    partKey,
		Size:       size,
		UploadedAt: time.Now(),
	}
	if err := s.repo.SaveUploadPart(ctx, part); err != nil {
		return fmt.Errorf("failed to record part %d: %w", partNumber, err)
	}

	return nil
}

// UploadStatus is what a client uses to figure out exactly what to
// (re-)send on resume — MissingParts is the actionable field.
type UploadStatus struct {
	FileID        string `json:"file_id"`
	FileSize      int64  `json:"file_size"`
	ChunkSize     int64  `json:"chunk_size"`
	TotalParts    int    `json:"total_parts"`
	ReceivedParts []int  `json:"received_parts"`
	MissingParts  []int  `json:"missing_parts"`
}

func (s *FileService) GetUploadStatus(ctx context.Context, fileID string) (*UploadStatus, error) {
	f, err := s.repo.Get(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	if f == nil {
		return nil, ErrUploadNotFound
	}

	parts, err := s.repo.ListUploadParts(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to list parts: %w", err)
	}

	totalParts := int((f.FileSize + s.chunkSize - 1) / s.chunkSize)
	received := make(map[int]bool, len(parts))
	receivedList := make([]int, 0, len(parts))
	for _, p := range parts {
		received[p.PartNumber] = true
		receivedList = append(receivedList, p.PartNumber)
	}

	var missing []int
	for i := 1; i <= totalParts; i++ {
		if !received[i] {
			missing = append(missing, i)
		}
	}

	return &UploadStatus{
		FileID:        fileID,
		FileSize:      f.FileSize,
		ChunkSize:     s.chunkSize,
		TotalParts:    totalParts,
		ReceivedParts: receivedList,
		MissingParts:  missing,
	}, nil
}

// partSequenceReader lazily concatenates part objects into one continuous
// stream, opening each part's object only when the previous one is
// exhausted — so reassembling even a very large file never requires
// holding more than one part in memory at a time.
type partSequenceReader struct {
	ctx      context.Context
	storage  ObjectStorage
	partKeys []string
	index    int
	current  io.ReadCloser
}

func newPartSequenceReader(ctx context.Context, storage ObjectStorage, partKeys []string) *partSequenceReader {
	return &partSequenceReader{ctx: ctx, storage: storage, partKeys: partKeys}
}

func (r *partSequenceReader) Read(p []byte) (int, error) {
	for {
		if r.current == nil {
			if r.index >= len(r.partKeys) {
				return 0, io.EOF
			}
			reader, err := r.storage.GetObjectStream(r.ctx, r.partKeys[r.index])
			if err != nil {
				return 0, fmt.Errorf("failed to open part %d for reassembly: %w", r.index+1, err)
			}
			r.current = reader
			r.index++
		}

		n, err := r.current.Read(p)
		if n > 0 {
			return n, nil
		}
		if err == io.EOF {
			r.current.Close()
			r.current = nil
			continue
		}
		if err != nil {
			return 0, err
		}
	}
}

func (r *partSequenceReader) Close() error {
	if r.current != nil {
		return r.current.Close()
	}
	return nil
}

// CompleteUpload verifies every expected part actually exists (never trusts
// a client's claim that it's done), reassembles them into the file's real
// final object, flips the file to Stored, and cleans up the now-redundant
// part objects/rows.
func (s *FileService) CompleteUpload(ctx context.Context, fileID string) (*domain.File, error) {
	f, err := s.repo.Get(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	if f == nil {
		return nil, ErrUploadNotFound
	}

	parts, err := s.repo.ListUploadParts(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to list parts: %w", err)
	}

	totalParts := int((f.FileSize + s.chunkSize - 1) / s.chunkSize)
	if len(parts) != totalParts {
		return nil, ErrUploadIncomplete
	}

	partKeys := make([]string, len(parts))
	for i, p := range parts {
		partKeys[i] = p.PartKey
	}

	reader := newPartSequenceReader(ctx, s.storage, partKeys)
	defer reader.Close()

	if err := s.storage.PutObjectStream(ctx, f.StorageKey, reader, f.FileSize, f.ContentType); err != nil {
		return nil, fmt.Errorf("failed to reassemble file: %w", err)
	}

	if err := s.repo.UpdateStatus(ctx, fileID, domain.FileStatusStored); err != nil {
		return nil, fmt.Errorf("failed to mark file stored: %w", err)
	}

	// Best-effort cleanup — the final file is already correct at this
	// point, so a failure here is logged by the caller but not fatal.
	for _, key := range partKeys {
		_ = s.storage.DeleteObject(ctx, key)
	}
	_ = s.repo.DeleteUploadParts(ctx, fileID)

	f.Status = domain.FileStatusStored
	return f, nil
}

func (s *FileService) AbortUpload(ctx context.Context, fileID string) error {
	f, err := s.repo.Get(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}
	if f == nil {
		return ErrUploadNotFound
	}

	parts, err := s.repo.ListUploadParts(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to list parts: %w", err)
	}
	for _, p := range parts {
		_ = s.storage.DeleteObject(ctx, p.PartKey)
	}

	if err := s.repo.DeleteUploadParts(ctx, fileID); err != nil {
		return fmt.Errorf("failed to delete upload parts: %w", err)
	}
	if err := s.repo.Delete(ctx, fileID); err != nil {
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}