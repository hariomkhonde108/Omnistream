package postgres

import (
	"context"
	"fmt"

	"omnistream/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FileRepository struct {
	pool *pgxpool.Pool
}

func NewFileRepository(pool *pgxpool.Pool) *FileRepository {
	return &FileRepository{pool: pool}
}

func (r *FileRepository) Save(ctx context.Context, f *domain.File) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO files (id, room_id, uploader_id, file_name, file_size, content_type, storage_key, status, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status
	`, f.ID, f.RoomID, f.UploaderID, f.FileName, f.FileSize, f.ContentType, f.StorageKey, f.Status, f.CreatedAt, f.ExpiresAt)
	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}
	return nil
}

func (r *FileRepository) Get(ctx context.Context, id string) (*domain.File, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, room_id, uploader_id, file_name, file_size, content_type, storage_key, status, created_at, expires_at
		FROM files
		WHERE id = $1
	`, id)

	f, err := scanFile(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	return f, nil
}

// UpdateStatus is what the worker calls once it's done processing a file
// (e.g. after any post-upload work completes) to flip it from "stored" to
// "ready". This only ever affects the file itself — never any individual
// participant's delivery record.
func (r *FileRepository) UpdateStatus(ctx context.Context, id string, status domain.FileStatus) error {
	_, err := r.pool.Exec(ctx, `UPDATE files SET status = $1 WHERE id = $2`, status, id)
	if err != nil {
		return fmt.Errorf("failed to update file status: %w", err)
	}
	return nil
}

// MarkDeliveredTo records that ONE specific participant has retrieved this
// file. ON CONFLICT DO NOTHING makes a duplicate download request (retry,
// double-click) a safe no-op rather than an error, and — critically — this
// has zero effect on any other participant's ability to still see and
// download the same file.
func (r *FileRepository) MarkDeliveredTo(ctx context.Context, fileID, participantID string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO file_deliveries (file_id, participant_id)
		VALUES ($1, $2)
		ON CONFLICT (file_id, participant_id) DO NOTHING
	`, fileID, participantID)
	if err != nil {
		return fmt.Errorf("failed to mark file delivered to participant: %w", err)
	}
	return nil
}

// ListWaitingForParticipant is the core of the async dropbox feature,
// correctly scoped per-person: "what files in this room is THIS specific
// participant still waiting on" — independent of what any other
// participant has already downloaded. This is what makes it correct for a
// participant who joins late, or for multiple receivers each grabbing their
// own copy of the same file.
func (r *FileRepository) ListWaitingForParticipant(ctx context.Context, roomID, participantID string) ([]*domain.File, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT f.id, f.room_id, f.uploader_id, f.file_name, f.file_size, f.content_type, f.storage_key, f.status, f.created_at, f.expires_at
		FROM files f
		WHERE f.room_id = $1
		  AND f.status = $2
		  AND NOT EXISTS (
		    SELECT 1 FROM file_deliveries fd
		    WHERE fd.file_id = f.id AND fd.participant_id = $3
		  )
		ORDER BY f.created_at ASC
	`, roomID, domain.FileStatusReady, participantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list waiting files: %w", err)
	}
	defer rows.Close()

	var files []*domain.File
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}
		files = append(files, f)
	}

	return files, rows.Err()
}

// scanRow abstracts over pgx.Row and pgx.Rows, both of which implement
// Scan(...) with the same signature — lets Get and ListWaitingForParticipant
// share one row->struct mapping instead of duplicating the same field list.
type scanRow interface {
	Scan(dest ...any) error
}

func scanFile(row scanRow) (*domain.File, error) {
	var f domain.File
	err := row.Scan(
		&f.ID, &f.RoomID, &f.UploaderID, &f.FileName, &f.FileSize,
		&f.ContentType, &f.StorageKey, &f.Status, &f.CreatedAt, &f.ExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	return &f, nil
}


// SaveUploadPart is an upsert — a retried/duplicate upload of the same part
// number (e.g. a client resending after a timeout, unsure if it landed)
// just overwrites the record rather than erroring.
func (r *FileRepository) SaveUploadPart(ctx context.Context, p *domain.UploadedPart) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO upload_parts (file_id, part_number, part_key, size, uploaded_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (file_id, part_number) DO UPDATE SET
			part_key = EXCLUDED.part_key,
			size = EXCLUDED.size,
			uploaded_at = EXCLUDED.uploaded_at
	`, p.FileID, p.PartNumber, p.PartKey, p.Size, p.UploadedAt)
	if err != nil {
		return fmt.Errorf("failed to save upload part: %w", err)
	}
	return nil
}

func (r *FileRepository) ListUploadParts(ctx context.Context, fileID string) ([]*domain.UploadedPart, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT file_id, part_number, part_key, size, uploaded_at
		FROM upload_parts
		WHERE file_id = $1
		ORDER BY part_number ASC
	`, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to list upload parts: %w", err)
	}
	defer rows.Close()

	var parts []*domain.UploadedPart
	for rows.Next() {
		var p domain.UploadedPart
		if err := rows.Scan(&p.FileID, &p.PartNumber, &p.PartKey, &p.Size, &p.UploadedAt); err != nil {
			return nil, fmt.Errorf("failed to scan upload part row: %w", err)
		}
		parts = append(parts, &p)
	}
	return parts, rows.Err()
}

func (r *FileRepository) DeleteUploadParts(ctx context.Context, fileID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM upload_parts WHERE file_id = $1`, fileID)
	if err != nil {
		return fmt.Errorf("failed to delete upload parts: %w", err)
	}
	return nil
}

// Delete removes the file record entirely — used when an upload is
// aborted before ever completing, so it doesn't linger as a permanent
// "uploading" ghost record.
func (r *FileRepository) Delete(ctx context.Context, fileID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM files WHERE id = $1`, fileID)
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

