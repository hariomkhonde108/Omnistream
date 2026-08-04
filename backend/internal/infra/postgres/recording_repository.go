package postgres

import (
	"context"
	"errors"
	"fmt"

	"omnistream/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RecordingRepository struct {
	pool *pgxpool.Pool
}

func NewRecordingRepository(pool *pgxpool.Pool) *RecordingRepository {
	return &RecordingRepository{pool: pool}
}

func (r *RecordingRepository) Save(ctx context.Context, rec *domain.Recording) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO recordings (egress_id, room_id, storage_key, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (egress_id) DO NOTHING
	`, rec.EgressID, rec.RoomID, rec.StorageKey, rec.Status, rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to save recording: %w", err)
	}
	return nil
}

func (r *RecordingRepository) Get(ctx context.Context, egressID string) (*domain.Recording, error) {
	var rec domain.Recording
	err := r.pool.QueryRow(ctx, `
		SELECT egress_id, room_id, storage_key, status, duration_seconds, size_bytes, created_at, completed_at
		FROM recordings WHERE egress_id = $1
	`, egressID).Scan(
		&rec.EgressID, &rec.RoomID, &rec.StorageKey, &rec.Status,
		&rec.DurationSeconds, &rec.SizeBytes, &rec.CreatedAt, &rec.CompletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get recording: %w", err)
	}
	return &rec, nil
}

func (r *RecordingRepository) MarkComplete(ctx context.Context, egressID string, durationSeconds, sizeBytes int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE recordings SET status = $1, duration_seconds = $2, size_bytes = $3, completed_at = now()
		WHERE egress_id = $4
	`, domain.RecordingStatusComplete, durationSeconds, sizeBytes, egressID)
	if err != nil {
		return fmt.Errorf("failed to mark recording complete: %w", err)
	}
	return nil
}

func (r *RecordingRepository) MarkFailed(ctx context.Context, egressID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE recordings SET status = $1, completed_at = now() WHERE egress_id = $2
	`, domain.RecordingStatusFailed, egressID)
	if err != nil {
		return fmt.Errorf("failed to mark recording failed: %w", err)
	}
	return nil
}

func (r *RecordingRepository) GetActiveByRoom(ctx context.Context, roomID string) (*domain.Recording, error) {
	var rec domain.Recording
	err := r.pool.QueryRow(ctx, `
		SELECT egress_id, room_id, storage_key, status, duration_seconds, size_bytes, created_at, completed_at
		FROM recordings
		WHERE room_id = $1 AND status = $2
		ORDER BY created_at DESC LIMIT 1
	`, roomID, domain.RecordingStatusStarting).Scan(
		&rec.EgressID, &rec.RoomID, &rec.StorageKey, &rec.Status,
		&rec.DurationSeconds, &rec.SizeBytes, &rec.CreatedAt, &rec.CompletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get active recording: %w", err)
	}
	return &rec, nil
}