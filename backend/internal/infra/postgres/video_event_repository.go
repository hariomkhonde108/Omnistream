package postgres

import (
	"context"
	"fmt"

	"omnistream/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type VideoEventRepository struct {
	pool *pgxpool.Pool
}

func NewVideoEventRepository(pool *pgxpool.Pool) *VideoEventRepository {
	return &VideoEventRepository{pool: pool}
}

func (r *VideoEventRepository) Save(ctx context.Context, e *domain.VideoEvent) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO video_events (room_id, event_type, participant_id, occurred_at)
		VALUES ($1, $2, $3, $4)
	`, e.RoomID, e.EventType, e.ParticipantID, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("failed to save video event: %w", err)
	}
	return nil
}

func (r *VideoEventRepository) ListForRoom(ctx context.Context, roomID string) ([]*domain.VideoEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, room_id, event_type, participant_id, occurred_at
		FROM video_events
		WHERE room_id = $1
		ORDER BY occurred_at ASC
	`, roomID)
	if err != nil {
		return nil, fmt.Errorf("failed to list video events: %w", err)
	}
	defer rows.Close()

	var events []*domain.VideoEvent
	for rows.Next() {
		var e domain.VideoEvent
		if err := rows.Scan(&e.ID, &e.RoomID, &e.EventType, &e.ParticipantID, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("failed to scan video event row: %w", err)
		}
		events = append(events, &e)
	}
	return events, rows.Err()
}