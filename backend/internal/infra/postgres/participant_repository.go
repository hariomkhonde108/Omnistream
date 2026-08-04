package postgres

import (
	"context"
	"fmt"

	"omnistream/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ParticipantRepository struct {
	pool *pgxpool.Pool
}

func NewParticipantRepository(pool *pgxpool.Pool) *ParticipantRepository {
	return &ParticipantRepository{pool: pool}
}

// Save is an upsert — a rejoining participant (same client-generated ID,
// e.g. reconnecting after a page refresh) just updates last_seen rather
// than erroring or creating a duplicate.
func (r *ParticipantRepository) Save(ctx context.Context, p *domain.Participant) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO participants (id, room_id, joined_at, last_seen)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET last_seen = EXCLUDED.last_seen
	`, p.ID, p.RoomID, p.JoinedAt, p.LastSeen)
	if err != nil {
		return fmt.Errorf("failed to save participant: %w", err)
	}
	return nil
}
