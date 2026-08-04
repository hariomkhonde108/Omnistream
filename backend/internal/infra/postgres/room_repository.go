package postgres

import (
	"context"
	"fmt"

	"omnistream/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RoomRepository struct {
	pool *pgxpool.Pool
}

func NewRoomRepository(pool *pgxpool.Pool) *RoomRepository {
	return &RoomRepository{pool: pool}
}

func (r *RoomRepository) Save(ctx context.Context, room *domain.Room) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO rooms (id, password_hash, created_at, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			password_hash = EXCLUDED.password_hash,
			expires_at = EXCLUDED.expires_at
	`, room.ID, room.PasswordHash, room.CreatedAt, room.ExpiresAt)
	if err != nil {
		return fmt.Errorf("failed to save room: %w", err)
	}
	return nil
}

func (r *RoomRepository) Get(ctx context.Context, id string) (*domain.Room, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, password_hash, created_at, expires_at
		FROM rooms
		WHERE id = $1
	`, id)

	var room domain.Room
	err := row.Scan(&room.ID, &room.PasswordHash, &room.CreatedAt, &room.ExpiresAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get room: %w", err)
	}

	return &room, nil
}
