package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"omnistream/internal/domain"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrRoomNotFound    = errors.New("room not found")
	ErrRoomExpired     = errors.New("room expired")
	ErrInvalidPassword = errors.New("invalid password")
)

// RoomRepository is the persistence port — satisfied by
// internal/infra/postgres.RoomRepository, but this use case has zero
// knowledge that Postgres specifically is behind it. Same dependency-
// inversion pattern as the original P2P project's SessionService.
type RoomRepository interface {
	Save(ctx context.Context, room *domain.Room) error
	Get(ctx context.Context, id string) (*domain.Room, error)
}

const defaultRoomTTL = 24 * time.Hour // longer than the P2P project's 1hr —
// async dropbox rooms are meant to outlive a single live session, since the
// whole point is the receiver might not show up for a while.

type RoomService struct {
	repo RoomRepository
}

func NewRoomService(repo RoomRepository) *RoomService {
	return &RoomService{repo: repo}
}

type CreateRoomResult struct {
	Room      *domain.Room
	IsSecured bool
}

func (s *RoomService) CreateRoom(ctx context.Context, password string) (*CreateRoomResult, error) {
	var hash string
	if password != "" {
		hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return nil, fmt.Errorf("failed to hash password: %w", err)
		}
		hash = string(hashed)
	}

	room := domain.NewRoom(uuid.New().String(), hash, defaultRoomTTL)

	if err := s.repo.Save(ctx, room); err != nil {
		return nil, fmt.Errorf("failed to persist room: %w", err)
	}

	return &CreateRoomResult{Room: room, IsSecured: room.IsSecured()}, nil
}

func (s *RoomService) VerifyRoom(ctx context.Context, roomID, password string) error {
	room, err := s.repo.Get(ctx, roomID)
	if err != nil {
		return fmt.Errorf("failed to fetch room: %w", err)
	}
	if room == nil {
		return ErrRoomNotFound
	}
	if room.IsExpired() {
		return ErrRoomExpired
	}
	if room.IsSecured() {
		if err := bcrypt.CompareHashAndPassword([]byte(room.PasswordHash), []byte(password)); err != nil {
			return ErrInvalidPassword
		}
	}
	return nil
}
