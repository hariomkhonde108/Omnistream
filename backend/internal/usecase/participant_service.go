package usecase

import (
	"context"
	"fmt"

	"omnistream/internal/domain"
)

type ParticipantRepository interface {
	Save(ctx context.Context, p *domain.Participant) error
}

type ParticipantService struct {
	repo ParticipantRepository
}

func NewParticipantService(repo ParticipantRepository) *ParticipantService {
	return &ParticipantService{repo: repo}
}

// Join registers a participant in a room. participantID is generated
// client-side (same crypto.randomUUID() pattern as the original P2P
// project's peerId) and passed in — not generated here — so a
// reconnecting client can rejoin with the SAME id and correctly resume
// seeing their own delivery history, rather than looking like a brand new
// person with nothing delivered yet.
func (s *ParticipantService) Join(ctx context.Context, participantID, roomID string) (*domain.Participant, error) {
	p := domain.NewParticipant(participantID, roomID)
	if err := s.repo.Save(ctx, p); err != nil {
		return nil, fmt.Errorf("failed to join room: %w", err)
	}
	return p, nil
}
