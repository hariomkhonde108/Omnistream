package domain

import "time"

// Participant represents one browser session/peer inside a room. This is
// the identity that per-file delivery tracking (FileDelivery) is keyed on —
// without a real notion of "who's asking," there's no way to correctly
// answer "what files is THIS person still waiting on" in a room with more
// than 2 people.
type Participant struct {
	ID       string
	RoomID   string
	JoinedAt time.Time
	LastSeen time.Time
}

func NewParticipant(id, roomID string) *Participant {
	now := time.Now()
	return &Participant{
		ID:       id,
		RoomID:   roomID,
		JoinedAt: now,
		LastSeen: now,
	}
}
