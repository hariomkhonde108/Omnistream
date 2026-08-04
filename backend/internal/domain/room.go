package domain

import "time"

// Room is the container files/participants belong to. Conceptually the same
// idea as the original P2P project's Session, extended with a mode: rooms
// here can be live (both parties connected right now) or used purely as an
// async dropbox (uploader present, receiver arrives later).
type Room struct {
	ID           string
	PasswordHash string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

func NewRoom(id, passwordHash string, ttl time.Duration) *Room {
	now := time.Now()
	return &Room{
		ID:           id,
		PasswordHash: passwordHash,
		CreatedAt:    now,
		ExpiresAt:    now.Add(ttl),
	}
}

func (r *Room) IsSecured() bool {
	return r.PasswordHash != ""
}

func (r *Room) IsExpired() bool {
	return time.Now().After(r.ExpiresAt)
}
