package domain

import "time"

// VideoEvent is an append-only log of what happened in a room's video
// session — sourced from LiveKit's webhooks, not something we compute
// ourselves. Deliberately a simple log rather than a full state machine:
// "what happened and when" is enough for now; richer session state
// (current participant list, session duration) can be derived from this
// log later if needed, rather than maintained as separate mutable state
// that could drift from what LiveKit itself reports.
type VideoEvent struct {
	ID            int64
	RoomID        string
	EventType     string // "room_started", "participant_joined", "participant_left", "room_finished", etc.
	ParticipantID string // empty for room-level events
	OccurredAt    time.Time
}