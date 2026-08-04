package ws

import (
	"encoding/json"
	"log/slog"
	"sync"
)

// Event is what gets pushed down to connected clients — deliberately
// generic (Type + Payload) so this Hub can carry more than just
// "file_ready" in the future without changing its shape.
type Event struct {
	Type    string `json:"type"`
	RoomID  string `json:"room_id"`
	Payload any    `json:"payload"`
}

// Client represents one connected browser's WebSocket. This is
// one-directional — the client only ever RECEIVES pushes, it never sends
// meaningful data back — so unlike the original P2P project's Client, this
// has no read-side routing logic at all.
type Client struct {
	RoomID string
	Send   chan []byte
}

// Hub tracks which clients are listening to which rooms and broadcasts to
// them. Uses a mutex rather than the channel-owned-by-one-goroutine pattern
// from the original P2P project's Hub — that pattern was necessary there
// because of complex bidirectional signaling state; this Hub only ever does
// one thing (fan a message out to a room), which a mutex protects just as
// correctly with less machinery.
type Hub struct {
	mu     sync.Mutex
	rooms  map[string]map[*Client]bool
	logger *slog.Logger
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{rooms: make(map[string]map[*Client]bool), logger: logger}
}

func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.rooms[c.RoomID] == nil {
		h.rooms[c.RoomID] = make(map[*Client]bool)
	}
	h.rooms[c.RoomID][c] = true
}

func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conns, ok := h.rooms[c.RoomID]
	if !ok {
		return
	}
	if _, exists := conns[c]; !exists {
		return
	}

	delete(conns, c)
	close(c.Send)

	if len(conns) == 0 {
		delete(h.rooms, c.RoomID)
	}
}

// Broadcast is called from the Kafka consumer goroutine in cmd/api — it's
// the bridge between "the worker (a different process) marked a file
// ready" and "tell every browser currently watching this room."
func (h *Hub) Broadcast(roomID string, event Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conns, ok := h.rooms[roomID]
	if !ok {
		return // nobody's currently connected to this room — nothing to do
	}

	data, err := json.Marshal(event)
	if err != nil {
		h.logger.Error("failed to marshal ws event", "error", err)
		return
	}

	for c := range conns {
		select {
		case c.Send <- data:
		default:
			h.logger.Warn("client send buffer full, dropping message", "room_id", roomID)
		}
	}
}