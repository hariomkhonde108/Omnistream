package ws

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// We already validate the Origin against cfg.CORSOrigins manually
		// in ServeWS before calling Upgrade — this just tells gorilla not
		// to ALSO run its own stricter same-host default check, which
		// would otherwise reject every legitimate cross-port request
		// (e.g. frontend on :3000 talking to this server on :8080) even
		// after our own check already passed it.
		return true
	},
}

type Handler struct {
	hub            *Hub
	logger         *slog.Logger
	allowedOrigins map[string]bool
}

func NewHandler(hub *Hub, corsOrigins []string, logger *slog.Logger) *Handler {
	allowed := make(map[string]bool, len(corsOrigins))
	for _, o := range corsOrigins {
		allowed[o] = true
	}
	return &Handler{hub: hub, logger: logger, allowedOrigins: allowed}
}

// ServeWS upgrades the connection and registers the client for push
// notifications on roomID. Blocks until the connection closes.
func (h *Handler) ServeWS(w http.ResponseWriter, r *http.Request, roomID string) {
	origin := r.Header.Get("Origin")
	if origin != "" && !h.allowedOrigins[origin] {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Warn("websocket upgrade failed", "error", err)
		return
	}

	client := &Client{RoomID: roomID, Send: make(chan []byte, 16)}
	h.hub.Register(client)

	go h.writePump(conn, client)
	h.readPump(conn, client) // blocks; this client never sends real data, only pongs/close
}

// readPump exists purely to detect disconnects and keep the pong handshake
// alive — this client is push-only and never sends meaningful messages.
func (h *Handler) readPump(conn *websocket.Conn, client *Client) {
	defer func() {
		h.hub.Unregister(client)
		conn.Close()
	}()

	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

func (h *Handler) writePump(conn *websocket.Conn, client *Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select {
		case msg, ok := <-client.Send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}