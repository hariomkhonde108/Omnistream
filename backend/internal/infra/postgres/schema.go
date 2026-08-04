package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema is intentionally run as idempotent CREATE TABLE IF NOT EXISTS
// statements at service startup rather than via a separate migration tool.
// This is a deliberate simplification for this stage of the project — fine
// for a small, evolving schema with one contributor. Once the schema
// stabilizes (or a second contributor joins), swap this for a real
// migration tool like golang-migrate so schema changes are versioned and
// reviewable, rather than "whatever CREATE TABLE currently says."
//
// Multi-peer design note: delivery status is NOT a flag on the file itself.
// A single "delivered_at" column on files would mean the moment ANY one
// participant downloads a file, it disappears from everyone else's view —
// wrong for a room with more than 2 people. Instead, delivery is tracked
// per (file, participant) pair in file_deliveries below, so each
// participant — including ones who join after the file was already grabbed
// by someone else — independently sees whether THEY have it yet.
const schema = `
CREATE TABLE IF NOT EXISTS rooms (
    id            TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS participants (
    id         TEXT PRIMARY KEY,
    room_id    TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_participants_room ON participants (room_id);

-- status only ever describes whether the FILE ITSELF is usable
-- (stored/ready/failed/expired) — never who has or hasn't received it.
CREATE TABLE IF NOT EXISTS files (
    id            TEXT PRIMARY KEY,
    room_id       TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    uploader_id   TEXT NOT NULL DEFAULT '',
    file_name     TEXT NOT NULL,
    file_size     BIGINT NOT NULL,
    content_type  TEXT NOT NULL DEFAULT '',
    storage_key   TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_room_status ON files (room_id, status);

-- The per-participant delivery record. A row existing here means exactly
-- one thing: this specific participant has retrieved this specific file.
-- Nothing else in the system is affected by one participant's delivery.
CREATE TABLE IF NOT EXISTS file_deliveries (
    file_id        TEXT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
    delivered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (file_id, participant_id)
);


-- Tracks individual chunks of an in-progress resumable upload. Each part
-- is stored as its own small MinIO object (part_key); this table is what
-- lets the server answer "what's still missing" on resume.
CREATE TABLE IF NOT EXISTS upload_parts (
    file_id     TEXT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    part_key    TEXT NOT NULL,
    size        BIGINT NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (file_id, part_number)
);
-- Append-only log of LiveKit webhook events, scoped to our own room IDs.
-- Deliberately not a state machine — just a record of what happened and
-- when, sourced from LiveKit itself rather than tracked independently.
CREATE TABLE IF NOT EXISTS video_events (
    id             BIGSERIAL PRIMARY KEY,
    room_id        TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    participant_id TEXT NOT NULL DEFAULT '',
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_video_events_room ON video_events (room_id, occurred_at);

CREATE TABLE IF NOT EXISTS recordings (
    egress_id        TEXT PRIMARY KEY,
    room_id          TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    storage_key      TEXT NOT NULL,
    status           TEXT NOT NULL,
    duration_seconds BIGINT NOT NULL DEFAULT 0,
    size_bytes       BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_recordings_room ON recordings (room_id);
`

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}
