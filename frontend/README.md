# DropVault v2 — Frontend

A deliberately minimal frontend for the dropvault-v2 backend: create/join a
room, drop a file, see it show up live for anyone else in the room (no
polling), download it, and start a video call. This isn't meant to be
polished — it exists to prove the whole backend works together, and to
make manual testing far less painful than juggling curl/websocat.

## Prerequisites

The full dropvault-v2 backend must already be running:
- `docker compose up -d` (Postgres, MinIO, Kafka, LiveKit) from the backend repo
- `go run ./cmd/api`
- `go run ./cmd/ingestion`
- `go run ./cmd/worker`

## Setup

```bash
npm install
cp .env.example .env   # defaults already match the backend's default ports
npm run dev
```

Open http://localhost:3000.

## How each piece maps to what you've already tested via curl

| UI action | Backend endpoint |
|---|---|
| "Create a new room" | `POST /api/rooms` |
| Landing on a room page | `POST /api/rooms/:id/join` |
| Drop a file | `POST /upload` (ingestion service) |
| File list appearing/updating | `GET /api/rooms/:id/files` (initial load) + live push via `GET /ws/rooms/:id` |
| "Download" button | `GET /api/rooms/:id/files/:fileId/download` — a plain link, so the browser's own download manager handles it, and Range-header resumability works transparently if the browser retries |
| "Join video call" | `POST /api/rooms/:id/video-token`, then LiveKit's own client SDK connects using that token |

## Testing multi-peer behavior locally

Participant identity is stored in `sessionStorage` (per-tab, not
per-browser) specifically so that **opening a second tab** gives you a
second, independent participant — no extra setup needed to simulate
multiple people in one room:

1. Create a room in tab 1, copy the room URL
2. Open that same URL in tab 2 (a new tab, not a duplicate of tab 1)
3. Drop a file in tab 1 — it should appear in tab 2's file list within a
   second or two, with **no page refresh**, confirming the live WebSocket
   push
4. Download it from tab 2 — refresh tab 1's list (or wait for it to
   naturally refresh) and confirm the file still doesn't disappear for
   anyone else who hasn't downloaded it yet, if you open a third tab
5. Click "Join video call" in both tabs — you should see and hear yourself
   in two tiles (or two different tabs' cameras, if testing with two
   physical devices instead of two tabs on one machine)

## What's deliberately NOT built here (by design, not oversight)

- No UI for the resumable-upload protocol (initiate/parts/status/complete)
  — uploads use the simple single-request `/upload` path only. The
  resumable protocol is fully built and tested on the backend; a resumable
  upload UI would mean chunking the file client-side and calling those
  endpoints directly, which is a real follow-up if you want it, not
  something this minimal frontend currently does.
- No recording/AI-notes UI — those features aren't built on the backend
  yet either.
- No room password UI — rooms are created without a password in this
  frontend; `verifyRoom` exists in `lib/api.ts` but isn't wired to any UI
  yet.
