export const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080'
export const INGESTION_URL = import.meta.env.VITE_INGESTION_URL || 'http://localhost:8081'
export const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8080'

export interface CreateRoomResult {
  room_id: string
  expires_at: string
  is_secured: boolean
}

export async function createRoom(password = ''): Promise<CreateRoomResult> {
  const res = await fetch(`${API_URL}/api/rooms`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  })
  if (!res.ok) throw new Error('Failed to create room')
  return res.json()
}

export async function joinRoom(roomId: string, participantId: string): Promise<void> {
  const res = await fetch(`${API_URL}/api/rooms/${roomId}/join`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ participant_id: participantId }),
  })
  if (!res.ok) throw new Error('Failed to join room')
}

// The backend's domain.File struct has no JSON tags, so it serializes in
// Go's PascalCase field names — this interface matches that reality
// exactly (confirmed against real API responses) rather than assuming a
// nicer snake_case shape that isn't what the server actually sends.
export interface RoomFile {
  ID: string
  RoomID: string
  UploaderID: string
  FileName: string
  FileSize: number
  ContentType: string
  StorageKey: string
  Status: string
  CreatedAt: string
  ExpiresAt: string
}

export async function listWaitingFiles(roomId: string, participantId: string): Promise<RoomFile[]> {
  const res = await fetch(`${API_URL}/api/rooms/${roomId}/files?participant_id=${participantId}`)
  if (!res.ok) throw new Error('Failed to list files')
  const data = await res.json()
  return data.files || []
}

export async function uploadFile(roomId: string, file: File): Promise<{ file_id: string }> {
  const res = await fetch(`${INGESTION_URL}/upload`, {
    method: 'POST',
    headers: {
      'X-Room-Id': roomId,
      'X-File-Name': file.name,
      'Content-Type': file.type || 'application/octet-stream',
    },
    body: file,
  })
  if (!res.ok) throw new Error('Upload failed')
  return res.json()
}

/**
 * Returns a direct download URL for a file. Deliberately a plain URL meant
 * for an <a href> tag rather than a fetch() call — this lets the browser's
 * own native download manager handle it, which means Range-header
 * resumability (built server-side) works transparently if the browser
 * itself retries an interrupted download, with no extra client code needed.
 */
export function downloadFileUrl(roomId: string, fileId: string, participantId: string): string {
  return `${API_URL}/api/rooms/${roomId}/files/${fileId}/download?participant_id=${participantId}`
}

export interface VideoTokenResult {
  token: string
  livekit_url: string
}

export async function getVideoToken(roomId: string, participantId: string, name: string): Promise<VideoTokenResult> {
  const res = await fetch(`${API_URL}/api/rooms/${roomId}/video-token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      participant_id: participantId, 
      name: name 
    }),
  })
  if (!res.ok) throw new Error('Failed to get video token')
  return res.json()
}


export async function startRecording(roomId: string) {
  const res = await fetch(`${API_URL}/api/rooms/${roomId}/recording/start`, {
    method: 'POST',
  })
  if (!res.ok) throw new Error('Failed to start recording')
  return res.json()
}


export async function stopRecording(roomId: string) {
  const res = await fetch(`${API_URL}/api/rooms/${roomId}/recording/stop`, {
    method: 'POST',
  })
  if (!res.ok) throw new Error('Failed to stop recording')
  return res.json()
}