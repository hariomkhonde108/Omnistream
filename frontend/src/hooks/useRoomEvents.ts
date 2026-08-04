import { useEffect, useRef, useState } from 'react'
import { WS_URL } from '../lib/api'

export interface FileReadyPayload {
  file_id: string
  room_id: string
  file_name: string
  file_size: number
  content_type: string
  ready_at: string
}

export function useRoomEvents(roomId: string, onFileReady: (payload: FileReadyPayload) => void) {
  const [connected, setConnected] = useState(false)

  const onFileReadyRef = useRef(onFileReady)
  onFileReadyRef.current = onFileReady

  useEffect(() => {
    if (!roomId) return

    const socketUrl = `${WS_URL}/ws/rooms/${roomId}`
    const ws = new WebSocket(socketUrl)

    ws.onopen = () => {
      console.log(`[WS] Connected to room events: ${roomId}`)
      setConnected(true)
    }

    ws.onclose = (evt) => {
      console.log(`[WS] Disconnected from room events: ${evt.reason || evt.code}`)
      setConnected(false)
    }

    ws.onerror = (err) => {
      console.error('[WS] Connection error:', err)
      setConnected(false)
    }

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data)
        if (msg.type === 'file_ready') {
          console.log('[WS] Received file_ready event:', msg.payload)
          onFileReadyRef.current(msg.payload as FileReadyPayload)
        }
      } catch (err) {
        console.error('Failed to parse websocket message:', event.data, err)
      }
    }

    return () => {
      // Prevent closing while still in CONNECTING state to avoid React Strict Mode warnings
      if (ws.readyState === WebSocket.CONNECTING) {
        ws.onopen = () => ws.close()
      } else {
        ws.close()
      }
    }
  }, [roomId])

  return { connected }
}