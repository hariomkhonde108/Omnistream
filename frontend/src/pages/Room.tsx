import { useCallback, useEffect, useRef, useState } from 'react'
import { useParams } from 'react-router-dom'
import { getParticipantId } from '../lib/participant'
import { joinRoom, listWaitingFiles, uploadFile, getVideoToken, startRecording, RoomFile, stopRecording } from '../lib/api'
import { useRoomEvents, FileReadyPayload } from '../hooks/useRoomEvents'
import { DropZone } from '../components/DropZone'
import { FileList } from '../components/FileList'
import { VideoPanel } from '../components/VideoPanel'
import { QRCodeSVG } from 'qrcode.react'

export function Room() {
  const { roomId = '' } = useParams()
  const [showQR, setShowQR] = useState(false)
  const [copied, setCopied] = useState(false)
  
  const joinUrl = `${window.location.origin}/room/${roomId}`
  const participantId = useRef(getParticipantId()).current
  const [displayName, setDisplayName] = useState('')

  const [joined, setJoined] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [files, setFiles] = useState<RoomFile[]>([])
  const [uploading, setUploading] = useState(false)
  const [videoToken, setVideoToken] = useState<string | null>(null)
  const [livekitUrl, setLivekitUrl] = useState<string | null>(null)
  
  // New state to track if recording has been requested
  const [isRecording, setIsRecording] = useState(false)

  const refreshFiles = useCallback(async () => {
    try {
      const list = await listWaitingFiles(roomId, participantId)
      setFiles(list)
    } catch (err) {
      console.error('Failed to refresh file list:', err)
    }
  }, [roomId, participantId])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        await joinRoom(roomId, participantId)
        if (cancelled) return
        setJoined(true)
        await refreshFiles()
      } catch {
        if (!cancelled) setError('Could not join this room. Check the room ID and try again.')
      }
    })()
    return () => {
      cancelled = true
    }
  }, [roomId, participantId, refreshFiles])

  const handleFileReady = useCallback(
    (_payload: FileReadyPayload) => {
      refreshFiles()
    },
    [refreshFiles]
  )

  const { connected } = useRoomEvents(roomId, handleFileReady)

  const handleFilesSelected = async (selected: File[]) => {
    setUploading(true)
    setError(null)
    try {
      for (const file of selected) {
        await uploadFile(roomId, file)
      }
    } catch {
      setError('Upload failed. Check that the ingestion service is reachable.')
    } finally {
      setUploading(false)
    }
  }

  const handleJoinVideo = async () => {
    if (!displayName.trim()) return
    setError(null)
    try {
      const { token, livekit_url } = await getVideoToken(roomId, participantId, displayName.trim())
      setVideoToken(token)
      setLivekitUrl(livekit_url)
    } catch {
      setError('Could not start the video call.')
    }
  }

  const handleCopyLink = async () => {
    try {
      await navigator.clipboard.writeText(joinUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      console.error('Failed to copy link', err)
    }
  }

  // New function to trigger the backend recording endpoint
const toggleRecording = async () => {
    try {
      if (isRecording) {
        // Optimistically set to false for immediate UI feedback
        setIsRecording(false);
        await stopRecording(roomId);
      } else {
        // Optimistically set to true
        setIsRecording(true);
        await startRecording(roomId);
      }
    } catch (err) {
      console.error('Failed to toggle recording state', err);
      // Revert the button state if the API call actually failed
      setIsRecording((prev) => !prev);
    }
  };

  if (error && !joined) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-sky-50 to-indigo-50 p-6">
        <div className="glass-surface rounded-[28px] p-8 text-center text-red-600">{error}</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-sky-50 to-indigo-50 p-6">
      <div className="mx-auto max-w-3xl space-y-6">
        <div className="glass-surface flex items-center justify-between rounded-[28px] p-6">
          <div>
            <p className="text-xs uppercase tracking-wide text-slate-500">Room</p>
            <p className="font-mono text-sm text-slate-900">{roomId}</p>
          </div>
          <div className="flex items-center gap-3">
            {/* Record AI Notes Button */}
            <button
              onClick={toggleRecording}
              disabled={!videoToken}
              className={`rounded-xl px-4 py-1.5 text-xs font-semibold shadow-sm ring-1 transition ${
                isRecording 
                  ? 'bg-red-50 text-red-600 ring-red-200 hover:bg-red-100' 
                  : 'bg-white text-slate-700 ring-slate-200 hover:bg-slate-50'
              } disabled:cursor-not-allowed disabled:opacity-50`}
            >
              {isRecording ? 'Stop Recording' : 'Record AI Notes'}
            </button>
            
            <button
              onClick={() => setShowQR(true)}
              className="rounded-xl bg-white px-4 py-1.5 text-xs font-semibold text-slate-700 shadow-sm ring-1 ring-slate-200 transition hover:bg-slate-50"
            >
              Share
            </button>
            <span
              className={`rounded-full px-3 py-1 text-xs font-semibold ${
                connected ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-500'
              }`}
            >
              {connected ? 'Live' : 'Connecting…'}
            </span>
          </div>
        </div>

        {!videoToken ? (
          <div className="glass-surface space-y-4 rounded-[28px] p-6">
            <h2 className="text-sm font-semibold text-slate-900">Join the video call</h2>
            <input 
              type="text" 
              placeholder="Enter your name" 
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleJoinVideo()}
              className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm outline-none focus:border-sky-400"
              maxLength={32}
            />
            <button
              onClick={handleJoinVideo}
              disabled={!displayName.trim()}
              className="w-full rounded-2xl bg-sky-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Join Room
            </button>
          </div>
        ) : (
          <VideoPanel serverUrl={livekitUrl!} token={videoToken} onDisconnect={() => setVideoToken(null)} />
        )}

        <div className="glass-surface rounded-[28px] p-6">
          <h2 className="mb-4 text-sm font-semibold text-slate-900">Share a file</h2>
          <DropZone onFilesSelected={handleFilesSelected} disabled={uploading} />
        </div>

        <div className="glass-surface rounded-[28px] p-6">
          <h2 className="mb-4 text-sm font-semibold text-slate-900">Files in this room</h2>
          <FileList files={files} roomId={roomId} participantId={participantId} onDownloaded={refreshFiles} />
        </div>

        {error && <p className="text-sm text-red-600">{error}</p>}
      </div>

      {showQR && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-6 backdrop-blur-sm"
          onClick={() => setShowQR(false)}
        >
          <div
            className="w-full max-w-sm rounded-[28px] bg-white p-8 text-center shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="mb-6 text-lg font-bold text-slate-900">Join this room</h3>

            <div className="mx-auto mb-6 flex justify-center rounded-2xl bg-white p-4 shadow-sm ring-1 ring-slate-100">
              <QRCodeSVG
                value={joinUrl}
                size={220}
                bgColor={"#ffffff"}
                fgColor={"#0f172a"}
                level={"L"}
              />
            </div>

            <div className="mb-6 flex items-center justify-between gap-3 rounded-xl bg-slate-50 p-3 ring-1 ring-slate-100">
              <p className="truncate font-mono text-xs text-slate-500">{joinUrl}</p>
              <button
                onClick={handleCopyLink}
                className="shrink-0 rounded-lg bg-sky-100 px-3 py-1.5 text-xs font-semibold text-sky-700 transition hover:bg-sky-200"
              >
                {copied ? 'Copied!' : 'Copy'}
              </button>
            </div>

            <button
              onClick={() => setShowQR(false)}
              className="w-full rounded-2xl bg-slate-100 px-4 py-3 text-sm font-semibold text-slate-700 transition hover:bg-slate-200"
            >
              Close
            </button>
          </div>
        </div>
      )}
    </div>
  )
}