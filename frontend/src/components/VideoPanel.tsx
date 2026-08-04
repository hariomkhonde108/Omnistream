import { useState } from 'react'
import { LiveKitRoom, VideoConference } from '@livekit/components-react'
import '@livekit/components-styles'

interface VideoPanelProps {
  serverUrl: string
  token: string
  onDisconnect: () => void
}

/**
 * Uses a fully self-controlled "expanded" state rather than relying on the
 * browser's native Fullscreen API or LiveKit's own icon — that icon turned
 * out to just be an internal focus/spotlight-layout toggle, not real
 * fullscreen, which is why the earlier :fullscreen CSS fix had nothing to
 * act on. This approach is guaranteed to actually cover the full viewport
 * when toggled, since it's plain, predictable positioning we control
 * directly. Also drops the old overflow-hidden wrapper, which was very
 * likely clipping LiveKit's chat side panel in the cramped docked view.
 */
export function VideoPanel({ serverUrl, token, onDisconnect }: VideoPanelProps) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div
      className={
        expanded
          ? 'fixed inset-0 z-50 bg-black'
          : 'relative w-full rounded-[28px] border border-slate-200'
      }
      style={expanded ? undefined : { height: 600 }}
    >
      <button
        onClick={() => setExpanded((e) => !e)}
        className="absolute right-3 top-3 z-10 rounded-lg bg-black/60 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-black/80"
      >
        {expanded ? 'Exit full screen' : 'Full screen'}
      </button>

      <LiveKitRoom
        serverUrl={serverUrl}
        token={token}
        connect
        video
        audio
        onDisconnected={onDisconnect}
        data-lk-theme="default"
        style={{ height: '100%', width: '100%' }}
      >
        <VideoConference />
      </LiveKitRoom>
    </div>
  )
}