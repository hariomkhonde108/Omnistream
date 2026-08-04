import { RoomFile, downloadFileUrl } from '../lib/api'

interface FileListProps {
  files: RoomFile[]
  roomId: string
  participantId: string
  onDownloaded: () => void
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

export function FileList({ files, roomId, participantId, onDownloaded }: FileListProps) {
  if (files.length === 0) {
    return <p className="text-sm text-slate-500">No files waiting for you in this room yet.</p>
  }

  return (
    <ul className="space-y-2">
      {files.map((f) => (
        <li
          key={f.ID}
          className="flex items-center justify-between rounded-2xl border border-slate-200 bg-white/80 px-4 py-3 text-sm"
        >
          <div>
            <p className="font-medium text-slate-900">{f.FileName}</p>
            <p className="text-slate-500">{formatBytes(f.FileSize)}</p>
          </div>
          <a
            href={downloadFileUrl(roomId, f.ID, participantId)}
              download={f.FileName}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => setTimeout(onDownloaded, 1500)}
              className="rounded-xl bg-sky-600 px-3 py-2 font-semibold text-white transition hover:bg-sky-700"
            >
              Download
            </a>
        </li>
      ))}
    </ul>
  )
}
