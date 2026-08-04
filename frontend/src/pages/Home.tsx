import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { createRoom } from '../lib/api'

export function Home() {
  const navigate = useNavigate()
  const [joinId, setJoinId] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  const handleCreate = async () => {
    setCreating(true)
    setError(null)
    try {
      const room = await createRoom('')
      navigate(`/room/${room.room_id}`)
    } catch {
      setError('Could not create a room. Is the backend running?')
    } finally {
      setCreating(false)
    }
  }

  const handleJoin = (e: React.FormEvent) => {
    e.preventDefault()
    if (joinId.trim()) {
      navigate(`/room/${joinId.trim()}`)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-sky-50 to-indigo-50 p-6">
      <div className="glass-surface w-full max-w-md rounded-[28px] p-8 text-center">
        <h1 className="mb-2 text-2xl font-bold text-slate-900">DropVault</h1>
        <p className="mb-8 text-sm text-slate-600">File sharing, async dropbox, and video in one room.</p>

        <button
          onClick={handleCreate}
          disabled={creating}
          className="mb-6 w-full rounded-2xl bg-sky-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:opacity-60"
        >
          {creating ? 'Creating room…' : 'Create a new room'}
        </button>

        <div className="mb-6 flex items-center gap-3 text-xs text-slate-400">
          <div className="h-px flex-1 bg-slate-200" />
          OR
          <div className="h-px flex-1 bg-slate-200" />
        </div>

        <form onSubmit={handleJoin} className="flex gap-2">
          <input
            value={joinId}
            onChange={(e) => setJoinId(e.target.value)}
            placeholder="Paste a room ID"
            className="flex-1 rounded-2xl border border-slate-200 px-4 py-3 text-sm outline-none focus:border-sky-400"
          />
          <button
            type="submit"
            className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-slate-800 transition hover:bg-slate-50"
          >
            Join
          </button>
        </form>

        {error && <p className="mt-4 text-sm text-red-600">{error}</p>}
      </div>
    </div>
  )
}
