import { useState } from 'react'

interface DropZoneProps {
  onFilesSelected: (files: File[]) => void
  disabled?: boolean
}

export function DropZone({ onFilesSelected, disabled }: DropZoneProps) {
  const [isDragging, setIsDragging] = useState(false)

  return (
    <div
      onDragOver={(e) => {
        e.preventDefault()
        setIsDragging(true)
      }}
      onDragLeave={(e) => {
        e.preventDefault()
        setIsDragging(false)
      }}
      onDrop={(e) => {
        e.preventDefault()
        setIsDragging(false)
        if (disabled) return
        const files = Array.from(e.dataTransfer.files)
        if (files.length) onFilesSelected(files)
      }}
      onClick={() => {
        if (disabled) return
        document.getElementById('dropvault-file-input')?.click()
      }}
      className={`flex h-32 w-full cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed text-center transition-colors ${
        isDragging ? 'border-sky-500 bg-sky-50' : 'border-slate-300 bg-white/60'
      } ${disabled ? 'cursor-not-allowed opacity-50' : ''}`}
    >
      <p className="text-sm font-medium text-slate-700">
        {disabled ? 'Uploading…' : 'Drop a file here, or click to browse'}
      </p>
      <input
        id="dropvault-file-input"
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          if (e.target.files) onFilesSelected(Array.from(e.target.files))
          e.target.value = '' // allow re-selecting the same file later
        }}
      />
    </div>
  )
}
