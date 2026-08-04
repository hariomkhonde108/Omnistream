package domain

import "time"

// UploadedPart represents one chunk of a resumable upload that has been
// successfully received and stored as its own small object in MinIO
// (separate from the file's final object). PartKey points at that
// standalone object.
//
// This is the record a client's GET /uploads/:fileId/status call is built
// from: it tells the client exactly which part numbers already exist, so a
// resumed upload only needs to (re-)send whatever's missing.
type UploadedPart struct {
	FileID     string
	PartNumber int
	PartKey    string
	Size       int64
	UploadedAt time.Time
}
