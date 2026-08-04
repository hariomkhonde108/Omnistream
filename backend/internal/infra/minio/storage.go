package minio

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storage struct {
	client *minio.Client
	bucket string
}

func NewStorage(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Storage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &Storage{client: client, bucket: bucket}, nil
}

// PutObjectStream writes directly from a reader — the ingestion service
// hands this an in-flight upload stream, so bytes flow client -> ingestion
// -> MinIO without ever being fully buffered in the Go process's memory.
// This is the same backpressure-aware principle as the OPFS work from the
// original P2P project, just applied server-side instead of in the browser.
func (s *Storage) PutObjectStream(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", key, err)
	}
	return nil
}

// GetObjectStream returns a reader a worker or delivery handler can stream
// from — again, no full-file buffering required on the reading side either.
func (s *Storage) GetObjectStream(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	return obj, nil
}
// GetObjectStreamRange returns a reader starting at byte `start`, ending at
// byte `end` (inclusive) — the server-side half of HTTP Range support. If
// end is -1, it means "read to the end of the object" (an open-ended range,
// e.g. "bytes=5000000-", which is what a resuming download client sends
// when it already has the first 5MB and wants the rest).
func (s *Storage) GetObjectStreamRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}

	var err error
	if end < 0 {
		err = opts.SetRange(start, 0) // 0 as the end param means "to EOF" in minio-go's SetRange
	} else {
		err = opts.SetRange(start, end)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to set range for %s: %w", key, err)
	}

	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get object range %s: %w", key, err)
	}
	return obj, nil
}

func (s *Storage) DeleteObject(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}
	return nil
}
