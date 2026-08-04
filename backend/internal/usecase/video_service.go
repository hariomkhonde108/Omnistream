package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"omnistream/internal/domain"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type VideoEventRepository interface {
	Save(ctx context.Context, e *domain.VideoEvent) error
	ListForRoom(ctx context.Context, roomID string) ([]*domain.VideoEvent, error)
}

type RecordingRepository interface {
	Save(ctx context.Context, rec *domain.Recording) error
	Get(ctx context.Context, egressID string) (*domain.Recording, error)
	MarkComplete(ctx context.Context, egressID string, durationSeconds, sizeBytes int64) error
	MarkFailed(ctx context.Context, egressID string) error
	GetActiveByRoom(ctx context.Context, roomID string) (*domain.Recording, error)
}

type KafkaPublisher interface {
	Publish(ctx context.Context, key string, event any) error
}

// S3RecordingConfig holds what's needed to tell LiveKit's Egress where to
// upload finished recordings. Endpoint MUST be reachable from the Egress
// CONTAINER, not the host — that's a Docker-internal address
// (http://minio:9000), different from the MinioEndpoint your Go processes
// use directly (localhost:9000).
type S3RecordingConfig struct {
	AccessKey string
	Secret    string
	Endpoint  string
	Bucket    string
}

const tokenTTL = 6 * time.Hour

type VideoService struct {
	apiKey                 string
	apiSecret              string
	videoEventRepo         VideoEventRepository
	recordingRepo          RecordingRepository
	egressClient           *lksdk.EgressClient
	s3Config               S3RecordingConfig
	recordingReadyProducer KafkaPublisher
	recordingReadyTopic    string
}

func NewVideoService(
	apiKey, apiSecret string,
	videoEventRepo VideoEventRepository,
	recordingRepo RecordingRepository,
	livekitURL string,
	s3Config S3RecordingConfig,
	recordingReadyProducer KafkaPublisher,
	recordingReadyTopic string,
) *VideoService {
	// EgressClient's REST/twirp API expects http(s)://, but LIVEKIT_URL is
	// stored as ws(s):// for the browser client's benefit — converting
	// locally rather than assuming an SDK helper exists for this, since
	// that wasn't confirmed during the API inspection.
	httpURL := strings.Replace(livekitURL, "ws://", "http://", 1)
	httpURL = strings.Replace(httpURL, "wss://", "https://", 1)

	return &VideoService{
		apiKey:                 apiKey,
		apiSecret:              apiSecret,
		videoEventRepo:         videoEventRepo,
		recordingRepo:          recordingRepo,
		egressClient:           lksdk.NewEgressClient(httpURL, apiKey, apiSecret),
		s3Config:               s3Config,
		recordingReadyProducer: recordingReadyProducer,
		recordingReadyTopic:    recordingReadyTopic,
	}
}

func (s *VideoService) GenerateJoinToken(roomName, participantID string, name string) (string, error) {
	at := auth.NewAccessToken(s.apiKey, s.apiSecret)
	grant := &auth.VideoGrant{RoomJoin: true, Room: roomName}
	at.AddGrant(grant).SetIdentity(participantID).SetValidFor(tokenTTL).SetName(name)
	token, err := at.ToJWT()
	if err != nil {
		return "", fmt.Errorf("failed to generate video token: %w", err)
	}
	return token, nil
}

// StartRecording begins an audio-only room composite recording — audio-
// only because that's all the notes pipeline needs; smaller files, faster
// upload, no wasted video encoding. storageKey is decided HERE,
// deterministically, before LiveKit starts anything — this is what lets
// the webhook handler avoid depending on parsing LiveKit's own response
// for the file's eventual location.
func (s *VideoService) StartRecording(ctx context.Context, roomID string) (*domain.Recording, error) {
	storageKey := fmt.Sprintf("%s/recording-%d.ogg", roomID, time.Now().Unix())

	req := &livekit.RoomCompositeEgressRequest{
		RoomName:  roomID,
		AudioOnly: true,
		FileOutputs: []*livekit.EncodedFileOutput{
			{
				FileType: livekit.EncodedFileType_OGG,
				Filepath: storageKey,
				Output: &livekit.EncodedFileOutput_S3{
					S3: &livekit.S3Upload{
						AccessKey:      s.s3Config.AccessKey,
						Secret:         s.s3Config.Secret,
						Region:         "us-east-1", // MinIO ignores region, but the client requires a non-empty value
						Endpoint:       s.s3Config.Endpoint,
						Bucket:         s.s3Config.Bucket,
						ForcePathStyle: true, // required for MinIO — it doesn't support virtual-hosted-style bucket URLs
					},
				},
			},
		},
	}

	info, err := s.egressClient.StartRoomCompositeEgress(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start egress: %w", err)
	}

	rec := domain.NewRecording(info.EgressId, roomID, storageKey)
	if err := s.recordingRepo.Save(ctx, rec); err != nil {
		return nil, fmt.Errorf("failed to save recording record: %w", err)
	}

	return rec, nil
}

func (s *VideoService) HandleWebhookEvent(ctx context.Context, event *livekit.WebhookEvent) error {
	roomID := ""
	if event.Room != nil {
		roomID = event.Room.Name
	}
	participantID := ""
	if event.Participant != nil {
		participantID = event.Participant.Identity
	}

	if roomID != "" {
		e := &domain.VideoEvent{
			RoomID:        roomID,
			EventType:     event.Event,
			ParticipantID: participantID,
			OccurredAt:    time.Now(),
		}
		if err := s.videoEventRepo.Save(ctx, e); err != nil {
			return fmt.Errorf("failed to save video event: %w", err)
		}
	}

	if event.EgressInfo != nil {
		return s.handleEgressWebhook(ctx, event.Event, event.EgressInfo)
	}

	return nil
}

func (s *VideoService) handleEgressWebhook(ctx context.Context, eventType string, info *livekit.EgressInfo) error {
	if eventType != "egress_ended" {
		return nil
	}

	if info.Status != livekit.EgressStatus_EGRESS_COMPLETE {
		if err := s.recordingRepo.MarkFailed(ctx, info.EgressId); err != nil {
			return fmt.Errorf("failed to mark recording failed: %w", err)
		}
		return nil
	}

	var duration, size int64
	if len(info.FileResults) > 0 {
		duration = info.FileResults[0].Duration
		size = info.FileResults[0].Size
	}
	if err := s.recordingRepo.MarkComplete(ctx, info.EgressId, duration, size); err != nil {
		return fmt.Errorf("failed to mark recording complete: %w", err)
	}

	rec, err := s.recordingRepo.Get(ctx, info.EgressId)
	if err != nil {
		return fmt.Errorf("failed to reload recording after marking complete: %w", err)
	}
	if rec == nil {
		return fmt.Errorf("recording %s not found after marking complete", info.EgressId)
	}

	if s.recordingReadyProducer != nil && s.recordingReadyTopic != "" {
		readyEvent := domain.RecordingReadyEvent{
			EgressID:   rec.EgressID,
			RoomID:     rec.RoomID,
			StorageKey: rec.StorageKey,
		}
		if err := s.recordingReadyProducer.Publish(ctx, rec.RoomID, readyEvent); err != nil {
			return fmt.Errorf("failed to publish recording-ready event: %w", err)
		}
	}

	return nil
}

func (s *VideoService) ListEventsForRoom(ctx context.Context, roomID string) ([]*domain.VideoEvent, error) {
	events, err := s.videoEventRepo.ListForRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("failed to list video events: %w", err)
	}
	return events, nil
}

func (s *VideoService) StopRecording(ctx context.Context, roomID string) error {
	rec, err := s.recordingRepo.GetActiveByRoom(ctx, roomID)
	if err != nil {
		return fmt.Errorf("failed to find active recording: %w", err)
	}
	if rec == nil {
		return fmt.Errorf("no active recording for room %s", roomID)
	}

	_, err = s.egressClient.StopEgress(ctx, &livekit.StopEgressRequest{EgressId: rec.EgressID})
	if err != nil {
		return fmt.Errorf("failed to stop egress: %w", err)
	}
	return nil
}