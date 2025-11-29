package checkpoint

import "context"

type Store interface {
	Get(ctx context.Context, streamName, shardID string) (string, error)
	Save(ctx context.Context, streamName, shardID, sequenceNumber string) error
	Delete(ctx context.Context, streamName, shardID string) error
}
