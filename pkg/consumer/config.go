package consumer

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

const identitySegmentMaxLength = 128

var identitySegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// StartPosition controls where consumption starts for a parentless shard with
// no checkpoint. Two cases always override it: a shard with a checkpoint
// resumes strictly after it, and a checkpoint-less shard with a parent in the
// known shard listing (a reshard child) starts from TRIM_HORIZON so records
// produced between the reshard and pickup are never skipped.
type StartPosition string

const (
	StartLatest      StartPosition = "LATEST"
	StartTrimHorizon StartPosition = "TRIM_HORIZON"
	StartAtTimestamp StartPosition = "AT_TIMESTAMP"
)

// Config contains the required stream and consumer-group identities plus the
// initial position settings.
type Config struct {
	// StreamName identifies the Kinesis stream by name. Set exactly one of
	// StreamName and StreamARN.
	StreamName string
	// StreamARN identifies the Kinesis stream by ARN. Set exactly one of
	// StreamName and StreamARN.
	StreamARN string
	// ConsumerGroup identifies the logical application sharing checkpoints and
	// shard ownership. Workers consume independently when their groups differ.
	ConsumerGroup  string
	StartPosition  StartPosition
	StartTimestamp *time.Time
}

func (c Config) withDefaults() Config {
	if c.StartPosition == "" {
		c.StartPosition = StartLatest
	}
	return c
}

func (c Config) validate() error {
	_, err := c.validateAndResolveStreamName()
	return err
}

func (c Config) validateAndResolveStreamName() (string, error) {
	if (c.StreamName == "") == (c.StreamARN == "") {
		return "", errors.New("exactly one of stream name or ARN is required")
	}
	if c.ConsumerGroup == "" {
		return "", errors.New("consumer group is required")
	}
	if err := validateIdentitySegment("consumer group", c.ConsumerGroup); err != nil {
		return "", err
	}
	streamName, err := resolveCanonicalStreamName(c)
	if err != nil {
		return "", err
	}

	switch c.StartPosition {
	case "", StartLatest, StartTrimHorizon, StartAtTimestamp:
	default:
		return "", fmt.Errorf("invalid start position %q", c.StartPosition)
	}

	if c.StartPosition == StartAtTimestamp && c.StartTimestamp == nil {
		return "", errors.New("start timestamp is required when using AT_TIMESTAMP")
	}

	return streamName, nil
}

func resolveCanonicalStreamName(cfg Config) (string, error) {
	if cfg.StreamName != "" {
		if err := validateIdentitySegment("stream name", cfg.StreamName); err != nil {
			return "", err
		}
		return cfg.StreamName, nil
	}

	parsed, err := arn.Parse(cfg.StreamARN)
	if err != nil {
		return "", fmt.Errorf("invalid stream ARN: %w", err)
	}
	const resourcePrefix = "stream/"
	if parsed.Service != "kinesis" || !strings.HasPrefix(parsed.Resource, resourcePrefix) {
		return "", errors.New("stream ARN must be a Kinesis stream ARN")
	}
	if parsed.Partition == "" || parsed.Region == "" || !validAWSAccountID(parsed.AccountID) {
		return "", errors.New("stream ARN must include a partition, region, and 12-digit account ID")
	}
	streamName := strings.TrimPrefix(parsed.Resource, resourcePrefix)
	if err := validateIdentitySegment("stream name in ARN", streamName); err != nil {
		return "", err
	}
	return streamName, nil
}

func validAWSAccountID(accountID string) bool {
	if len(accountID) != 12 {
		return false
	}
	for _, char := range accountID {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func validateIdentitySegment(field, value string) error {
	if len(value) > identitySegmentMaxLength {
		return fmt.Errorf("%s must be at most %d characters", field, identitySegmentMaxLength)
	}
	if !identitySegmentPattern.MatchString(value) {
		return fmt.Errorf("%s must contain only letters, numbers, '.', '_', or '-'", field)
	}
	return nil
}
