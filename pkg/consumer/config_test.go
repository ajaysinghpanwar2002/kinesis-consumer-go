package consumer

import (
	"strings"
	"testing"
	"time"
)

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{StreamName: "stream", ConsumerGroup: "group"}.withDefaults()
	if cfg.StartPosition != StartLatest {
		t.Fatalf("StartPosition = %q, want %q", cfg.StartPosition, StartLatest)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	timestamp := time.Unix(1700000000, 0)

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "missing stream identity",
			cfg:  Config{ConsumerGroup: "group"},
			want: "exactly one of stream name or ARN is required",
		},
		{
			name: "both stream identities",
			cfg: Config{
				StreamName:    "stream",
				StreamARN:     "arn:aws:kinesis:ap-south-1:123456789012:stream/demo",
				ConsumerGroup: "group",
			},
			want: "exactly one of stream name or ARN is required",
		},
		{
			name: "missing consumer group",
			cfg:  Config{StreamName: "stream"},
			want: "consumer group is required",
		},
		{
			name: "invalid consumer group",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: "orders:blue",
			},
			want: "consumer group must contain only letters, numbers, '.', '_', or '-'",
		},
		{
			name: "invalid stream ARN",
			cfg: Config{
				StreamARN:     "not-an-arn",
				ConsumerGroup: "group",
			},
			want: "invalid stream ARN: arn: invalid prefix",
		},
		{
			name: "non Kinesis stream ARN",
			cfg: Config{
				StreamARN:     "arn:aws:s3:::bucket",
				ConsumerGroup: "group",
			},
			want: "stream ARN must be a Kinesis stream ARN",
		},
		{
			name: "missing ARN partition region and account",
			cfg: Config{
				StreamARN:     "arn::kinesis:::stream/orders",
				ConsumerGroup: "group",
			},
			want: "stream ARN must include a partition, region, and 12-digit account ID",
		},
		{
			name: "invalid ARN account",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:us-east-1:not-an-account:stream/orders",
				ConsumerGroup: "group",
			},
			want: "stream ARN must include a partition, region, and 12-digit account ID",
		},
		{
			name: "consumer group at maximum length",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: strings.Repeat("g", identitySegmentMaxLength),
			},
		},
		{
			name: "consumer group above maximum length",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: strings.Repeat("g", identitySegmentMaxLength+1),
			},
			want: "consumer group must be at most 128 characters",
		},
		{
			name: "stream name at maximum length",
			cfg: Config{
				StreamName:    strings.Repeat("s", identitySegmentMaxLength),
				ConsumerGroup: "group",
			},
		},
		{
			name: "stream name above maximum length",
			cfg: Config{
				StreamName:    strings.Repeat("s", identitySegmentMaxLength+1),
				ConsumerGroup: "group",
			},
			want: "stream name must be at most 128 characters",
		},
		{
			name: "stream name contains unsafe character",
			cfg: Config{
				StreamName:    "orders:blue",
				ConsumerGroup: "group",
			},
			want: "stream name must contain only letters, numbers, '.', '_', or '-'",
		},
		{
			name: "stream name in ARN at maximum length",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:us-east-1:123456789012:stream/" + strings.Repeat("s", identitySegmentMaxLength),
				ConsumerGroup: "group",
			},
		},
		{
			name: "stream name in ARN above maximum length",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:us-east-1:123456789012:stream/" + strings.Repeat("s", identitySegmentMaxLength+1),
				ConsumerGroup: "group",
			},
			want: "stream name in ARN must be at most 128 characters",
		},
		{
			name: "stream name in ARN contains unsafe character",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:us-east-1:123456789012:stream/orders/blue",
				ConsumerGroup: "group",
			},
			want: "stream name in ARN must contain only letters, numbers, '.', '_', or '-'",
		},
		{
			name: "invalid start position",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: "group",
				StartPosition: "EARLIEST",
			},
			want: `invalid start position "EARLIEST"`,
		},
		{
			name: "timestamp required for at timestamp",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: "group",
				StartPosition: StartAtTimestamp,
			},
			want: "start timestamp is required when using AT_TIMESTAMP",
		},
		{
			name: "stream name only",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: "group",
				StartPosition: StartLatest,
			},
		},
		{
			name: "stream ARN only",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:ap-south-1:123456789012:stream/demo",
				ConsumerGroup: "group",
				StartPosition: StartTrimHorizon,
			},
		},
		{
			name: "at timestamp with timestamp",
			cfg: Config{
				StreamName:     "stream",
				ConsumerGroup:  "group",
				StartPosition:  StartAtTimestamp,
				StartTimestamp: &timestamp,
			},
		},
		{
			name: "empty start position allowed before defaults",
			cfg: Config{
				StreamName:    "stream",
				ConsumerGroup: "group",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("validate() error = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("validate() error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestConfigValidateAndResolveStreamName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "name only",
			cfg:  Config{StreamName: "orders", ConsumerGroup: "billing"},
			want: "orders",
		},
		{
			name: "ARN only",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:ap-south-1:123456789012:stream/orders",
				ConsumerGroup: "billing",
			},
			want: "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.cfg.validateAndResolveStreamName()
			if err != nil {
				t.Fatalf("validateAndResolveStreamName() error = %v, want nil", err)
			}
			if got != tt.want {
				t.Fatalf("validateAndResolveStreamName() = %q, want %q", got, tt.want)
			}
		})
	}
}
