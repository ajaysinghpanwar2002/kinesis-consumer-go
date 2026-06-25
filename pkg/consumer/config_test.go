package consumer

import (
	"testing"
	"time"
)

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{StreamName: "stream"}.withDefaults()
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
			cfg:  Config{},
			want: "stream name or ARN is required",
		},
		{
			name: "invalid start position",
			cfg: Config{
				StreamName:    "stream",
				StartPosition: "EARLIEST",
			},
			want: `invalid start position "EARLIEST"`,
		},
		{
			name: "timestamp required for at timestamp",
			cfg: Config{
				StreamName:    "stream",
				StartPosition: StartAtTimestamp,
			},
			want: "start timestamp is required when using AT_TIMESTAMP",
		},
		{
			name: "stream name only",
			cfg: Config{
				StreamName:    "stream",
				StartPosition: StartLatest,
			},
		},
		{
			name: "stream ARN only",
			cfg: Config{
				StreamARN:     "arn:aws:kinesis:ap-south-1:123456789012:stream/demo",
				StartPosition: StartTrimHorizon,
			},
		},
		{
			name: "stream name and ARN",
			cfg: Config{
				StreamName:    "stream",
				StreamARN:     "arn:aws:kinesis:ap-south-1:123456789012:stream/demo",
				StartPosition: StartLatest,
			},
		},
		{
			name: "at timestamp with timestamp",
			cfg: Config{
				StreamName:     "stream",
				StartPosition:  StartAtTimestamp,
				StartTimestamp: &timestamp,
			},
		},
		{
			name: "empty start position allowed before defaults",
			cfg: Config{
				StreamName: "stream",
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
