package consumer

import "testing"

func TestShardCompletionValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seq  string
		want string
	}{
		{name: "empty", seq: "", want: "SHARD_END"},
		{name: "sequence", seq: "sequence-1", want: "SHARD_END:sequence-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := shardCompletionValue(tt.seq)
			if got != tt.want {
				t.Fatalf("shardCompletionValue(%q) = %q, want %q", tt.seq, got, tt.want)
			}
		})
	}
}

func TestIsShardCompletedCheckpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seq  string
		want bool
	}{
		{name: "empty", seq: "", want: false},
		{name: "marker", seq: "SHARD_END", want: true},
		{name: "marker with sequence", seq: "SHARD_END:sequence-1", want: true},
		{name: "other checkpoint", seq: "sequence-1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := isShardCompletedCheckpoint(tt.seq)
			if got != tt.want {
				t.Fatalf("isShardCompletedCheckpoint(%q) = %v, want %v", tt.seq, got, tt.want)
			}
		})
	}
}
