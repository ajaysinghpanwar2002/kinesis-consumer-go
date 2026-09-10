// Package layout checks namespaces before opening a v3 backend.
package layout

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	valkey "github.com/valkey-io/valkey-go"
)

// ErrIncompatible identifies state that requires an explicit offline reset or reconciliation.
var ErrIncompatible = errors.New("incompatible Valkey layout")

// Check scans every node because old checkpoint keys did not share a cluster slot.
// Consumers must be stopped during upgrades; mixed-version writers are unsupported.
func Check(ctx context.Context, client valkey.Client, prefixes ...string) error {
	escapedPrefixes := make([]string, len(prefixes))
	escaper := strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D")
	for i, prefix := range prefixes {
		escapedPrefixes[i] = escaper.Replace(prefix)
	}
	nodes := client.Nodes()
	if len(nodes) == 0 {
		nodes = map[string]valkey.Client{"": client}
	}
	for _, node := range nodes {
		var cursor uint64
		for {
			page, err := node.Do(ctx, node.B().Scan().Cursor(cursor).Count(1000).Build()).AsScanEntry()
			if err != nil {
				return fmt.Errorf("check Valkey layout: %w", err)
			}
			for _, key := range page.Elements {
				// Check every configured namespace before rejecting a broader prefix:
				// checkpoint "tenant" and lease "tenant:leases" may be nested.
				valid := false
				for _, prefix := range escapedPrefixes {
					if isV3Key(key, prefix) || isNestedV3Key(key, prefix) {
						valid = true
						break
					}
				}
				if valid {
					continue
				}
				for i, prefix := range prefixes {
					if strings.HasPrefix(key, escapedPrefixes[i]+":") || strings.HasPrefix(key, prefix+":") {
						return fmt.Errorf("%w: namespace %q contains non-v3 state", ErrIncompatible, prefix)
					}
				}
			}
			cursor = page.Cursor
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}

// isV3Key requires the complete injective encoding, not just a version prefix:
// a legacy raw checkpoint may itself have consumer group "v3".
func isV3Key(key, prefix string) bool {
	rest, ok := strings.CutPrefix(key, prefix+":v3:{")
	if !ok {
		return false
	}
	identity, suffix, ok := strings.Cut(rest, "}")
	if !ok || (identity != "-" && (identity == "" || !canonicalBase64(identity))) {
		return false
	}
	switch suffix {
	case ":lease-owners", ":lease-expirations", ":lease-generations", ":workers", ":recovery":
		return true
	}
	shard, ok := strings.CutPrefix(suffix, ":recovery:")
	if !ok {
		return false
	}
	shard = strings.TrimSuffix(shard, ":initial")
	return canonicalBase64(shard)
}

func canonicalBase64(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && base64.RawURLEncoding.EncodeToString(decoded) == value
}

// A standalone manager may not know a store's nested checkpoint prefix. Valid
// v3 namespaces remain compatible even when only their parent is configured.
func isNestedV3Key(key, prefix string) bool {
	nested, _, ok := strings.Cut(key, ":v3:{")
	if !ok || !strings.HasPrefix(nested, prefix+":") {
		return false
	}
	decoded := strings.NewReplacer("%25", "%", "%7B", "{", "%7D", "}").Replace(nested)
	encoded := strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D").Replace(decoded)
	return encoded == nested && isV3Key(key, nested)
}
