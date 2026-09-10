package layout

import "testing"

func TestV3KeyStructure(t *testing.T) {
	for _, suffix := range []string{"lease-owners", "lease-expirations", "lease-generations", "workers", "recovery", "recovery:c2hhcmQ", "recovery:c2hhcmQ:initial", "recovery:", "recovery::initial"} {
		if !isV3Key("prefix:v3:{c3RyZWFt}:"+suffix, "prefix") {
			t.Errorf("rejected suffix %s", suffix)
		}
	}
	if !isV3Key("prefix:v3:{-}:recovery:c2hhcmQ", "prefix") {
		t.Error("empty identity rejected")
	}
	for _, key := range []string{
		"prefix:v3:orders:shard-000", "prefix:v3:{}:recovery:c2hhcmQ",
		"prefix:v3:{c3RyZWFt=}:recovery:c2hhcmQ", "prefix:v3:{c3RyZWFt}:unknown",
		"prefix:v3:{c3RyZWFt}:recovery:c2hhcmQ:extra", "prefix:v3:{c3RyZWFt}:recovery:c2hhcmR",
		"prefix:v3:{c3RyZWFt}:recovery:shard!", "prefix:v3:{c3RyZWFt}:workers:extra",
	} {
		if isV3Key(key, "prefix") {
			t.Errorf("accepted malformed key %s", key)
		}
	}
}

func TestNestedNamespaceStillRequiresV3Structure(t *testing.T) {
	if !isNestedV3Key("tenant:checkpoints:v3:{c3RyZWFt}:recovery:c2hhcmQ", "tenant") {
		t.Fatal("valid nested namespace rejected")
	}
	for _, key := range []string{"tenant:v3:orders:shard", "tenant:checkpoints:v2:c3RyZWFt:c2hhcmQ", "tenant:checkpoints:v3:{bad}:garbage", "tenant:{raw}:v3:{c3RyZWFt}:recovery:c2hhcmQ", "tenant:%raw:v3:{c3RyZWFt}:recovery:c2hhcmQ"} {
		if isNestedV3Key(key, "tenant") {
			t.Errorf("legacy or malformed nested key accepted: %s", key)
		}
	}
}
