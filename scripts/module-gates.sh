#!/usr/bin/env bash
set -euo pipefail

# Module hygiene gates that must ignore the go.work workspace overrides.
#
#   scripts/module-gates.sh tidy-check   # `go mod tidy -diff` per module
#   scripts/module-gates.sh gowork-off   # GOWORK=off build+test of pkg/backend/valkey
#
# Both gates exercise each module the way a third party consumes it: without
# the workspace replaces, so intra-repo requires resolve against PUBLISHED
# tags. Until the first release exists, an intra-repo require (currently
# v0.1.0) is not resolvable from any proxy, and module-scoped commands fail
# with "unknown revision" through no fault of the checked-in files.
#
# Skipping is BOOTSTRAP-ONLY. The gates first ask the git remote whether any
# release tag (vX.Y.Z or pkg/backend/valkey/vX.Y.Z) exists:
#
#   - no release tags yet (pre-first-release) -> a module pinning an
#     unresolvable intra-repo version is SKIPPED with a loud notice; the
#     skip disappears on its own once the first tags are pushed.
#   - release tags exist -> nothing is ever skipped. A pinned intra-repo
#     version that does not resolve is a hard failure (release-candidate
#     drift: pins bumped ahead of tags), as is tidy drift, a missing go.sum
#     entry (expected on the first post-release run until `make tidy` is
#     committed), or genuine tag/format skew between the published modules.
#
# The published-version probe runs against a FRESH GOMODCACHE: a developer
# machine's module cache can contain versions whose tags no longer exist on
# the remote (observed here: phantom v1.x entries from deleted tags), and a
# stale cache hit must not make an unpublished pin look published. Probe or
# ls-remote failures that do NOT look like an unpublished version (network,
# auth, malformed go.mod) fail the gate rather than skip, so an outage can
# never silently pass as "pre-release".

MODULE_PREFIX="github.com/ajaysinghpanwar2002/kinesis-consumer-go"
MODULES=(. pkg/backend/valkey examples/valkey test/integration)

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

usage() {
  echo "usage: $0 tidy-check|gowork-off" >&2
  exit 2
}

[ $# -eq 1 ] || usage
MODE="$1"

# intra_requires <module-dir>
# Prints "modpath version" for every intra-repo module the module requires.
intra_requires() {
  awk -v prefix="$MODULE_PREFIX" '
    /^require[ \t]*\(/ { inblk = 1; next }
    /^\)/              { inblk = 0 }
    {
      if ($1 == "require" && index($2, prefix) == 1) { print $2, $3 }
      else if (inblk && index($1, prefix) == 1)      { print $1, $2 }
    }' "$1/go.mod"
}

# release_tags_exist
# Returns 0 when the git remote has at least one release tag for either
# published module; 1 when it has none (the bootstrap state). Exits nonzero
# when the remote cannot be queried — the bootstrap decision must never be
# guessed from a network failure.
RELEASE_TAGS_STATE=""
release_tags_exist() {
  local out
  if [ -z "$RELEASE_TAGS_STATE" ]; then
    if ! out="$(git ls-remote --tags origin 'refs/tags/v[0-9]*' 'refs/tags/pkg/backend/valkey/v[0-9]*' 2>&1)"; then
      echo "ERROR: could not query release tags on 'origin' (needed to decide whether pre-release skips are allowed):" >&2
      printf '%s\n' "$out" | sed 's/^/  /' >&2
      exit 1
    fi
    if [ -n "$out" ]; then RELEASE_TAGS_STATE="yes"; else RELEASE_TAGS_STATE="no"; fi
  fi
  [ "$RELEASE_TAGS_STATE" = "yes" ]
}

# probe_published <modpath> <version>
# Returns 0 when the version resolves, 1 when it is unpublished, and exits
# nonzero on any other probe failure. Runs with a fresh GOMODCACHE so a stale
# local cache entry for a deleted tag cannot make a version look published.
PROBE_DIR=""
probe_published() {
  local modpath="$1" version="$2" out
  if [ -z "$PROBE_DIR" ]; then
    PROBE_DIR="$(mktemp -d)"
    trap 'rm -rf "$PROBE_DIR"' EXIT
    mkdir -p "$PROBE_DIR/mod" "$PROBE_DIR/modcache"
    ( cd "$PROBE_DIR/mod" && go mod init probe >/dev/null 2>&1 )
  fi
  if out="$(cd "$PROBE_DIR/mod" && GOWORK=off GOFLAGS= GOMODCACHE="$PROBE_DIR/modcache" go list -m "$modpath@$version" 2>&1)"; then
    return 0
  fi
  # Only the two failure shapes that specifically mean "this version has no
  # published tag" count as unpublished: a direct-VCS lookup that finds the
  # repo but not the revision, or a proxy 404/410 for the version files.
  # Everything else (auth, network, nonexistent repo, malformed pin) falls
  # through to the hard failure below.
  case "$out" in
    *"unknown revision"*|*"404 Not Found"*|*"410 Gone"*)
      return 1 ;;
    *)
      echo "ERROR: could not probe $modpath@$version (not an 'unpublished' failure):" >&2
      printf '%s\n' "$out" | sed 's/^/  /' >&2
      exit 1 ;;
  esac
}

# published_state <module-dir>
# Echoes "ok" when every intra-repo require is published. For an unpublished
# require: echoes "bootstrap:<modpath>@<version>" when no release tags exist
# yet (skipping allowed), and FAILS the gate when release tags exist — pins
# bumped ahead of published tags are drift, never a skip.
published_state() {
  local modpath version
  while read -r modpath version; do
    [ -n "$modpath" ] || continue
    if ! probe_published "$modpath" "$version"; then
      if release_tags_exist; then
        echo "ERROR: $1/go.mod pins $modpath@$version, which is not published, but release tags already exist on 'origin'." >&2
        echo "       Publish the matching tags (scripts/release.sh) or fix the pin; this is never skippable post-first-release." >&2
        exit 1
      fi
      echo "bootstrap:$modpath@$version"
      return 0
    fi
  done < <(intra_requires "$1")
  echo "ok"
}

case "$MODE" in
  tidy-check)
    status=0
    for m in "${MODULES[@]}"; do
      state="$(published_state "$m")" || exit 1
      if [ "$state" != "ok" ]; then
        echo "==> SKIP go mod tidy -diff $m (requires unpublished ${state#bootstrap:}; no release tags exist yet)"
        continue
      fi
      echo "==> go mod tidy -diff $m"
      ( cd "$m" && GOWORK=off go mod tidy -diff ) || status=1
    done
    exit "$status"
    ;;

  gowork-off)
    BACKEND="pkg/backend/valkey"
    state="$(published_state "$BACKEND")" || exit 1
    if [ "$state" != "ok" ]; then
      echo "==> SKIP GOWORK=off gate for $BACKEND (requires unpublished ${state#bootstrap:}; no release tags exist yet)"
      exit 0
    fi
    echo "==> GOWORK=off go build/test $BACKEND against pinned core version"
    ( cd "$BACKEND" && GOWORK=off go build ./... && GOWORK=off go test ./... )
    ;;

  *) usage ;;
esac
