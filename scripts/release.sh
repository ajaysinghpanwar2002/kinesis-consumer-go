#!/usr/bin/env bash
set -euo pipefail

# Release helper for the two published modules in this repository:
#
#   1. the core library      -> tag  vX.Y.Z
#   2. the Valkey backend     -> tag  pkg/backend/valkey/vX.Y.Z
#
# Go identifies a submodule's version by a tag whose prefix is the submodule's
# path relative to the repo root. Both tags point at the SAME commit, so a
# consumer's `go get .../pkg/backend/valkey@vX.Y.Z` resolves that module's
# `require .../kinesis-consumer-go vX.Y.Z` against the core tag published here.
#
# Usage:
#   scripts/release.sh vX.Y.Z [--dry-run] [--remote <name>]
#
# The committed source must already pin the backend module's parent require at
# the version being released (this script verifies it and refuses otherwise).

usage() {
  echo "usage: $0 vX.Y.Z [--dry-run] [--remote <name>]" >&2
  exit 2
}

VERSION=""
DRY_RUN=0
REMOTE="origin"

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --remote)
      REMOTE="${2:-}"
      # Reject a missing value or one that looks like the next option, so
      # `--remote --dry-run` fails loudly instead of eating the --dry-run flag.
      case "$REMOTE" in
        ""|-*) echo "ERROR: --remote requires a non-option value" >&2; usage ;;
      esac
      shift 2 ;;
    -h|--help) usage ;;
    v*) [ -z "$VERSION" ] || usage; VERSION="$1"; shift ;;
    *) usage ;;
  esac
done

[ -n "$VERSION" ] || usage

# Semantic version, no pre-release/build metadata (keep releases simple).
if ! printf '%s' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "ERROR: version must look like vX.Y.Z (got: $VERSION)" >&2
  exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

CORE_TAG="$VERSION"
BACKEND_MODULE="pkg/backend/valkey"
BACKEND_TAG="$BACKEND_MODULE/$VERSION"

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN: $*"
  else
    echo "+ $*"
    "$@"
  fi
}

# --- Preconditions -----------------------------------------------------------

# Clean working tree: tags must capture exactly what is committed.
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: working tree is not clean; commit or stash before releasing." >&2
  exit 1
fi

# Neither tag may already exist locally.
for t in "$CORE_TAG" "$BACKEND_TAG"; do
  if git rev-parse -q --verify "refs/tags/$t" >/dev/null; then
    echo "ERROR: tag already exists locally: $t" >&2
    exit 1
  fi
done

# ...nor on the remote. A remote-only tag would collide on push, and because the
# two tags must be published as a matched pair, a partial collision must be
# caught before any tag is created.
if remote_refs="$(git ls-remote "$REMOTE" "refs/tags/$CORE_TAG" "refs/tags/$BACKEND_TAG" 2>/dev/null)"; then
  if [ -n "$remote_refs" ]; then
    echo "ERROR: a release tag already exists on remote '$REMOTE':" >&2
    printf '%s\n' "$remote_refs" | sed 's/^/  /' >&2
    exit 1
  fi
else
  msg="could not query tags on remote '$REMOTE' (is it configured and reachable?)"
  if [ "$DRY_RUN" = "1" ]; then
    echo "WARNING: $msg; skipping the remote tag check (dry run)." >&2
  else
    echo "ERROR: $msg" >&2
    exit 1
  fi
fi

# Every committed intra-repo version pin must already be at the version being
# released (docs/releasing.md step 2), or a consumer of a published tag would
# resolve a different (or missing) version — and local workspace builds would
# silently diverge from what the tags describe.

CORE_MODULE_PATH="github.com/ajaysinghpanwar2002/kinesis-consumer-go"
BACKEND_MODULE_PATH="$CORE_MODULE_PATH/pkg/backend/valkey"
PIN_ERRORS=0

# require_version <go.mod> <module-path>: prints the version a go.mod requires
# for the module, from either a one-line or a block require.
require_version() {
  awk -v mod="$2" '/^require[ \t]*\(/{inblk=1; next} /^\)/{inblk=0}
       $1=="require" && $2==mod {print $3; exit}
       inblk && $1==mod {print $2; exit}' "$1"
}

# check_require <go.mod> <module-path>
check_require() {
  local got
  got="$(require_version "$1" "$2")"
  if [ "$got" != "$VERSION" ]; then
    echo "ERROR: $1 requires $2 at '${got:-<none>}', not $VERSION." >&2
    PIN_ERRORS=1
  fi
}

# The backend's core require is what a consumer of the backend tag resolves.
check_require "$BACKEND_MODULE/go.mod" "$CORE_MODULE_PATH"

# The unpublished workspace modules must pin the same version, or local
# builds/tests after the release run against something other than what was
# tagged.
for gomod in examples/valkey/go.mod test/integration/go.mod; do
  check_require "$gomod" "$CORE_MODULE_PATH"
  check_require "$gomod" "$BACKEND_MODULE_PATH"
done

# The go.work replaces redirect exactly the pinned versions to the local
# checkout; a stale version there means the workspace stops overriding the
# new pins and every local build downloads the published modules instead.
# check_work_replace <module-path>
check_work_replace() {
  local got
  got="$(awk -v mod="$1" '$1=="replace" && $2==mod {print $3; exit}' go.work)"
  if [ "$got" != "$VERSION" ]; then
    echo "ERROR: go.work replace for $1 is at '${got:-<none>}', not $VERSION." >&2
    PIN_ERRORS=1
  fi
}
check_work_replace "$CORE_MODULE_PATH"
check_work_replace "$BACKEND_MODULE_PATH"

if [ "$PIN_ERRORS" != "0" ]; then
  echo "       Update the pins above to $VERSION (docs/releasing.md step 2), commit, then re-run." >&2
  exit 1
fi

# --- Tag and push ------------------------------------------------------------

echo "Releasing $VERSION"
echo "  core module   : tag $CORE_TAG"
echo "  valkey backend: tag $BACKEND_TAG"
echo "  remote        : $REMOTE"
[ "$DRY_RUN" = "1" ] && echo "  (dry run: no tags created or pushed)"
echo

run git tag -a "$CORE_TAG" -m "Release $CORE_TAG"
run git tag -a "$BACKEND_TAG" -m "Release $BACKEND_MODULE $VERSION"
# --atomic: publish both tags in one transaction, so the matched pair never
# ends up half-pushed if the remote rejects one ref.
run git push --atomic "$REMOTE" "$CORE_TAG" "$BACKEND_TAG"

echo
echo "Done. Verify third-party resolution once the remote has the tags:"
echo "  GOPROXY=proxy.golang.org GOFLAGS=-mod=mod \\"
echo "    go get github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey@$VERSION"
