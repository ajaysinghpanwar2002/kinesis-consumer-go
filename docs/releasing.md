# Releasing and versioning

This repository publishes **two** Go modules from one repository:

| Module | Import path | Version tag |
| --- | --- | --- |
| Core library | `github.com/ajaysinghpanwar2002/kinesis-consumer-go` | `vX.Y.Z` |
| Valkey backend | `github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey` | `pkg/backend/valkey/vX.Y.Z` |

They are released together and share a version number.

The repository is also a Go workspace and contains two more modules —
`examples/valkey` and `test/integration` — that are **not** published. They
exist only for local development and testing.

## Versioning policy

The project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **MAJOR** — incompatible API changes.
- **MINOR** — backwards-compatible additions.
- **PATCH** — backwards-compatible fixes.

While the project is pre-1.0 (`0.y.z`), the public API is frozen within a
release line but may still change in a new **minor** version. Treat a `0.y.z`
to `0.(y+1).0` bump as potentially breaking and read the
[CHANGELOG](../CHANGELOG.md).

Both modules always carry the same version, even when only one changed, so that
`pkg/backend/valkey@vX.Y.Z` and the core `@vX.Y.Z` it requires are always a
matched pair.

## How local development resolves the modules

The published `go.mod` files require the sibling modules at their real release
version (for example the backend requires the core at `v0.1.0`) with **no
`replace` directives** — that is what a third party gets, and it resolves
against the published tags.

Before a tag exists (and between releases), those version requires cannot be
fetched from the network. Local development bridges the gap with `replace`
directives in **`go.work`**:

```
replace github.com/ajaysinghpanwar2002/kinesis-consumer-go v0.1.0 => .
replace github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey v0.1.0 => ./pkg/backend/valkey
```

`go.work` is never included in a published module zip, so these replacements
affect only this checkout, never a consumer. (They live in `go.work` rather
than in each `go.mod` because Go's workspace `use` directive alone does not
substitute the local checkout for a sibling's *pinned require version* when it
builds the pruned module graph for a transitively-imported dependency, and a
`replace` inside a published `go.mod` would be shipped as dead weight.)

The replacement versions must match the `require` versions in the `go.mod`
files. When you bump the release version, update both (see below).

## Cutting a release

1. Decide the new version `vX.Y.Z`.
2. Update the two published `go.mod` files and `go.work` to the new version:
   - `pkg/backend/valkey/go.mod`: `require .../kinesis-consumer-go vX.Y.Z`.
   - `examples/valkey/go.mod` and `test/integration/go.mod`: the two intra-repo
     `require` lines to `vX.Y.Z`.
   - `go.work`: both `replace ... vX.Y.Z => ...` lines.
3. Update [CHANGELOG.md](../CHANGELOG.md): move items out of `Unreleased` into a
   new `vX.Y.Z` section with the date, and refresh the compare/tag links.
4. Run the full gate: `make fmt-check vet build test` (and `make integration`
   if coordination, checkpointing, or drain changed).
5. Commit. The working tree must be clean before tagging.
6. Tag and push both modules with the helper:

   ```bash
   scripts/release.sh vX.Y.Z            # tags core + backend, pushes to origin
   scripts/release.sh vX.Y.Z --dry-run  # print the tag/push commands only
   scripts/release.sh vX.Y.Z --remote upstream
   ```

   The script refuses to run on a dirty tree, refuses to reuse an existing tag,
   and verifies that `pkg/backend/valkey/go.mod` requires the core module at
   exactly `vX.Y.Z` so the two tags stay a matched pair.

7. Verify third-party resolution once the tags are on the remote:

   ```bash
   cd "$(mktemp -d)" && go mod init tmp
   GOFLAGS=-mod=mod go get \
     github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey@vX.Y.Z
   ```

## First release: repository setup

Before the first `scripts/release.sh` run, the repository must exist on the
host that matches the module path and have an `origin` remote:

```bash
git remote add origin git@github.com:ajaysinghpanwar2002/kinesis-consumer-go.git
git push -u origin main
```

The module path (`github.com/ajaysinghpanwar2002/...`) must match the hosting
repository, or `go get` cannot find the modules.
