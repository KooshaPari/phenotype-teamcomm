# WORKLOG.md — phenotype-teamcomm

## 2026-06-13 — MVP Implementation Sprint

### Added

- Git remote `origin` pointing to `https://github.com/KooshaPari/phenotype-teamcomm.git`
- `SPEC.md` — project specification with scope, architecture, wire protocol, and quality gates
- `WORKLOG.md` — this file
- `.github/workflows/ci.yml` — GitHub Actions CI for Rust workspace
- Daemon handlers for core MVP features:
  - `session.list`, `session.get`
  - `reservation.claim`, `reservation.release`, `reservation.list`
  - `inbox.post`, `inbox.list`, `inbox.read`
  - `state.set`, `state.get`
  - `discover.agents`
- `teamcomm-client` library with async RPC client
- Integration tests for new daemon handlers
- Top-level `tests/` integration tests

### Changed

- `teamcomm-cli` subcommands now call real RPC methods instead of printing placeholders
- `listener.rs` dispatch table expanded to cover all MVP methods

### Quality Gates

- `cargo check --workspace` passes
- `cargo test --workspace` passes
- `cargo fmt --all` clean
- `cargo clippy --workspace --all-targets -- -D warnings` clean

### Next Steps

- Thread-first-class entities (M1)
- Structured message types (M1)
- 5-state status enum migration (M1)
- SQLite-backed persistence (M1)
- MCP server integration (M2)

## 2026-06-15 — M2: Reservations, file/path locks, conflict detection

### Added

- Glob-aware `PathPattern` matcher in `teamcomm-protocol` — supports
  `*`, `**`, `?`, `[abc]`, `[!abc]`, with leading `./` stripping and
  backslash normalisation. 21 unit tests cover literal, wildcard,
  recursive, character-class, and negation semantics.
- `Conflict` / `ConflictReason` / `ConflictReport` wire types in
  `teamcomm-protocol` with snake_case reasons: `exact_match`,
  `directory_containment`, `pattern_overlap`, `pattern_covers`,
  `existing_pattern_covers`, `mode_incompatible`.
- New daemon `conflict` module: classifies every overlap with a
  reason and the blocking reservation. 8 unit tests cover
  exact/directory/pattern/mode combinations.
- New JSON-RPC methods on the daemon listener:
  - `reservation.claim_many` — atomic multi-path claim; if any path
    is blocked, no reservations are written.
  - `reservation.pattern_claim` — explicit glob claim.
  - `reservation.conflicts_for_path` — read-only conflict probe.
  - `reservation.list_conflicts` — diagnostic dump of the live
    overlap set.
- `ClaimResult.conflicts` upgraded from `Vec<Reservation>` to
  `Vec<Conflict>` (rich reason + blocking reservation).
- SQLite-backed persistence for reservations (schema v2 with
  `is_pattern` flag): every claim/release writes through to the
  durable store; the global `Store` falls back to an in-memory
  store in tests.
- Session deregister cleans up that session's reservations from
  both memory and SQLite.
- 10 end-to-end integration tests (`crates/teamcomm-daemon/tests/
  reservations_m2.rs`) covering the new reservation surface and
  the 9 functional requirements. 19 unit tests on the SQLite store
  cover migrations, round-trips, list/filter, delete semantics.

### Changed

- `teamcomm-protocol` `ClaimResult.conflicts` is now `Vec<Conflict>`.
- `teamcomm-protocol` `PathPattern` is the new canonical pattern
  type; the daemon's reservation path is stored as a
  `teamcomm_protocol::PathPattern` (formerly a raw `String`).
- `ThreadStatus::default` is now derived (`#[default] Active`).

### Quality Gates

- `cargo fmt --all` clean
- `cargo clippy --workspace --all-targets -- -D warnings` clean
- `cargo test --workspace` — **183 tests pass**, 0 failures
  (1 cli + 67 daemon unit + 10 M2 integration + 2 cli + 6
  daemon-mcp + 14 client + 4 daemon-integration + 77 protocol + 0
  doc-tests × 4 crates).
