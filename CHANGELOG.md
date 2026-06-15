# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **M2 — Reservations, file/path locks, and conflict detection.**
  - Glob-aware `PathPattern` matcher in `teamcomm-protocol` (supports
    `*`, `**`, `?`, `[abc]`, `[!abc]`, leading `./` stripping, and
    backslash → forward-slash normalisation).
  - `Conflict` / `ConflictReason` / `ConflictReport` wire types in
    `teamcomm-protocol` (snake_case reasons: `exact_match`,
    `directory_containment`, `pattern_overlap`, `pattern_covers`,
    `existing_pattern_covers`, `mode_incompatible`).
  - New daemon `conflict` module that classifies overlaps with a clear
    reason taxonomy, including `directory_containment` (inner path is
    inside an existing directory reservation) and the four pattern
    directions.
  - New JSON-RPC methods on the daemon listener:
    - `reservation.claim_many` — atomic multi-path claim.
    - `reservation.pattern_claim` — explicit glob claim.
    - `reservation.conflicts_for_path` — read-only conflict probe.
    - `reservation.list_conflicts` — diagnostic dump of the live
      overlap set.
  - `ClaimResult.conflicts` is now `Vec<Conflict>` (rich reason +
    blocking reservation), not just `Vec<Reservation>`.
  - SQLite-backed persistence for reservations: every claim/release
    writes through to the durable store; a daemon restart re-reads
    active reservations. Schema v2 adds the `is_pattern` flag.
  - Session deregister cleans up that session's reservations from
    both memory and SQLite.
  - 10 end-to-end integration tests covering the new reservation
    surface, plus 21 unit tests for `PathPattern`, 8 for the
    conflict module, and 19 for the SQLite store.

### Changed

- `teamcomm-protocol` `ClaimResult.conflicts` is now `Vec<Conflict>`.

## [0.1.0] - 2026-06-14

### Added

- Initial release with version tracking.

[Unreleased]: https://github.com/KooshaPari/phenotype-teamcomm/compare/0.1.0...HEAD
[0.1.0]: https://github.com/KooshaPari/phenotype-teamcomm/releases/tag/0.1.0
