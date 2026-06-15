// SPDX-License-Identifier: MIT OR Apache-2.0
//! Conflict reporting for reservations (M2).
//!
//! When a `claim` is rejected, the daemon returns the existing
//! reservation that blocked it. M2 enriches that with a [`ConflictReason`]
//! so clients can tell *why* a claim was rejected and react accordingly
//! (e.g. wait for the blocker to release, escalate to a human, switch
//! to a different path).
//!
//! Conflict reasons are computed by pure logic in
//! `teamcomm_daemon::conflict`; this module just defines the wire types.

use serde::{Deserialize, Serialize};

use crate::reservation::{Reservation, ReservationMode};

/// Why two reservations overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictReason {
    /// Both reservations target the exact same path. The strongest
    /// reason — the candidate and the existing claim are not just
    /// overlapping, they are *the same* path.
    ExactMatch,
    /// The existing reservation's pattern matches the candidate's path
    /// (the candidate is a child of a glob or directory lock).
    ExistingPatternCovers,
    /// The candidate's pattern matches the existing reservation's path
    /// (the candidate is trying to claim more than the existing
    /// reservation covers).
    CandidatePatternCovers,
    /// Both patterns overlap but neither is a strict subset of the
    /// other; e.g. `src/*.rs` vs `src/sub/*.rs`.
    PatternOverlap,
    /// A directory-style reservation (`/repo/src`) blocks a child
    /// reservation (`/repo/src/lib.rs`) or vice versa.
    DirectoryContainment,
}

/// A single conflict between a candidate (the reservation the agent is
/// trying to acquire) and an existing reservation. Returned in the
/// `conflicts` array of a `claim` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflict {
    /// The existing reservation that blocked the candidate.
    pub existing: Reservation,
    /// The reason the conflict was detected.
    pub reason: ConflictReason,
    /// Optional human-readable explanation, e.g. the path or pattern
    /// that triggered the conflict.
    pub detail: Option<String>,
}

impl Conflict {
    /// Convenience constructor.
    pub fn new(existing: Reservation, reason: ConflictReason) -> Self {
        Self {
            existing,
            reason,
            detail: None,
        }
    }

    /// Convenience constructor with a detail string.
    pub fn with_detail(
        existing: Reservation,
        reason: ConflictReason,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            existing,
            reason,
            detail: Some(detail.into()),
        }
    }
}

/// Returns `true` if `a` and `b` represent a mode-level conflict.
///
/// Mode ordering: `Read < Write < Exclusive`. A claim of mode `A` and
/// an existing claim of mode `B` conflict iff `A >= B` in the ordering
/// AND `B` is not strictly weaker than `A` — i.e. two `Read`s on the
/// same path do *not* conflict, but a `Write` and an `Exclusive` on
/// the same path always do.
pub fn mode_conflicts(candidate: ReservationMode, existing: ReservationMode) -> bool {
    use ReservationMode::*;
    match (candidate, existing) {
        (Read, Read) => false,
        (Read, Write) => false,
        (Read, Exclusive) => true,
        (Write, Read) => false,
        (Write, Write) => true,
        (Write, Exclusive) => true,
        (Exclusive, _) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::path::PathBuf;

    fn res(mode: ReservationMode) -> Reservation {
        Reservation {
            reservation_id: "res-1".into(),
            session_id: "sess-1".into(),
            path: PathBuf::from("/repo/src/lib.rs"),
            mode,
            acquired_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 30, 0).unwrap(),
        }
    }

    #[test]
    fn read_vs_read_does_not_conflict() {
        assert!(!mode_conflicts(
            ReservationMode::Read,
            ReservationMode::Read
        ));
    }

    #[test]
    fn read_vs_write_does_not_conflict() {
        assert!(!mode_conflicts(
            ReservationMode::Read,
            ReservationMode::Write
        ));
    }

    #[test]
    fn read_vs_exclusive_conflicts() {
        assert!(mode_conflicts(
            ReservationMode::Read,
            ReservationMode::Exclusive
        ));
    }

    #[test]
    fn write_vs_write_conflicts() {
        assert!(mode_conflicts(
            ReservationMode::Write,
            ReservationMode::Write
        ));
    }

    #[test]
    fn write_vs_exclusive_conflicts() {
        assert!(mode_conflicts(
            ReservationMode::Write,
            ReservationMode::Exclusive
        ));
    }

    #[test]
    fn exclusive_always_conflicts() {
        for m in [
            ReservationMode::Read,
            ReservationMode::Write,
            ReservationMode::Exclusive,
        ] {
            assert!(
                mode_conflicts(ReservationMode::Exclusive, m),
                "Exclusive must conflict with {m:?}"
            );
        }
    }

    #[test]
    fn write_does_not_block_read() {
        assert!(!mode_conflicts(
            ReservationMode::Write,
            ReservationMode::Read
        ));
    }

    #[test]
    fn conflict_serialises_with_snake_case_reason() {
        let c = Conflict::new(res(ReservationMode::Write), ConflictReason::ExactMatch);
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("\"reason\":\"exact_match\""), "got: {s}");
        let back: Conflict = serde_json::from_str(&s).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn conflict_reason_roundtrip() {
        for r in [
            ConflictReason::ExactMatch,
            ConflictReason::ExistingPatternCovers,
            ConflictReason::CandidatePatternCovers,
            ConflictReason::PatternOverlap,
            ConflictReason::DirectoryContainment,
        ] {
            let s = serde_json::to_string(&r).unwrap();
            let back: ConflictReason = serde_json::from_str(&s).unwrap();
            assert_eq!(back, r);
        }
    }

    #[test]
    fn conflict_with_detail_serialises_detail() {
        let c = Conflict::with_detail(
            res(ReservationMode::Exclusive),
            ConflictReason::PatternOverlap,
            "src/* overlaps src/sub/*",
        );
        let s = serde_json::to_string(&c).unwrap();
        assert!(
            s.contains("\"detail\":\"src/* overlaps src/sub/*\""),
            "got: {s}"
        );
    }
}
