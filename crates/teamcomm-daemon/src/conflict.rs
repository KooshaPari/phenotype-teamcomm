// SPDX-License-Identifier: MIT OR Apache-2.0
//! Conflict detection for reservations (M2).
//!
//! Given a candidate reservation (the agent wants to claim this path /
//! pattern in this mode) and the set of currently-active reservations,
//! produce a list of [`Conflict`]s describing each existing reservation
//! that blocks the candidate.
//!
//! The logic here is **pure**: no I/O, no async, no clock. It's exercised
//! by integration tests in `tests/integration.rs` and unit-tested in
//! this module. The daemon's `handlers` module wires these results into
//! the JSON-RPC `claim` response.

use teamcomm_protocol::{
    match_compile, mode_conflicts, reservation::ReservationMode, Conflict, ConflictReason,
    Reservation,
};

/// Result of probing a candidate against the active reservation set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictReport {
    /// All existing reservations that block the candidate. Empty if the
    /// candidate is unblocked.
    pub conflicts: Vec<Conflict>,
}

impl ConflictReport {
    /// `true` if there are no conflicts.
    pub fn is_clean(&self) -> bool {
        self.conflicts.is_empty()
    }
}

/// What a candidate looks like before it's been inserted into the store.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// The literal path or pattern the agent is trying to claim.
    pub path: std::path::PathBuf,
    /// The lock mode the agent wants.
    pub mode: ReservationMode,
    /// Whether the candidate path contains glob wildcards. If it does,
    /// it is treated as a pattern (matches any candidate path that
    /// satisfies the glob).
    pub is_pattern: bool,
}

impl Candidate {
    /// Convenience constructor: derive `is_pattern` from the path string.
    pub fn from_path(path: impl Into<std::path::PathBuf>, mode: ReservationMode) -> Self {
        let p = path.into();
        let is_pattern = p.to_string_lossy().contains(['*', '?', '[']);
        Self {
            path: p,
            mode,
            is_pattern,
        }
    }

    /// Construct an explicitly-pattern candidate (skips wildcard
    /// detection, in case the caller wants to opt in / out manually).
    pub fn pattern(path: impl Into<std::path::PathBuf>, mode: ReservationMode) -> Self {
        Self {
            path: path.into(),
            mode,
            is_pattern: true,
        }
    }

    /// Construct an explicitly-literal candidate.
    pub fn literal(path: impl Into<std::path::PathBuf>, mode: ReservationMode) -> Self {
        Self {
            path: path.into(),
            mode,
            is_pattern: false,
        }
    }
}

/// Compute every conflict between `candidate` and `existing`.
///
/// `existing` is the full set of currently-active reservations (i.e.
/// not expired, not released). The same reservation may be present
/// in `existing` and also be the candidate's own previous acquisition;
/// callers are responsible for filtering those out before invoking
/// `detect_conflicts`.
pub fn detect_conflicts(
    candidate: Candidate,
    existing: impl IntoIterator<Item = Reservation>,
) -> ConflictReport {
    let mut conflicts = Vec::new();

    for res in existing {
        if !mode_conflicts(candidate.mode, res.mode) {
            continue;
        }
        if let Some((reason, detail)) = classify(&candidate, &res) {
            conflicts.push(Conflict::with_detail(
                res,
                reason,
                detail.unwrap_or_default(),
            ));
        }
    }

    ConflictReport { conflicts }
}

/// Classify the relationship between a candidate and a single existing
/// reservation. Returns `Some((reason, detail))` if they overlap, else
/// `None`.
fn classify(
    candidate: &Candidate,
    existing: &Reservation,
) -> Option<(ConflictReason, Option<String>)> {
    let cand_str = candidate.path.to_string_lossy().to_string();
    let exist_str = existing.path.to_string_lossy().to_string();

    if !candidate.is_pattern {
        // Candidate is a literal path.
        if let Ok(pat) = match_compile(&exist_str) {
            if pat.matches(&cand_str) {
                let is_existing_literal = !exist_str.contains(['*', '?', '[']);
                let detail = if is_existing_literal && exist_str == cand_str {
                    Some(format!("path {} already claimed", cand_str))
                } else {
                    Some(format!(
                        "pattern {} covers candidate path {}",
                        exist_str, cand_str
                    ))
                };
                return Some((
                    if candidate.path == existing.path {
                        ConflictReason::ExactMatch
                    } else {
                        ConflictReason::ExistingPatternCovers
                    },
                    detail,
                ));
            }
        }

        // Directory containment: candidate path is under a directory
        // reservation, or vice versa.
        if let Some(dir_detail) = directory_containment(&candidate.path, &existing.path) {
            return Some((ConflictReason::DirectoryContainment, Some(dir_detail)));
        }
    } else {
        // Candidate is a pattern.
        if let Ok(pat) = match_compile(&cand_str) {
            if pat.matches(&exist_str) {
                // Existing reservation is a child of the candidate pattern.
                return Some((
                    ConflictReason::CandidatePatternCovers,
                    Some(format!(
                        "candidate pattern {} covers existing reservation {}",
                        cand_str, exist_str
                    )),
                ));
            }
        }

        // Pattern-vs-pattern: try every candidate path through the
        // existing pattern, looking for one that matches. We can only
        // detect overlap when one side is "more specific" in a way we
        // can prove by literal-prefix or directory containment.
        if let Some(overlap_detail) = pattern_overlap(&cand_str, &exist_str) {
            return Some((ConflictReason::PatternOverlap, Some(overlap_detail)));
        }
    }

    None
}

/// Detect directory-containment overlap: `inner` lives under `outer` or
/// `outer` lives under `inner`. Returns a detail string when they
/// overlap, else `None`. Both paths must be absolute to be comparable;
/// relative paths are compared lexically.
fn directory_containment(inner: &std::path::Path, outer: &std::path::Path) -> Option<String> {
    let inner_s = inner.to_string_lossy().into_owned();
    let outer_s = outer.to_string_lossy().into_owned();
    if inner_s == outer_s {
        return None; // exact match is handled by ExactMatch.
    }
    if inner_s.starts_with(&*(outer_s.clone() + "/")) {
        return Some(format!(
            "{} is inside reserved directory {}",
            inner_s, outer_s
        ));
    }
    if outer_s.starts_with(&*(inner_s.clone() + "/")) {
        return Some(format!(
            "existing reservation {} is inside candidate directory {}",
            outer_s, inner_s
        ));
    }
    None
}

/// Conservative pattern-vs-pattern overlap: detects cases where the two
/// patterns share a literal prefix and overlap somewhere under that
/// prefix. Returns a detail string when overlap is detected, else
/// `None`.
fn pattern_overlap(a: &str, b: &str) -> Option<String> {
    let a_segs: Vec<&str> = a.split('/').filter(|s| !s.is_empty()).collect();
    let b_segs: Vec<&str> = b.split('/').filter(|s| !s.is_empty()).collect();

    // Find the longest literal-only prefix shared by both patterns.
    let mut shared = 0;
    for (a_seg, b_seg) in a_segs.iter().zip(b_segs.iter()) {
        if a_seg == b_seg && !is_pattern_segment(a_seg) {
            shared += 1;
        } else {
            break;
        }
    }
    if shared == 0 {
        return None;
    }
    let prefix: Vec<&str> = a_segs[..shared].to_vec();
    let prefix_str = prefix.join("/");

    // Now check the next segment of each pattern: at least one must
    // admit the other side's next segment under the prefix.
    let a_rest = a_segs.get(shared).copied();
    let b_rest = b_segs.get(shared).copied();

    let compatible = match (a_rest, b_rest) {
        (Some(ar), Some(br)) => segment_compatible(ar, br),
        (Some(_), None) => true, // b is a prefix of a — overlap.
        (None, Some(_)) => true, // a is a prefix of b — overlap.
        (None, None) => true,    // both identical literal paths.
    };
    if compatible {
        Some(format!(
            "patterns {} and {} overlap under {}/",
            a, b, prefix_str
        ))
    } else {
        None
    }
}

fn is_pattern_segment(seg: &str) -> bool {
    seg.contains('*') || seg.contains('?') || seg.contains('[')
}

fn segment_compatible(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    if is_pattern_segment(a) || is_pattern_segment(b) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::path::PathBuf;

    fn res(path: &str, mode: ReservationMode) -> Reservation {
        Reservation {
            reservation_id: format!("res-{path}"),
            session_id: "sess-1".into(),
            path: PathBuf::from(path),
            mode,
            acquired_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2026, 6, 14, 12, 30, 0).unwrap(),
        }
    }

    fn cand(path: &str, mode: ReservationMode, is_pattern: bool) -> Candidate {
        let p = PathBuf::from(path);
        Candidate {
            path: p,
            mode,
            is_pattern,
        }
    }

    fn run(c: Candidate, existing: Vec<Reservation>) -> ConflictReport {
        detect_conflicts(c, existing)
    }

    #[test]
    fn exact_path_match_is_detected() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Write)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].reason, ConflictReason::ExactMatch);
    }

    #[test]
    fn pattern_covers_candidate() {
        let existing = vec![res("/repo/src/**", ReservationMode::Write)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(
            report.conflicts[0].reason,
            ConflictReason::ExistingPatternCovers
        );
    }

    #[test]
    fn candidate_pattern_covers_existing() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Write)];
        let c = cand("/repo/src/**", ReservationMode::Write, true);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(
            report.conflicts[0].reason,
            ConflictReason::CandidatePatternCovers
        );
    }

    #[test]
    fn read_does_not_block_read() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Read)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Read, false);
        let report = run(c, existing);
        assert!(report.is_clean());
    }

    #[test]
    fn write_does_not_block_read() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Write)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Read, false);
        let report = run(c, existing);
        assert!(report.is_clean());
    }

    #[test]
    fn read_blocks_exclusive() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Read)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Exclusive, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
    }

    #[test]
    fn exclusive_blocks_everything() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Exclusive)];
        for m in [
            ReservationMode::Read,
            ReservationMode::Write,
            ReservationMode::Exclusive,
        ] {
            let c = cand("/repo/src/lib.rs", m, false);
            let report = run(c, existing.clone());
            assert_eq!(report.conflicts.len(), 1);
        }
    }

    #[test]
    fn pattern_overlap_detected() {
        let existing = vec![res("/repo/src/*.rs", ReservationMode::Write)];
        let c = cand("/repo/src/sub/*.rs", ReservationMode::Write, true);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].reason, ConflictReason::PatternOverlap);
    }

    #[test]
    fn directory_containment_detected() {
        let existing = vec![res("/repo/src", ReservationMode::Write)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(
            report.conflicts[0].reason,
            ConflictReason::DirectoryContainment
        );
    }

    #[test]
    fn disjoint_paths_have_no_conflict() {
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Write)];
        let c = cand("/repo/tests/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert!(report.is_clean());
    }

    #[test]
    fn multiple_conflicts_are_all_reported() {
        let existing = vec![
            res("/repo/src/lib.rs", ReservationMode::Write),
            res("/repo/src", ReservationMode::Exclusive),
        ];
        let c = cand("/repo/src/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 2);
    }

    #[test]
    fn from_path_detects_wildcards() {
        let c = Candidate::from_path("/repo/src/**", ReservationMode::Write);
        assert!(c.is_pattern);
        let c2 = Candidate::from_path("/repo/src/lib.rs", ReservationMode::Write);
        assert!(!c2.is_pattern);
    }

    #[test]
    fn pattern_constructor_sets_flag() {
        let c = Candidate::pattern("/repo/src/lib.rs", ReservationMode::Write);
        assert!(c.is_pattern);
    }

    #[test]
    fn literal_constructor_clears_flag() {
        let c = Candidate::literal("/repo/src/*", ReservationMode::Write);
        assert!(!c.is_pattern);
    }

    #[test]
    fn conflict_report_clean() {
        let r = ConflictReport { conflicts: vec![] };
        assert!(r.is_clean());
    }

    #[test]
    fn multiple_blocking_reservations_all_listed() {
        // Three reservations, all on the same literal path → three
        // conflicts reported.
        let existing = vec![
            res("/repo/a.rs", ReservationMode::Write),
            res("/repo/a.rs", ReservationMode::Exclusive),
            res("/repo/a.rs", ReservationMode::Write),
        ];
        let c = cand("/repo/a.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 3);
    }

    #[test]
    fn pattern_with_no_overlap_is_clean() {
        let existing = vec![res("/other/src/*.rs", ReservationMode::Write)];
        let c = cand("/repo/src/lib.rs", ReservationMode::Write, false);
        let report = run(c, existing);
        assert!(report.is_clean());
    }

    #[test]
    fn pattern_overlap_disjoint_prefix_is_clean() {
        let existing = vec![res("/repo/src/*.rs", ReservationMode::Write)];
        let c = cand("/other/src/*.rs", ReservationMode::Write, true);
        let report = run(c, existing);
        assert!(report.is_clean());
    }

    #[test]
    fn directory_containment_vice_versa() {
        // Candidate is a directory, existing is a child file.
        let existing = vec![res("/repo/src/lib.rs", ReservationMode::Write)];
        let c = cand("/repo/src", ReservationMode::Write, false);
        let report = run(c, existing);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(
            report.conflicts[0].reason,
            ConflictReason::DirectoryContainment
        );
    }
}
