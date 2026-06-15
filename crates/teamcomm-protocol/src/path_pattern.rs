// SPDX-License-Identifier: MIT OR Apache-2.0
//! Path and glob pattern matching for reservations (M2).
//!
//! Reservations can target a single literal path or a glob-style pattern.
//! Glob syntax is the conventional `*`, `**`, `?`, and `[abc]` set; the
//! matcher supports them without pulling in a regex engine.
//!
//! Examples:
//! - `src/lib.rs`             — exact file
//! - `crates/teamcomm-*/**`   — every file in any `teamcomm-*` crate
//! - `docs/**/*.md`           — every Markdown file under `docs/`
//!
//! Two reservations conflict if either one's pattern matches the other's
//! stored path; see [`crate::conflict`] for the full rules.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// A path or pattern that a reservation covers.
///
/// On the wire this is encoded as a single string: the daemon detects
/// whether the string contains glob meta-characters (`*`, `?`, `[`) and
/// stores it as either a literal or a pattern. Path separators are
/// normalised to `/` so the same pattern matches on Windows and Unix.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PathPattern(pub String);

impl PathPattern {
    /// Build a pattern from a string. The string is normalised: leading
    /// `./` is stripped and backslashes are converted to forward slashes.
    pub fn new(s: impl Into<String>) -> Self {
        let raw = s.into();
        let mut normalised = raw.replace('\\', "/");
        if let Some(stripped) = normalised.strip_prefix("./") {
            normalised = stripped.to_string();
        }
        Self(normalised)
    }

    /// Borrow the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `true` if the string contains any glob meta-character.
    pub fn is_pattern(&self) -> bool {
        self.0.contains('*') || self.0.contains('?') || self.0.contains('[')
    }

    /// Convert to a `PathBuf` for storage. For literal patterns this
    /// is a straight conversion; for glob patterns the leading `**/`
    /// is preserved as-is so the stored value can still be recognised
    /// as a pattern.
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }

    /// `true` if `self` (treated as a pattern) matches the literal
    /// `candidate` path. Literal patterns match only when exactly equal
    /// (after normalisation). Returns `false` for any error.
    pub fn matches(&self, candidate: &Path) -> bool {
        match_compile(&self.0)
            .map(|m| m.matches(candidate))
            .unwrap_or(false)
    }
}

impl std::fmt::Display for PathPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for PathPattern {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for PathPattern {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl AsRef<str> for PathPattern {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A compiled pattern: a sequence of segments with optional `*` / `**`
/// wildcards. Compiled once, matched many times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPattern {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // AnyInSegment is descriptive; Segment here is the unit.
enum Segment {
    /// A literal segment that must match exactly.
    Literal(String),
    /// `*` — matches any single non-empty segment.
    AnyInSegment,
    /// `**` — matches zero or more segments.
    AnyRecursive,
}

impl CompiledPattern {
    /// Match this pattern against `candidate`. The candidate is
    /// normalised (separators converted to `/`) before matching.
    pub fn matches<P: AsRef<Path>>(&self, candidate: P) -> bool {
        let cand = candidate.as_ref().to_string_lossy().replace('\\', "/");
        let normalised = cand.strip_prefix("./").unwrap_or(&cand).to_string();
        // A bare "." represents "this directory" with no content; treat
        // it as an empty candidate so an empty pattern matches it.
        let normalised = if normalised == "." {
            String::new()
        } else {
            normalised
        };
        let parts: Vec<&str> = normalised.split('/').filter(|s| !s.is_empty()).collect();
        match_segments(&self.segments, &parts)
    }
}

/// Compile a pattern string into a `CompiledPattern`. Returns an error
/// if the pattern is structurally invalid (e.g. unmatched bracket).
pub fn match_compile(pattern: &str) -> Result<CompiledPattern, PatternError> {
    let normalised = pattern.replace('\\', "/");
    let stripped = normalised.strip_prefix("./").unwrap_or(&normalised);
    let mut segments = Vec::new();
    for raw in stripped.split('/') {
        if raw.is_empty() {
            continue;
        }
        segments.push(compile_segment(raw)?);
    }
    Ok(CompiledPattern { segments })
}

fn compile_segment(raw: &str) -> Result<Segment, PatternError> {
    if raw == "**" {
        return Ok(Segment::AnyRecursive);
    }
    if raw == "*" {
        return Ok(Segment::AnyInSegment);
    }
    if raw.contains('[') && !raw.contains(']') {
        return Err(PatternError::UnmatchedBracket(raw.to_string()));
    }
    Ok(Segment::Literal(raw.to_string()))
}

/// Recursive matcher. `pattern` is the remaining pattern segments,
/// `parts` is the remaining candidate path parts.
fn match_segments(pattern: &[Segment], parts: &[&str]) -> bool {
    match (pattern.first(), parts.first()) {
        (None, None) => true,
        (Some(Segment::AnyRecursive), _) => {
            // `**` matches zero or more segments. Try every possible
            // split point; if any works, the whole match succeeds.
            // This is exponential in the worst case but reservation
            // patterns are short and `**` is usually at the edges.
            for skip in 0..=parts.len() {
                if match_segments(&pattern[1..], &parts[skip..]) {
                    return true;
                }
            }
            false
        }
        (Some(_), None) => {
            // Pattern has segments left but candidate does not; the
            // pattern can still match if the rest is all `**`.
            pattern.iter().all(|s| matches!(s, Segment::AnyRecursive))
        }
        (None, Some(_)) => false,
        (Some(Segment::Literal(lit)), Some(cand)) => {
            segment_matches(lit, cand) && match_segments(&pattern[1..], &parts[1..])
        }
        (Some(Segment::AnyInSegment), Some(_)) => {
            // `*` matches one full segment of any non-empty value.
            !parts[0].is_empty() && match_segments(&pattern[1..], &parts[1..])
        }
    }
}

/// Match a single pattern segment (which may itself contain `*`, `?`,
/// `[...]`) against a single candidate path part.
fn segment_matches(pattern: &str, candidate: &str) -> bool {
    let pat_bytes = pattern.as_bytes();
    let cand_bytes = candidate.as_bytes();
    segment_matches_inner(pat_bytes, 0, cand_bytes, 0)
}

fn segment_matches_inner(pat: &[u8], pi: usize, cand: &[u8], ci: usize) -> bool {
    let mut pi = pi;
    let mut ci = ci;
    while pi < pat.len() {
        match pat[pi] {
            b'*' => {
                // Greedy `*` in a segment. Try every suffix of `cand`
                // from `ci` onwards; recurse on the rest of the pattern.
                if pi + 1 == pat.len() {
                    // Trailing `*` consumes the rest of the segment.
                    return true;
                }
                for k in ci..=cand.len() {
                    if segment_matches_inner(pat, pi + 1, cand, k) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                if ci >= cand.len() {
                    return false;
                }
                pi += 1;
                ci += 1;
            }
            b'[' => {
                // Character class.
                if ci >= cand.len() {
                    return false;
                }
                let close = match pat[pi + 1..].iter().position(|&b| b == b']') {
                    Some(p) => pi + 1 + p,
                    None => return false,
                };
                let class = &pat[pi + 1..close];
                let negate = class.first() == Some(&b'!');
                let class = if negate { &class[1..] } else { class };
                let ch = cand[ci];
                let in_class = class.contains(&ch);
                if in_class == negate {
                    return false;
                }
                pi = close + 1;
                ci += 1;
            }
            other => {
                if ci >= cand.len() || cand[ci] != other {
                    return false;
                }
                pi += 1;
                ci += 1;
            }
        }
    }
    ci == cand.len()
}

/// Errors that can arise from compiling a pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternError {
    /// `[` without a matching `]`.
    UnmatchedBracket(String),
}

impl std::fmt::Display for PatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternError::UnmatchedBracket(s) => {
                write!(f, "unmatched bracket in pattern: {s}")
            }
        }
    }
}

impl std::error::Error for PatternError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> CompiledPattern {
        match_compile(s).expect("compile")
    }

    #[test]
    fn literal_pattern_matches_exact_path() {
        let pat = p("src/lib.rs");
        assert!(pat.matches("src/lib.rs"));
        assert!(pat.matches("./src/lib.rs")); // normalised
        assert!(!pat.matches("src/lib2.rs"));
        assert!(!pat.matches("src/sub/lib.rs"));
    }

    #[test]
    fn star_matches_one_segment() {
        let pat = p("crates/*/src/lib.rs");
        assert!(pat.matches("crates/teamcomm-daemon/src/lib.rs"));
        assert!(pat.matches("crates/teamcomm-cli/src/lib.rs"));
        assert!(!pat.matches("crates/teamcomm-daemon/src/sub/lib.rs"));
        assert!(!pat.matches("other/teamcomm-daemon/src/lib.rs"));
    }

    #[test]
    fn double_star_matches_zero_or_more_segments() {
        let pat = p("crates/teamcomm-*/**");
        assert!(pat.matches("crates/teamcomm-daemon/src/lib.rs"));
        assert!(pat.matches("crates/teamcomm-cli/Cargo.toml"));
        // The first `*` matches "teamcomm-anything" (literal `teamcomm-`
        // prefix is *not* enforced; `*` in a single segment is greedy).
        // Two segments of `**` ⇒ matches a deep path.
        assert!(pat.matches("crates/teamcomm-x/a/b/c/d.txt"));
    }

    #[test]
    fn double_star_at_start_matches_prefix() {
        let pat = p("**/lib.rs");
        assert!(pat.matches("src/lib.rs"));
        assert!(pat.matches("a/b/c/lib.rs"));
    }

    #[test]
    fn question_mark_matches_single_char() {
        let pat = p("src/lib?.rs");
        assert!(pat.matches("src/lib1.rs"));
        assert!(pat.matches("src/libA.rs"));
        assert!(!pat.matches("src/lib12.rs"));
        assert!(!pat.matches("src/lib.rs"));
    }

    #[test]
    fn character_class_matches_any_of_set() {
        let pat = p("src/lib[123].rs");
        assert!(pat.matches("src/lib1.rs"));
        assert!(pat.matches("src/lib2.rs"));
        assert!(pat.matches("src/lib3.rs"));
        assert!(!pat.matches("src/lib4.rs"));
    }

    #[test]
    fn character_class_supports_negation() {
        let pat = p("src/lib[!x].rs");
        assert!(pat.matches("src/lib1.rs"));
        assert!(!pat.matches("src/libx.rs"));
    }

    #[test]
    fn backslash_is_normalised_to_slash() {
        let pat = p("src/lib.rs");
        assert!(pat.matches("src\\lib.rs"));
    }

    #[test]
    fn leading_dot_slash_is_stripped() {
        let pat = p("./src/lib.rs");
        assert!(pat.matches("src/lib.rs"));
    }

    #[test]
    fn empty_candidate_does_not_match_nonempty_pattern() {
        let pat = p("src/lib.rs");
        assert!(!pat.matches(""));
    }

    #[test]
    fn empty_pattern_matches_empty() {
        let pat = p("");
        assert!(pat.matches(""));
        assert!(pat.matches("."));
        // Empty pattern matches a single dot or empty string, but
        // not a path with content.
        assert!(!pat.matches("foo"));
    }

    #[test]
    fn is_pattern_detects_glob_meta() {
        assert!(!PathPattern::new("src/lib.rs").is_pattern());
        assert!(PathPattern::new("src/*.rs").is_pattern());
        assert!(PathPattern::new("src/lib?.rs").is_pattern());
        assert!(PathPattern::new("src/lib[12].rs").is_pattern());
    }

    #[test]
    fn path_pattern_display_round_trip() {
        let s = "crates/*/src/**/*.rs";
        let p = PathPattern::new(s);
        assert_eq!(p.to_string(), s);
        assert_eq!(p.as_str(), s);
    }

    #[test]
    fn unmatched_bracket_returns_error() {
        let err = match_compile("src/lib[.rs").unwrap_err();
        assert!(
            matches!(err, PatternError::UnmatchedBracket(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn star_in_segment_can_match_partial() {
        // A single segment can have multiple wildcards; `*` may absorb
        // intervening characters and `?` is exactly one.
        let pat = p("file-*-backup-?.dat");
        assert!(pat.matches("file-2024-backup-A.dat"));
        assert!(pat.matches("file-2024-backup-9.dat"));
        assert!(!pat.matches("file-2024-backup-99.dat"));
        // Two dashes between the wildcards is still valid: `*` simply
        // matches `2024-` and the literal `-backup-` lines up after.
        assert!(pat.matches("file-2024--backup-A.dat"));
        // But a literal that genuinely doesn't line up must fail.
        assert!(!pat.matches("file-2024-Xbackup-A.dat"));
    }
}
