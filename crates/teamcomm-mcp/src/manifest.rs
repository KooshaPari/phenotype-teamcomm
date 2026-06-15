// SPDX-License-Identifier: MIT OR Apache-2.0
//! Manifest loader for the MCP tool catalogue.
//!
//! The manifest is the single source of truth for which tools the server
//! exposes; the dispatcher and the integration test both consume the names
//! extracted from it so that adding a tool here automatically updates both.

/// The raw manifest text, embedded at compile time from `mcp/manifest.json`.
pub const MANIFEST: &str = include_str!("../mcp/manifest.json");

/// Returns the list of tool names declared in the manifest, in declaration
/// order. Panics if the manifest is not valid JSON or is missing the `tools`
/// array — both are compile-time invariants because the manifest is
/// `include_str!`-ed and the manifest is committed.
pub fn tool_names() -> Vec<String> {
    let value: serde_json::Value = serde_json::from_str(MANIFEST)
        .expect("mcp/manifest.json must be valid JSON (compile-time invariant)");
    let tools = value
        .get("tools")
        .and_then(|t| t.as_array())
        .expect("mcp/manifest.json must have a top-level 'tools' array");
    tools
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_parses() {
        let _ = tool_names();
    }

    #[test]
    fn manifest_has_expected_top_level_keys() {
        let v: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
        assert_eq!(v["name"], "teamcomm");
        assert_eq!(v["transport"], "stdio");
        assert!(v["tools"].is_array());
    }
}
