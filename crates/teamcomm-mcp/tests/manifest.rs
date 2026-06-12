//! Integration tests for the MCP manifest embedded in the binary.

use teamcomm_mcp::manifest;

/// The manifest must be valid JSON.
#[test]
fn manifest_is_valid_json() {
    let value: serde_json::Value =
        serde_json::from_str(manifest::MANIFEST).expect("MANIFEST must be valid JSON");
    assert!(
        value.is_object(),
        "manifest root must be a JSON object, got: {value:?}"
    );
}

/// The manifest must declare the top-level fields MCP clients rely on.
#[test]
fn manifest_has_required_top_level_fields() {
    let value: serde_json::Value = serde_json::from_str(manifest::MANIFEST).unwrap();
    assert_eq!(value["name"], "teamcomm", "manifest.name");
    assert_eq!(value["transport"], "stdio", "manifest.transport");
    assert!(
        value["tools"].is_array(),
        "manifest.tools must be an array"
    );
}

/// Every tool entry must have at minimum a name, description, params schema,
/// and returns schema.
#[test]
fn every_tool_has_required_fields() {
    let value: serde_json::Value = serde_json::from_str(manifest::MANIFEST).unwrap();
    let tools = value["tools"].as_array().expect("tools is array");
    assert!(!tools.is_empty(), "manifest must declare at least one tool");
    for tool in tools {
        let name = tool
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        assert!(tool.get("name").and_then(|n| n.as_str()).is_some(), "tool missing name");
        assert!(
            tool.get("description").and_then(|d| d.as_str()).is_some(),
            "tool {name} missing description"
        );
        assert!(tool.get("params").is_some(), "tool {name} missing params");
        assert!(tool.get("returns").is_some(), "tool {name} missing returns");
    }
}

/// All 11 M0 tool names must be present in the manifest. Names must be
/// unique (the dispatcher uses them as map keys).
#[test]
fn manifest_declares_all_11_m0_tools() {
    let names = manifest::tool_names();
    let expected = [
        "register_session",
        "deregister_session",
        "list_sessions",
        "claim_file",
        "release_file",
        "list_claims",
        "post_message",
        "read_inbox",
        "announce_focus",
        "set_status",
        "discover_agents_for_path",
    ];
    assert_eq!(
        names.len(),
        expected.len(),
        "manifest has {} tool entries, expected {}: {:?}",
        names.len(),
        expected.len(),
        names
    );
    let mut sorted = names.clone();
    sorted.sort();
    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort();
    assert_eq!(
        sorted, expected_sorted,
        "manifest tool name set does not match M0 spec"
    );
}
