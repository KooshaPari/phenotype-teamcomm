// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for the dispatcher — every M0 method must produce a
//! well-shaped response.

use serde_json::{json, Value};
use teamcomm_mcp::dispatch::dispatch;

/// Helper: dispatch a method and assert it returned a success (non-error)
/// payload, then return the payload.
async fn check_success(method: &str, params: Value) -> Value {
    let resp = dispatch(method, params).await;
    assert!(
        resp.get("error").is_none(),
        "method {method} returned an error response: {resp:?}"
    );
    resp
}

#[tokio::test]
async fn register_session_returns_session_id_and_lease() {
    let resp = check_success("register_session", json!({})).await;
    let obj = resp
        .as_object()
        .expect("register_session must return an object");
    assert!(obj.contains_key("session_id"), "missing session_id");
    assert!(obj.contains_key("lease_ttl_sec"), "missing lease_ttl_sec");
    let session_id = obj["session_id"].as_str().expect("session_id is a string");
    assert!(
        session_id.starts_with("sess_"),
        "session_id should start with 'sess_', got: {session_id}"
    );
    assert_eq!(obj["lease_ttl_sec"], json!(90));
}

#[tokio::test]
async fn register_session_produces_unique_ids() {
    let a = check_success("register_session", json!({})).await;
    let b = check_success("register_session", json!({})).await;
    assert_ne!(a["session_id"], b["session_id"], "ids should be unique");
}

#[tokio::test]
async fn deregister_session_returns_ok() {
    let resp = check_success("deregister_session", json!({})).await;
    assert_eq!(resp, json!({ "ok": true }));
}

#[tokio::test]
async fn list_sessions_returns_empty_array() {
    let resp = check_success("list_sessions", json!({})).await;
    assert!(resp.is_array(), "list_sessions must return an array");
    assert_eq!(resp.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn claim_file_returns_reservation_id_expires_at_no_conflicts() {
    let resp = check_success("claim_file", json!({})).await;
    let obj = resp.as_object().expect("claim_file must return an object");
    let reservation_id = obj["reservation_id"]
        .as_str()
        .expect("reservation_id is a string");
    assert!(
        reservation_id.starts_with("r_"),
        "reservation_id should start with 'r_', got: {reservation_id}"
    );
    let expires_at = obj["expires_at"].as_str().expect("expires_at is a string");
    assert!(
        chrono::DateTime::parse_from_rfc3339(expires_at).is_ok(),
        "expires_at must be RFC3339, got: {expires_at}"
    );
    assert!(obj["conflicts"].is_array(), "conflicts must be an array");
    assert_eq!(obj["conflicts"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn release_file_returns_ok() {
    let resp = check_success("release_file", json!({})).await;
    assert_eq!(resp, json!({ "ok": true }));
}

#[tokio::test]
async fn list_claims_returns_empty_array() {
    let resp = check_success("list_claims", json!({})).await;
    assert!(resp.is_array());
    assert_eq!(resp.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn post_message_returns_message_id() {
    let resp = check_success("post_message", json!({})).await;
    let obj = resp
        .as_object()
        .expect("post_message must return an object");
    let message_id = obj["message_id"].as_str().expect("message_id is a string");
    assert!(
        message_id.starts_with("m_"),
        "message_id should start with 'm_', got: {message_id}"
    );
}

#[tokio::test]
async fn read_inbox_returns_empty_array() {
    let resp = check_success("read_inbox", json!({})).await;
    assert!(resp.is_array());
    assert_eq!(resp.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn announce_focus_returns_ok() {
    let resp = check_success("announce_focus", json!({})).await;
    assert_eq!(resp, json!({ "ok": true }));
}

#[tokio::test]
async fn set_status_returns_ok() {
    let resp = check_success("set_status", json!({})).await;
    assert_eq!(resp, json!({ "ok": true }));
}

#[tokio::test]
async fn discover_agents_for_path_returns_empty_array() {
    let resp = check_success("discover_agents_for_path", json!({})).await;
    assert!(resp.is_array());
    assert_eq!(resp.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn unknown_method_returns_method_not_found_error() {
    let resp = dispatch("definitely_not_a_method", json!({})).await;
    let err = resp.get("error").expect("error present");
    assert_eq!(err["code"], json!(-32601));
    assert_eq!(err["message"], json!("method not found"));
}

/// Cross-check: the dispatcher's set of methods must match the manifest.
#[tokio::test]
async fn dispatcher_methods_match_manifest() {
    use teamcomm_mcp::manifest::tool_names;
    let names = tool_names();
    for name in &names {
        // A dummy call is enough — we just need the dispatcher to NOT
        // return a method-not-found error.
        let resp = dispatch(name, json!({})).await;
        assert!(
            resp.get("error").is_none(),
            "manifest tool {name} is not wired into the dispatcher"
        );
    }
}
