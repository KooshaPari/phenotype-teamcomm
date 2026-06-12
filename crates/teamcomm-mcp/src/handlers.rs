//! M0 stub handlers — return realistic-shaped mock responses for each tool
//! without touching any real daemon state. The M1 implementation will replace
//! these bodies with calls into the teamcomm client (or daemon IPC).
//!
//! The handler signatures are stable: `(serde_json::Value) -> serde_json::Value`
//! where the input is the JSON-RPC `params` (defaults to `{}` if the caller
//! omitted it) and the output is the JSON-RPC `result`. Errors are signalled
//! by the dispatcher in `dispatch.rs`, not here.

use serde_json::{json, Value};

/// `register_session` — mint a new session id and return a 90s lease.
pub async fn register_session(_params: Value) -> Value {
    json!({
        "session_id": format!("sess_{}", uuid::Uuid::new_v4().simple()),
        "lease_ttl_sec": 90,
    })
}

/// `deregister_session` — acknowledge teardown.
pub async fn deregister_session(_params: Value) -> Value {
    json!({ "ok": true })
}

/// `list_sessions` — empty list in M0.
pub async fn list_sessions(_params: Value) -> Value {
    json!([])
}

/// `claim_file` — mint a reservation id, set an expires_at, and report no
/// conflicts.
pub async fn claim_file(_params: Value) -> Value {
    json!({
        "reservation_id": format!("r_{}", uuid::Uuid::new_v4().simple()),
        "expires_at": chrono::Utc::now().to_rfc3339(),
        "conflicts": [],
    })
}

/// `release_file` — acknowledge release.
pub async fn release_file(_params: Value) -> Value {
    json!({ "ok": true })
}

/// `list_claims` — empty list in M0.
pub async fn list_claims(_params: Value) -> Value {
    json!([])
}

/// `post_message` — mint a message id.
pub async fn post_message(_params: Value) -> Value {
    json!({
        "message_id": format!("m_{}", uuid::Uuid::new_v4().simple()),
    })
}

/// `read_inbox` — empty list in M0.
pub async fn read_inbox(_params: Value) -> Value {
    json!([])
}

/// `announce_focus` — acknowledge the announcement.
pub async fn announce_focus(_params: Value) -> Value {
    json!({ "ok": true })
}

/// `set_status` — acknowledge the status change.
pub async fn set_status(_params: Value) -> Value {
    json!({ "ok": true })
}

/// `discover_agents_for_path` — empty list in M0.
pub async fn discover_agents_for_path(_params: Value) -> Value {
    json!([])
}
