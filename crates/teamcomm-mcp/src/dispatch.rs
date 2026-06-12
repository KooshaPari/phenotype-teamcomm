//! Method dispatch — look up a JSON-RPC `method` string and call the
//! corresponding handler. Unknown methods return a JSON-RPC 2.0 error
//! (`code = -32601`, "method not found").
//!
//! The handler list is intentionally aligned with the manifest in
//! `mcp/manifest.json`; if you add a method here, add a tool entry there too.

use serde_json::{json, Value};

use crate::handlers;

/// Dispatch a single JSON-RPC method call to the matching handler.
///
/// Returns either:
/// * the handler's success payload, or
/// * `{"error": {"code": -32601, "message": "method not found"}}` for unknown
///   methods.
///
/// The caller (`main.rs`) is responsible for wrapping the result in a full
/// JSON-RPC 2.0 envelope and detecting the `error` key.
pub async fn dispatch(method: &str, params: Value) -> Value {
    match method {
        "register_session" => handlers::register_session(params).await,
        "deregister_session" => handlers::deregister_session(params).await,
        "list_sessions" => handlers::list_sessions(params).await,
        "claim_file" => handlers::claim_file(params).await,
        "release_file" => handlers::release_file(params).await,
        "list_claims" => handlers::list_claims(params).await,
        "post_message" => handlers::post_message(params).await,
        "read_inbox" => handlers::read_inbox(params).await,
        "announce_focus" => handlers::announce_focus(params).await,
        "set_status" => handlers::set_status(params).await,
        "discover_agents_for_path" => handlers::discover_agents_for_path(params).await,
        _ => json!({
            "error": {
                "code": -32601,
                "message": "method not found",
            }
        }),
    }
}
