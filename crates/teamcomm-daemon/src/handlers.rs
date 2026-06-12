//! JSON-RPC method handlers for M0.
//!
//! Each handler takes the shared [`AppState`] and the raw JSON-RPC
//! `params` value, and returns either a successful JSON result payload
//! or a [`TeamcommError`]. The listener wraps the result in the
//! `JsonRpcResponse` envelope; the error is mapped to a
//! `JsonRpcErrorResponse` by the listener.

use std::path::PathBuf;

use chrono::Utc;
use serde_json::{json, Value};
use tracing::{debug, info};

use teamcomm_protocol::rpc::RpcId;
use teamcomm_protocol::{AgentType, Session};

use crate::error::TeamcommError;
use crate::state::{mint_session_id, AppState};

/// Heartbeat interval the daemon recommends to agents, in seconds.
///
/// Returned in `session.heartbeat` responses so clients can dynamically
/// pick up a new cadence without redeploying.
pub const HEARTBEAT_INTERVAL_SEC: u64 = 30;

/// Session lease length, in seconds. After `LEASE_TTL_SEC` of no
/// heartbeats, the daemon may reap the session. Returned in
/// `session.register` responses.
pub const LEASE_TTL_SEC: u64 = 90;

/// `session.register` — create or refresh a session for the given pid.
///
/// Idempotent on `pid`: if the same pid re-registers, the existing
/// session is reused and its `last_heartbeat` is bumped. This makes
/// at-startup agent restarts safe.
pub async fn handle_session_register(
    state: AppState,
    payload: Value,
) -> Result<Value, TeamcommError> {
    let pid = payload
        .get("pid")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| TeamcommError::InvalidParams("missing required field: pid".into()))?
        as u32;

    let agent_kind_str = payload
        .get("agent_type")
        .and_then(|v| v.as_str())
        .unwrap_or("custom")
        .to_string();
    let agent_type = parse_agent_type(&agent_kind_str);

    let working_dir: PathBuf = payload
        .get("working_dir")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"));

    let capabilities: Vec<String> = payload
        .get("capabilities")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let now = Utc::now();

    // Idempotency: re-use the existing session for this pid, if any.
    let session_id = {
        let mut guard = state.write().await;
        if let Some(existing_id) = guard.sessions_by_pid.get(&pid).cloned() {
            if let Some(existing) = guard.sessions.get_mut(&existing_id) {
                existing.last_heartbeat = now;
                debug!(pid, session_id = %existing_id, "re-registered existing session");
                existing_id
            } else {
                // Reverse index is out of sync — recover by re-inserting
                // under a fresh id. Should not normally happen.
                insert_new_session(
                    &mut guard,
                    pid,
                    agent_type,
                    working_dir,
                    capabilities,
                    now,
                )
            }
        } else {
            insert_new_session(
                &mut guard,
                pid,
                agent_type,
                working_dir,
                capabilities,
                now,
            )
        }
    };

    info!(session_id = %session_id, pid, agent = %agent_kind_str, "session registered");

    Ok(json!({
        "session_id": session_id,
        "lease_ttl_sec": LEASE_TTL_SEC,
    }))
}

/// `session.deregister` — remove a session by id.
///
/// Missing session is a no-op success: we return `{"ok": true}` so that
/// at-shutdown cleanup requests are idempotent.
pub async fn handle_session_deregister(
    state: AppState,
    payload: Value,
) -> Result<Value, TeamcommError> {
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TeamcommError::InvalidParams("missing required field: session_id".into()))?
        .to_string();

    let mut guard = state.write().await;
    let removed = guard.sessions.remove(&session_id);
    if let Some(sess) = &removed {
        guard.sessions_by_pid.remove(&sess.pid);
    }
    drop(guard);

    info!(session_id = %session_id, removed = removed.is_some(), "session deregistered");

    Ok(json!({ "ok": true }))
}

/// `session.heartbeat` — refresh a session's `last_heartbeat` and report
/// the recommended next-heartbeat interval.
pub async fn handle_session_heartbeat(
    state: AppState,
    payload: Value,
) -> Result<Value, TeamcommError> {
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TeamcommError::InvalidParams("missing required field: session_id".into()))?
        .to_string();

    let now = Utc::now();
    {
        let mut guard = state.write().await;
        let session = guard
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| TeamcommError::NotFound(format!("session {session_id}")))?;
        session.last_heartbeat = now;
        // Status is tracked on `LiveState` in the protocol crate, not on
        // `Session` itself. M0 doesn't receive explicit state pushes, so
        // we leave the agent's previously-reported status untouched.
    }

    debug!(session_id = %session_id, "heartbeat");

    Ok(json!({
        "ok": true,
        "next_heartbeat_sec": HEARTBEAT_INTERVAL_SEC,
    }))
}

// ----- helpers -----

/// Insert a freshly-minted [`Session`] into the in-memory state and
/// return its id. Caller must already hold a write lock.
fn insert_new_session(
    guard: &mut crate::state::AppStateInner,
    pid: u32,
    agent_type: AgentType,
    working_dir: PathBuf,
    capabilities: Vec<String>,
    now: chrono::DateTime<Utc>,
) -> String {
    let session_id = mint_session_id();
    let session = Session {
        session_id: session_id.clone(),
        agent_type,
        pid,
        started_at: now,
        working_dir,
        capabilities,
        last_heartbeat: now,
    };
    guard.sessions.insert(session_id.clone(), session);
    guard.sessions_by_pid.insert(pid, session_id.clone());
    session_id
}

/// Map a free-form agent kind string to the typed [`AgentType`] enum.
fn parse_agent_type(s: &str) -> AgentType {
    match s.to_ascii_lowercase().as_str() {
        "forge" => AgentType::Forge,
        "codex" => AgentType::Codex,
        "claude" => AgentType::Claude,
        "copilot" => AgentType::Copilot,
        other => AgentType::Custom(other.to_string()),
    }
}

/// Convenience for the listener: given an [`RpcId`] and a successful
/// handler result, build a JSON-RPC success envelope.
pub fn success_envelope(id: RpcId, result: Value) -> teamcomm_protocol::rpc::JsonRpcResponse {
    teamcomm_protocol::rpc::JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result,
    }
}

/// Convenience for the listener: given an [`RpcId`] and a
/// [`TeamcommError`], build a JSON-RPC error envelope.
pub fn error_envelope(
    id: RpcId,
    err: TeamcommError,
) -> teamcomm_protocol::rpc::JsonRpcErrorResponse {
    err.into_response(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::new_state;

    #[tokio::test]
    async fn register_assigns_session_id_and_lease() {
        let state = new_state();
        let result = handle_session_register(
            state.clone(),
            json!({ "pid": 1001, "agent_type": "forge" }),
        )
        .await
        .unwrap();
        assert!(result["session_id"].as_str().unwrap().starts_with("sess_"));
        assert_eq!(result["lease_ttl_sec"], LEASE_TTL_SEC);
    }

    #[tokio::test]
    async fn register_is_idempotent_on_pid() {
        let state = new_state();
        let r1 = handle_session_register(state.clone(), json!({ "pid": 2002 }))
            .await
            .unwrap();
        let id1 = r1["session_id"].as_str().unwrap().to_string();
        let r2 = handle_session_register(state.clone(), json!({ "pid": 2002 }))
            .await
            .unwrap();
        let id2 = r2["session_id"].as_str().unwrap().to_string();
        assert_eq!(id1, id2, "second register must reuse the first session");
    }

    #[tokio::test]
    async fn register_rejects_missing_pid() {
        let state = new_state();
        let err = handle_session_register(state.clone(), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, TeamcommError::InvalidParams(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn deregister_returns_ok() {
        let state = new_state();
        let r = handle_session_register(state.clone(), json!({ "pid": 1 }))
            .await
            .unwrap();
        let id = r["session_id"].as_str().unwrap();
        let out = handle_session_deregister(state.clone(), json!({ "session_id": id }))
            .await
            .unwrap();
        assert_eq!(out, json!({ "ok": true }));
    }

    #[tokio::test]
    async fn deregister_unknown_session_is_idempotent_ok() {
        let state = new_state();
        let out =
            handle_session_deregister(state.clone(), json!({ "session_id": "sess_nope" }))
                .await
                .unwrap();
        assert_eq!(out, json!({ "ok": true }));
    }

    #[tokio::test]
    async fn heartbeat_refreshes_and_returns_next_interval() {
        let state = new_state();
        let r = handle_session_register(state.clone(), json!({ "pid": 1 }))
            .await
            .unwrap();
        let id = r["session_id"].as_str().unwrap();
        let out = handle_session_heartbeat(state.clone(), json!({ "session_id": id }))
            .await
            .unwrap();
        assert_eq!(out["ok"], json!(true));
        assert_eq!(out["next_heartbeat_sec"], json!(HEARTBEAT_INTERVAL_SEC));
    }

    #[tokio::test]
    async fn heartbeat_unknown_session_returns_not_found() {
        let state = new_state();
        let err = handle_session_heartbeat(state.clone(), json!({ "session_id": "sess_nope" }))
            .await
            .unwrap_err();
        assert!(matches!(err, TeamcommError::NotFound(_)), "got {err:?}");
    }

    #[test]
    fn parse_agent_type_known_and_custom() {
        assert_eq!(parse_agent_type("forge"), AgentType::Forge);
        assert_eq!(parse_agent_type("CLAUDE"), AgentType::Claude);
        assert_eq!(
            parse_agent_type("aider"),
            AgentType::Custom("aider".to_string())
        );
    }
}
