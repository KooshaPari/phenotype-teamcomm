// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end daemon flow tests for the v0.1 wire protocol.
//!
//! These tests exercise the daemon's full JSON-RPC surface (all 14 methods
//! in SPEC.md §Wire Protocol) against an in-memory state, sharing one
//! `AppState` instance across the whole test scope so reservations,
//! inbox messages, and live state persist across method calls.
//!
//! Coverage map:
//! - session.register / deregister / heartbeat / list / get
//! - reservation.claim / release / list (with conflict semantics)
//! - inbox.post / list / read (cross-session routing)
//! - state.set / get (status, focus_file, focus_branch, worktree)
//! - discover.agents (path / capability filters)

use std::time::Duration;

use serde_json::{json, Value};
use teamcomm_daemon::handlers::{
    handle_discover_agents, handle_inbox_list, handle_inbox_post, handle_inbox_read,
    handle_reservation_claim, handle_reservation_list, handle_reservation_release,
    handle_session_deregister, handle_session_get, handle_session_heartbeat, handle_session_list,
    handle_session_register, handle_state_get, handle_state_set,
};
use teamcomm_daemon::state::new_state;
use teamcomm_daemon::AppState;

async fn register(state: &AppState, pid: u32, agent_type: &str, caps: &[&str]) -> String {
    let result = handle_session_register(
        state.clone(),
        json!({
            "pid": pid,
            "agent_type": agent_type,
            "capabilities": caps,
        }),
    )
    .await
    .expect("register");
    result["session_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn full_lifecycle_register_heartbeat_deregister() {
    let state = new_state();
    let pid = 1001;

    let id = register(&state, pid, "forge", &["search", "edit"]).await;
    assert!(id.starts_with("sess_"));

    let hb = handle_session_heartbeat(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();
    assert_eq!(hb["ok"], json!(true));
    assert!(hb["next_heartbeat_sec"].as_u64().unwrap() > 0);

    let got = handle_session_get(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();
    assert_eq!(got["session_id"], json!(id));
    assert_eq!(got["agent_type"], json!("Forge"));
    assert_eq!(got["capabilities"], json!(["search", "edit"]));

    let dereg = handle_session_deregister(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();
    assert_eq!(dereg, json!({ "ok": true }));

    let err = handle_session_get(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap_err();
    assert!(matches!(err, teamcomm_daemon::TeamcommError::NotFound(_)));
}

#[tokio::test]
async fn register_is_idempotent_on_pid() {
    let state = new_state();
    let id1 = register(&state, 2002, "codex", &[]).await;
    let id2 = register(&state, 2002, "codex", &[]).await;
    assert_eq!(id1, id2, "second register must reuse the first session");
}

#[tokio::test]
async fn session_list_filters_by_agent_type() {
    let state = new_state();
    let _id_forge = register(&state, 3001, "forge", &[]).await;
    let _id_codex = register(&state, 3002, "codex", &[]).await;
    let _id_claude = register(&state, 3003, "claude", &[]).await;

    let all = handle_session_list(state.clone(), json!({}))
        .await
        .unwrap();
    assert_eq!(all.as_array().unwrap().len(), 3);

    let only_forge = handle_session_list(state.clone(), json!({ "agent_type": "forge" }))
        .await
        .unwrap();
    assert_eq!(only_forge.as_array().unwrap().len(), 1);
    assert_eq!(only_forge[0]["agent_type"], json!("Forge"));
}

#[tokio::test]
async fn reservation_claim_release_round_trip() {
    let state = new_state();
    let id = register(&state, 4001, "forge", &[]).await;

    let claim = handle_reservation_claim(
        state.clone(),
        json!({
            "session_id": id,
            "target": "crates/foo",
            "mode": "write",
            "ttl_sec": 60,
        }),
    )
    .await
    .unwrap();
    let resv_id = claim["reservation_id"].as_str().unwrap().to_string();
    assert!(resv_id.starts_with("resv_"));

    let listed = handle_reservation_list(state.clone(), json!({}))
        .await
        .unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["reservation_id"], json!(resv_id));
    assert_eq!(listed[0]["target"], json!("crates/foo"));

    let release = handle_reservation_release(
        state.clone(),
        json!({ "reservation_id": resv_id }),
    )
    .await
    .unwrap();
    assert_eq!(release, json!({ "ok": true }));

    let listed = handle_reservation_list(state.clone(), json!({}))
        .await
        .unwrap();
    assert!(listed.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn reservation_conflicts_with_existing_write() {
    let state = new_state();
    let id_a = register(&state, 5001, "forge", &[]).await;
    let id_b = register(&state, 5002, "codex", &[]).await;

    // A claims write on shared target
    let _ = handle_reservation_claim(
        state.clone(),
        json!({
            "session_id": id_a,
            "target": "shared/file.rs",
            "mode": "write",
        }),
    )
    .await
    .unwrap();

    // B's write claim should conflict
    let conflict = handle_reservation_claim(
        state.clone(),
        json!({
            "session_id": id_b,
            "target": "shared/file.rs",
            "mode": "write",
        }),
    )
    .await;
    assert!(
        conflict.is_err(),
        "second write claim on shared target should be rejected"
    );

    // B's read claim against A's write should also fail
    let read_conflict = handle_reservation_claim(
        state.clone(),
        json!({
            "session_id": id_b,
            "target": "shared/file.rs",
            "mode": "read",
        }),
    )
    .await;
    assert!(
        read_conflict.is_err(),
        "read claim against existing write should conflict"
    );
}

#[tokio::test]
async fn cross_session_inbox_post_list_read() {
    let state = new_state();
    let sender = register(&state, 6001, "forge", &[]).await;
    let receiver = register(&state, 6002, "codex", &[]).await;

    // sender posts to receiver with P1 (high) priority
    let post = handle_inbox_post(
        state.clone(),
        json!({
            "from_session": sender,
            "to_session": receiver,
            "subject": "task ready",
            "body": "the auth refactor is in PR #42",
            "priority": "P1",
        }),
    )
    .await
    .unwrap();
    let msg_id = post["message_id"].as_str().unwrap().to_string();
    assert!(msg_id.starts_with("msg_"));

    // receiver lists their inbox
    let listed = handle_inbox_list(state.clone(), json!({ "session_id": receiver }))
        .await
        .unwrap();
    let arr = listed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["message_id"], json!(msg_id));
    assert_eq!(arr[0]["from_session"], json!(sender));
    assert_eq!(arr[0]["subject"], json!("task ready"));
    assert_eq!(arr[0]["priority"], json!("P1"));

    // read by message id
    let read = handle_inbox_read(state.clone(), json!({ "message_id": msg_id }))
        .await
        .unwrap();
    assert_eq!(read["message_id"], json!(msg_id));
    assert_eq!(read["read"], json!(true));
}

#[tokio::test]
async fn state_set_get_round_trip() {
    let state = new_state();
    let id = register(&state, 7001, "claude", &[]).await;

    let set = handle_state_set(
        state.clone(),
        json!({
            "session_id": id,
            "status": "working",
            "focus_file": "/repos/pheno/crates/pheno-context/src/lib.rs",
            "focus_branch": "absorb/pheno-context-2026-07-17",
            "worktree": "/Users/kooshapari/.worktrees/pheno-context",
        }),
    )
    .await
    .unwrap();
    assert_eq!(set, json!({ "ok": true }));

    let got = handle_state_get(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();
    assert_eq!(got["session_id"], json!(id));
    assert_eq!(got["status"], json!("Working"));
    assert_eq!(
        got["focus_file"],
        json!("/repos/pheno/crates/pheno-context/src/lib.rs")
    );
    assert_eq!(
        got["focus_branch"],
        json!("absorb/pheno-context-2026-07-17")
    );
}

#[tokio::test]
async fn discover_agents_filters_by_path_and_capability() {
    let state = new_state();
    let id_a = register(
        &state,
        8001,
        "forge",
        &["search", "edit", "commit"],
    )
    .await;
    let id_b = register(&state, 8002, "codex", &["search"]).await;

    handle_state_set(
        state.clone(),
        json!({
            "session_id": id_a,
            "status": "working",
            "focus_file": "/repos/pheno/crates/pheno-context/src/lib.rs",
            "focus_branch": "main",
        }),
    )
    .await
    .unwrap();
    handle_state_set(
        state.clone(),
        json!({
            "session_id": id_b,
            "status": "working",
            "focus_file": "/repos/thegent/crates/thegent-cli/src/main.rs",
            "focus_branch": "main",
        }),
    )
    .await
    .unwrap();

    let all = handle_discover_agents(state.clone(), json!({}))
        .await
        .unwrap();
    assert_eq!(all.as_array().unwrap().len(), 2);

    let pheno_only = handle_discover_agents(
        state.clone(),
        json!({ "path": "/repos/pheno" }),
    )
    .await
    .unwrap();
    let arr = pheno_only.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["session_id"], json!(id_a));

    let commit_only = handle_discover_agents(
        state.clone(),
        json!({ "capabilities": ["commit"] }),
    )
    .await
    .unwrap();
    let arr = commit_only.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["session_id"], json!(id_a));

    let search_only = handle_discover_agents(
        state.clone(),
        json!({ "capabilities": ["search"] }),
    )
    .await
    .unwrap();
    assert_eq!(search_only.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn session_deregister_clears_live_state() {
    let state = new_state();
    let id = register(&state, 9001, "forge", &[]).await;

    handle_state_set(
        state.clone(),
        json!({
            "session_id": id,
            "status": "working",
            "focus_file": "/tmp/x.rs",
        }),
    )
    .await
    .unwrap();

    let live = handle_state_get(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();
    assert_eq!(live["status"], json!("Working"));

    handle_session_deregister(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap();

    let err = handle_state_get(state.clone(), json!({ "session_id": id }))
        .await
        .unwrap_err();
    assert!(matches!(err, teamcomm_daemon::TeamcommError::NotFound(_)));
}

#[tokio::test]
async fn smoke_app_state_default_is_empty() {
    let state = new_state();
    let listed = handle_session_list(state.clone(), json!({}))
        .await
        .unwrap();
    assert!(listed.as_array().unwrap().is_empty());

    tokio::time::sleep(Duration::from_millis(1)).await;

    let _: Value = listed;
}