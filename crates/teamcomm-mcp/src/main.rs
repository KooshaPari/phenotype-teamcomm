//! teamcomm-mcp binary — JSON-RPC 2.0 server over stdio.
//!
//! Wire protocol: one request per line on stdin, one response per line on
//! stdout. Empty lines are ignored. EOF on stdin terminates the process.
//!
//! See `mcp/manifest.json` for the supported method catalogue. For M0 every
//! method returns a mocked successful payload; M1 will replace the dispatcher
//! with calls into the teamcomm daemon (or its client library).
//!
//! Tracing is initialised from the `RUST_LOG` env var (default `info`).
//! Diagnostic output goes to stderr, never stdout — stdout is reserved for
//! protocol responses.

use std::process::ExitCode;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use tracing_subscriber::{fmt, EnvFilter};

use teamcomm_mcp::dispatch;

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let stdin = stdin();
    let mut reader = BufReader::new(stdin);
    let mut writer = stdout();
    let mut line = String::new();

    loop {
        line.clear();
        let n = match reader.read_line(&mut line).await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "failed to read stdin");
                return ExitCode::FAILURE;
            }
        };
        if n == 0 {
            // EOF — graceful shutdown.
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let response = handle_request_line(trimmed).await;
        let serialised = match serde_json::to_string(&response) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialise response");
                continue;
            }
        };

        if let Err(e) = writer.write_all(serialised.as_bytes()).await {
            tracing::error!(error = %e, "failed to write response");
            return ExitCode::FAILURE;
        }
        if let Err(e) = writer.write_all(b"\n").await {
            tracing::error!(error = %e, "failed to write newline");
            return ExitCode::FAILURE;
        }
        if let Err(e) = writer.flush().await {
            tracing::error!(error = %e, "failed to flush stdout");
            return ExitCode::FAILURE;
        }
    }

    ExitCode::SUCCESS
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // try_init is fine — if another component already set up a subscriber we
    // simply skip (we never want tracing setup to crash the daemon).
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Parse a single line as a JSON-RPC 2.0 request, dispatch the method, and
/// return the response envelope. Always returns a complete envelope (never
/// `None`) so the caller can serialise it unconditionally.
async fn handle_request_line(line: &str) -> Value {
    let request: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return jsonrpc_error(Value::Null, -32700, "parse error", Some(e.to_string()));
        }
    };

    let id = request.get("id").cloned().unwrap_or(Value::Null);

    let method = match request.get("method").and_then(|m| m.as_str()) {
        Some(m) => m.to_string(),
        None => {
            return jsonrpc_error(id, -32600, "invalid request: missing method", None);
        }
    };

    if method.is_empty() {
        return jsonrpc_error(id, -32600, "invalid request: empty method", None);
    }

    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

    // JSON-RPC 2.0 spec: a Notification is a request with no `id` member. We
    // still process it (the dispatcher is cheap and the M0 mock handlers
    // have no observable side effects) but we don't emit a response. This
    // keeps us compatible with `initialize`-style handshakes and pings.
    if !request.as_object().map(|o| o.contains_key("id")).unwrap_or(false) {
        let _ = dispatch::dispatch(&method, params).await;
        return Value::Null;
    }

    let result = dispatch::dispatch(&method, params).await;

    if let Some(error) = result.get("error") {
        // The dispatcher signalled a JSON-RPC error — promote it into the
        // response envelope. We strip the outer `error` key from the dispatch
        // value to avoid double-nesting.
        let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(-32603);
        let message = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("internal error")
            .to_string();
        let data = error.get("data").cloned();
        jsonrpc_error(id, code, &message, data.as_ref().map(|v| v.to_string()))
    } else {
        jsonrpc_result(id, result)
    }
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn jsonrpc_error(id: Value, code: i64, message: &str, data: Option<String>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message,
    });
    if let Some(d) = data {
        error["data"] = Value::String(d);
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn handles_known_method() {
        let resp = handle_request_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"register_session","params":{}}"#,
        )
        .await;
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        let result = resp.get("result").expect("result present");
        assert!(result["session_id"].as_str().unwrap().starts_with("sess_"));
        assert_eq!(result["lease_ttl_sec"], 90);
    }

    #[tokio::test]
    async fn handles_unknown_method_with_method_not_found() {
        let resp = handle_request_line(
            r#"{"jsonrpc":"2.0","id":7,"method":"nope","params":{}}"#,
        )
        .await;
        assert_eq!(resp["id"], 7);
        let err = resp.get("error").expect("error present");
        assert_eq!(err["code"], -32601);
        assert_eq!(err["message"], "method not found");
    }

    #[tokio::test]
    async fn handles_malformed_json_with_parse_error() {
        let resp = handle_request_line("not json").await;
        let err = resp.get("error").expect("error present");
        assert_eq!(err["code"], -32700);
        assert_eq!(err["message"], "parse error");
    }

    #[tokio::test]
    async fn handles_missing_method_with_invalid_request() {
        let resp = handle_request_line(r#"{"jsonrpc":"2.0","id":2}"#).await;
        let err = resp.get("error").expect("error present");
        assert_eq!(err["code"], -32600);
    }

    #[tokio::test]
    async fn handles_notification_without_id() {
        // Notification: no `id` member. We process but emit no response.
        let resp = handle_request_line(
            r#"{"jsonrpc":"2.0","method":"deregister_session","params":{}}"#,
        )
        .await;
        assert_eq!(resp, Value::Null);
    }

    #[tokio::test]
    async fn defaults_missing_params_to_empty_object() {
        let resp = handle_request_line(r#"{"jsonrpc":"2.0","id":3,"method":"list_sessions"}"#).await;
        assert_eq!(resp["id"], json!(3));
        assert_eq!(resp["result"], json!([]));
    }
}
