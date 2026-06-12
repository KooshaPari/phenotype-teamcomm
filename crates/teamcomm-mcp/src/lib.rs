//! teamcomm-mcp — Model Context Protocol server exposing teamcomm primitives.
//!
//! M0 stub: every tool returns a mocked successful response without touching
//! any real state. The wire format is JSON-RPC 2.0 over stdio (one request /
//! one response per line). See `mcp/manifest.json` for the tool catalogue and
//! `docs/PROTOCOL.md` (forthcoming) for the long-term protocol design.

pub mod dispatch;
pub mod handlers;
pub mod manifest;
