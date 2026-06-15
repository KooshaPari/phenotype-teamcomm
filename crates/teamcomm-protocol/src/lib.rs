// SPDX-License-Identifier: MIT OR Apache-2.0
//! `teamcomm-protocol` — wire types for inter-agent coordination.
//!
//! M0/M1 surface used by the daemon and its clients:
//!
//! - [`Session`], [`SessionRegistration`] — what an agent declares on
//!   `session.register`.
//! - [`LiveState`], [`AgentStatus`] — what an agent publishes on
//!   `state` (and incidentally in heartbeats).
//! - [`Reservation`], [`ReservationMode`] — advisory file locks.
//! - [`PathPattern`], [`CompiledPattern`] — glob-style reservation
//!   patterns (M2).
//! - [`Conflict`], [`ConflictReason`], [`mode_conflicts`] —
//!   structured conflict reporting (M2).
//! - [`InboxMessage`], [`Priority`] — durable, addressed messaging.
//! - [`Thread`], [`ThreadStatus`] — first-class conversation threads
//!   (M1).
//! - [`rpc`] — JSON-RPC 2.0 envelope types (`JsonRpcRequest`,
//!   `JsonRpcResponse`, `JsonRpcErrorResponse`, `RpcId`).
//! - [`TeamcommError`] — crate-level error type and JSON-RPC code map.

pub mod conflict;
pub mod error;
pub mod inbox;
pub mod path_pattern;
pub mod reservation;
pub mod rpc;
pub mod session;
pub mod state;
pub mod thread;

pub use conflict::{mode_conflicts, Conflict, ConflictReason};
pub use error::{error_code, ErrorCode, TeamcommError};
pub use inbox::{InboxMessage, Priority};
pub use path_pattern::{match_compile, CompiledPattern, PathPattern, PatternError};
pub use reservation::{Reservation, ReservationMode};
pub use rpc::{JsonRpcErrorResponse, JsonRpcRequest, JsonRpcResponse, RpcId};
pub use session::{AgentType, Session, SessionRegistration, SessionSummary};
pub use state::{AgentStatus, LiveState};
pub use thread::{CreateThreadRequest, Thread, ThreadDetails, ThreadListQuery, ThreadStatus};
