# teamcomm JSON Schemas

This directory contains the formal JSON-Schema definitions for the teamcomm
wire protocol. Schemas mirror the Rust types in
`crates/teamcomm-protocol/src/` — they are the outward-facing interop
contract; Rust types are the implementation.

## Files

| Schema | Purpose |
|--------|---------|
| `jsonrpc-envelope.schema.json` | JSON-RPC 2.0 request, success response, error response |
| `errors.schema.json` | Numeric `error.code` catalog (`ErrorCode` enum) |
| `session.schema.json` | `session.register`, `session.deregister`, `session.heartbeat`, `session.list`, `session.get` |
| `reservation.schema.json` | `reservation.claim`, `reservation.release`, `reservation.list` |
| `inbox.schema.json` | `inbox.post`, `inbox.list`, `inbox.read` |
| `state.schema.json` | `state.set`, `state.get` |
| `discover.schema.json` | `discover.agents` |

## Wire transport

teamcomm carries JSON-RPC 2.0 frames over a Unix domain socket:

- Default socket path: `$XDG_RUNTIME_DIR/teamcomm/daemon.sock` (Linux) or
  `$TMPDIR/teamcomm/daemon.sock` (macOS).
- The PID file lives next to the socket as `daemon.pid`.
- All frames are length-delimited by the JSON-RPC framing rules
  (line-delimited newline for the current listener implementation).

## Versioning

- **Schema version**: the `$id` URL on each schema encodes the major version
  (`teamcomm://v0.1.0/...`).
- **Method additions** require a schema bump; clients MAY ignore unknown
  methods but MUST reject responses whose schema they cannot parse.
- **Field deprecations** flow through `note: deprecated` markers on the
  affected field; the field stays present for at least one minor version.

## Validation

To validate an incoming frame against these schemas:

```bash
# request
ajv validate -s schemas/jsonrpc-envelope.schema.json -d my-request.json \
  --spec=draft2020 -c ajv-formats

# session.register response
ajv validate -s schemas/session.schema.json -d register-result.json \
  --spec=draft2020 -c ajv-formats
```

(`ajv` ≥ 8.12 with `ajv-formats` provides the format keywords used here.)

## Cross-references

- Source of truth: `crates/teamcomm-protocol/src/` (Rust types).
- Handler dispatch: `crates/teamcomm-daemon/src/listener.rs`.
- Spec prose: `SPEC.md` (method narrative, semantic guarantees).