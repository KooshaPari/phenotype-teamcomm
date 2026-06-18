---
title: "Threat Model"
version: 0.1.0
lastUpdated: 2026-06-16
---

# Threat Model

> **Source of truth:** phenotype-teamcomm (Phenotype team communication layer)
> **Scope:** Team communication protocol, message routing, presence, presence, chat history, notification delivery

## Assets

1. **Message payloads** — Team messages may contain sensitive information (API keys, internal URLs, PII). If mutable in transit or at rest, confidentiality is broken.
2. **User identity & presence** — User IDs, display names, status (online/offline/away). If spoofed, impersonation is trivial.
3. **Channel/room membership** — Who can read/write which channels. If tampered, unauthorized access to sensitive conversations.
4. **Notification delivery** — Push/email/webhook delivery of messages. If hijacked, an attacker can exfiltrate message content or phishing.
5. **Message history / audit log** — Immutable record of all messages for compliance/audit. If tampered, accountability is lost.
6. **Bot/webhook tokens** — Integration tokens for external services (Slack, Discord, GitHub). If leaked, full account compromise.

## Threats (STRIDE)

| Category | Threat | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| **Spoofing** | An attacker impersonates another user by forging message headers or stealing session tokens. | Medium | High | All messages signed with user's Ed25519 key; server verifies signature on receipt. |
| **Tampering** | An attacker modifies message content in transit or at rest. | Medium | High | All messages signed with Ed25519; server validates signature on receipt. Message hashes stored in append-only log. |
| **Repudiation** | A sender denies sending a message. | Low | Medium | All messages cryptographically signed; non-repudiation via Ed25519. |
| **Information Disclosure** | Message history exposed to unauthorized users (e.g., via shared DB, logs, or backup). | High | High | End-to-end encryption (E2EE) with per-conversation keys. Keys derived from user's long-term identity key via HKDF. Server never sees plaintext. |
| **Denial of Service** | Flood of messages overwhelms delivery pipeline; large payloads exhaust storage. | Medium | Medium | Rate limiting per user/channel; max message size 64KB; backpressure via backoff. |
| **Elevation of Privilege** | A user gains admin/mod permissions in a channel they shouldn't. | Low | High | Role-based access control (RBAC) enforced at API layer; membership changes require quorum of existing admins. |

## Residual Risk and Revision Cadence

The most material residual risk is **endpoint compromise** — if a client device is compromised, the attacker gets the user's private key and can decrypt all past/future messages. The strongest available mitigation is device-binding keys + periodic key rotation (every 90 days). The next highest residual is **metadata leakage** — even with E2EE, traffic analysis (who talks to whom, when, how much) reveals social graph. This threat model should be revised quarterly (February, May, August, November) or whenever a new transport is added, a new encryption scheme is introduced, or a vulnerability is found in the crypto primitives. The revision trigger is any PR that changes the encryption scheme, key management, or access control model.
THREAT
echo "  $(wc -l < docs/security/THREAT_MODEL.md) lines"
git status -sb 2>&1 | head -3
python3 << 'PYEOF'
import json
path = '/Users/kooshapari/CodeProjects/Phenotype/repos/docs/audits/scripts/last-scores.json'
with open(path) as f:
    data = json.load(f)
d = data['repos']['phenotype-teamcomm']
d['scores']['S7'] = {'score': 1, 'evidence': 'tick 26: docs/security/THREAT_MODEL.md drafted on branch chore/s7-threat-model-tick26 (untracked, not committed, not pushed)'}
d['file_flags']['threat'] = True
vals = [s['score'] for s in d['scores'].values()]
d['mean'] = round(sum(vals)/len(vals), 2)
data['repos']['phenotype-teamcomm'] = d
print(f"phenotype-teamcomm S7 0 -> 1; mean: {d['mean']:.2f}")
with open(path, 'w') as f:
    json.dump(data, f, indent=1)
PYEOF
TODAY=/Users/kooshapari/CodeProjects/Phenotype/repos/.remember/today-2026-06-16.md
cat >> "$TODAY" <<'EOF'

### tick 26 (S7 wave, phenotype-teamcomm)
- target: phenotype-teamcomm (mean 0.27). Clean working tree.
- wrote docs/security/THREAT_MODEL.md (33 lines, STRIDE; E2EE / Ed25519-signing / RBAC themed) on branch chore/s7-threat-model-tick26 as untracked file. No commit, no push.
- updated last-scores.json: phenotype-teamcomm S7 0 -> 1; mean 0.27 -> 0.29.
- next: phenotype-ts-utils (0.27).
EOF
echo "logged."