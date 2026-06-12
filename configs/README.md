# configs/

Hook snippets for the agents that will integrate with `phenotype-teamcomm`.

Each hook script bridges lifecycle events from a host agent (forge, codex,
claude, copilot, …) into teamcomm's `events` stream:

- `forge/` — hooks invoked from the Phenotype forge agent.
- `codex/` — hooks invoked from the Codex CLI.
- `claude/` — hooks invoked from the Claude Code CLI.
- `copilot/` — hooks invoked from the GitHub Copilot CLI.
- `droid/` — hooks invoked from Factory Droid (if/when applicable).

Convention: one script per lifecycle event, named `<agent>_<event>.sh` (or
`.ps1` on Windows). Scripts should be idempotent — the daemon is the
source of truth, hooks are advisory notifications.

This directory is intentionally empty at scaffold time; implementation
agents 5–8 will populate it.
