# CLAUDE.md — Event-Triggered Automation Agent (ETAA)

Read [AGENTS.md](AGENTS.md) for the project overview, commands, layout, and core rules.

- Path-scoped rules: `.claude/rules/` (linked to `.agents/rules/`)
- Repo-specific skills: `.claude/skills/` (linked to `.agents/skills/`)

## Release gates

Before calling work done, merging, tagging or pushing, run the global
`enterprise-release-gates` skill (check → fix → re-check, evidence required). In this repo
the gates map to:

| Gate | Check here |
|---|---|
| G2 static, G3 tests, G6 scans | `make check` (ruff, black, mypy, bandit, pip-audit, pytest ≥ 90%) |
| G4 runs as documented | `make demo`, `make benchmark`, `make eval`, and the README commands |
| G8 LLM evaluation | `make eval` (labelled tickets, accuracy gate) |
| G9 container | `docker build -t etaa .` — report `not verified` if the daemon is not running |
| G11 CI on remote | `gh run list --branch main --limit 1` — `not verified` until pushed |
| G12 delivery | `graphify update .`, merge `--no-ff`, tag; push only after the user confirms |

## Commands that run without a permission prompt

`.claude/settings.json` allowlists read-only commands by **exact form**. Use these forms
as written; a variant (extra flag, different path order) will prompt.

| Purpose | Command |
|---|---|
| Full verification | `make check` |
| Lint, whole repo | `ruff check .` or `ruff check . -q` |
| Lint, package only | `ruff check etaa -q` or `ruff check etaa --output-format concise` |
| Lint, package + tests | `ruff check etaa tests --output-format concise` |
| Lint, everything listed | `ruff check etaa tests benchmark.py examples --output-format concise` |
| Format check | `black --check -q .` |
| Tool present? | `command -v <tool>` |
| Docker daemon up? | `docker info` |

Rules that follow from the allowlist:

- **Verify with `make check`**, not ad-hoc `python -m pytest` / `mypy` / `bandit` calls.
  Interpreter commands are never allowlisted and always prompt.
- **Never add `--fix` to an allowlisted ruff command.** Fixing rewrites files, so it is a
  separate, prompted step (`ruff check . --fix`, then `black .`).
- **Run one allowlisted command per call.** Chaining it with a non-allowlisted command
  (`&&`, `;`, `|`) makes the whole call prompt.
- **Run `git commit` on its own.** A command containing both `git commit` and a `-n` flag
  anywhere (for example a chained `grep -n`) is blocked by the no-verify hook.
- **Do not widen the allowlist** to wildcards for `python`, `uv run`, `make *`, `curl`,
  `ruff check *` or `git` write commands. Add a new entry only for an exact, read-only
  command that has been needed several times.
