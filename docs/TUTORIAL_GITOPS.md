# Tutorial: GitOps Automation

## Overview

Triggered by GitHub `push` events to `refs/heads/main`, this workflow starts a CI pipeline
(local function) and notifies the team on Slack (HTTP). Slack retries use fixed backoff;
exhausted retries are written to `./data/dlq/gitops.jsonl`.

## Key config

```yaml
triggers:
  - source: github
    type: push
    filters:
      ref: "refs/heads/main"     # only main-branch pushes

actions:
  - id: start_ci
    type: function
    config:
      module_path: examples.gitops_automation.handlers.start_ci

  - id: notify_slack
    type: http
    config:
      url: "https://hooks.slack.com/services/EXAMPLE/WEBHOOK/URL"
      body: { text: "New push on {{source}} repo. Commit: {{payload.head_commit.id}}" }
    retry_policy:
      max_attempts: 3
      backoff_strategy: fixed
      backoff_seconds: 2.0
```

## Run

```bash
PYTHONPATH=. uv run python examples/gitops_automation/demo.py
```

## Expected behaviour

1. `evt_git_001` (push to `main`) matches → `start_ci` returns a pipeline id → `notify_slack`
   is sent to the demo's mock endpoint → run ends in `success`.
2. `evt_git_002` (push to `feature/auth`) does not match the branch filter → no workflow runs.
