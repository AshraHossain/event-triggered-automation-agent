# Tutorial: GitOps Automation

## Overview

Triggered by GitHub `push` events to `refs/heads/main`, this workflow starts a CI pipeline (local function stub) and notifies the team on Slack (HTTP POST to webhook URL). Slack retries use fixed backoff; failures land in an in-memory DLQ queue.

## Key Config Highlights

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
PYTHONPATH=. python examples/gitops_automation/demo.py
```

The second sample event has `ref: refs/heads/feature/auth` and is correctly ignored (0 matching workflows).

## Expected Behaviour

1. `evt_git_001` matches → `start_ci` succeeds (stub returns pipeline ID) → `notify_slack` fails (fake URL, 404) → workflow marked `failed`.
2. `evt_git_002` does NOT match (branch filter) → no workflows run.
