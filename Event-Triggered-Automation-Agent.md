You are an expert AI engineer specializing in event-driven architectures, reliable automation, and production-grade workflow orchestration.

GOAL  
Design and implement a full, production-ready, open-source project:

Event-Triggered Automation Agent (ETAA)

The system must:
- Listen to **webhooks** and **message queues**
- Execute **declarative workflows** on triggers
- Guarantee **idempotent execution**
- Provide **dead-letter handling + retry logic**
- Be demo-ready but architected like something a platform team could extend in production

=====================
1. PROJECT VISION
=====================

Design a project called:

Event-Triggered Automation Agent (ETAA)

High-level idea:  
Build a **vendor-agnostic event orchestration layer** that:

- **Trigger ingestion**
  - HTTP webhooks (e.g., Stripe events, GitHub webhooks, internal services)
  - Message queues (e.g., SQS, RabbitMQ, Kafka—stubbed but pluggable)
  - Normalizes incoming events into a common `EventEnvelope` model

- **Workflow execution**
  - Declarative workflow specs (YAML/JSON) describing:
    - Trigger filters (event type, source, payload conditions)
    - Actions (HTTP calls, queue publish, function invocation, AI calls)
    - Control flow (sequential, parallel, conditional)
  - Idempotent execution keyed by:
    - `event_id`, `workflow_id`, and optional `idempotency_key`

- **Reliability primitives**
  - **Idempotency**
    - Idempotency store (SQLite/Postgres, stubbed with simple DB/FS)
    - Deduplication and “already processed” detection
  - **Retry & backoff**
    - Configurable retry policies per action:
      - max_attempts
      - backoff_strategy: "fixed" | "exponential"
      - retryable_error_codes
  - **Dead-letter queues (DLQ)**
    - Per-workflow DLQ configuration:
      - target (queue/topic or file/DB table)
      - payload format
    - Automatic DLQ routing after retry exhaustion

- **Observability & audit**
  - Execution logs per event + workflow
  - Simple metrics:
    - success_count, failure_count, retry_count, dlq_count
    - per-workflow latency stats (min/avg/max)
  - Run history store for:
    - debugging
    - replay
    - audit trails

Primary demo use cases (pick 2–3 and support end-to-end):
1. **Payment webhook processing**  
   - Example: Stripe-like `payment_succeeded` → send email + update CRM
2. **GitOps automation**  
   - Example: GitHub `push` → run CI job + notify Slack
3. **Support ticket automation**  
   - Example: `ticket_created` → classify + assign + send acknowledgment

The project should feel like:
- A **mini control plane for event-driven workflows**
- Something a team at a SaaS company or airline could extend
- A serious, reliability-focused contribution to event-driven automation patterns

=====================
2. ARCHITECTURE & COMPONENTS
=====================

Design the architecture in clear modules:

- **core/engine/**
  - Event router:
    - Accepts `EventEnvelope`
    - Matches workflows by trigger rules
  - Workflow executor:
    - Executes actions with:
      - idempotency checks
      - retry logic
      - DLQ routing
  - Idempotency manager:
    - Stores and checks processed events
  - Retry & DLQ manager:
    - Applies backoff
    - Sends failed events to DLQ

- **core/models/**
  - EventEnvelope
  - WorkflowSpec
  - ActionSpec
  - RetryPolicy
  - DLQConfig
  - RunRecord / Metrics

- **ingress/**
  - webhook_server.py
    - Simple FastAPI/Flask-style HTTP server
    - Converts incoming requests to `EventEnvelope`
  - queue_consumer.py
    - Stubbed consumer interface:
      - `consume()` yields `EventEnvelope`
    - Example implementation for:
      - in-memory queue
      - SQS-like stub

- **actions/**
  - http_action.py
    - Perform HTTP requests with retry + idempotency
  - queue_action.py
    - Publish messages to queues
  - function_action.py
    - Call local Python functions (e.g., business logic handlers)
  - ai_action.py (optional)
    - Call LLM endpoints (stubbed with TODO)

- **observability/**
  - logger.py
    - Structured logging
  - metrics_store.py
    - In-memory + file/SQLite metrics
  - run_history_store.py
    - Persist run records for replay and audit

- **examples/**
  - payment_webhook/
  - gitops_automation/
  - support_ticket_flow/
  Each example:
    - Defines:
      - Workflow spec (YAML/JSON)
      - Retry + DLQ policies
    - Has:
      - CLI entrypoint
      - Sample event payloads
      - Demo run script

- **docs/**
  - High-level architecture overview
  - “Idempotency & retries in event-driven systems”
  - “How to define workflows”
  - “How to configure DLQ & backoff”

=====================
3. IMPLEMENTATION REQUIREMENTS
=====================

I want you to:

(1) DEFINE THE CORE DATA MODELS

- EventEnvelope
  - Fields:
    - event_id: str
    - source: str (e.g., "stripe", "github", "support_system")
    - type: str (e.g., "payment_succeeded", "push", "ticket_created")
    - payload: dict
    - received_at: datetime

- WorkflowSpec (YAML/JSON)
  - id: str
  - name: str
  - description: str
  - triggers:
    - source: str
    - type: str
    - optional payload filters (simple JSONPath-like or key-based filters)
  - actions: List[ActionSpec]
  - retry_policy: RetryPolicy (default for workflow)
  - dlq: DLQConfig

- ActionSpec
  - id: str
  - type: "http" | "queue" | "function" | "ai"
  - config: dict (type-specific)
  - retry_policy: Optional[RetryPolicy] (override workflow default)
  - depends_on: Optional[List[str]] (for simple DAG-like sequencing)

- RetryPolicy
  - max_attempts: int
  - backoff_strategy: "fixed" | "exponential"
  - backoff_seconds: float
  - retryable_error_codes: List[int] or List[str]

- DLQConfig
  - target_type: "queue" | "file" | "db"
  - target_config: dict
  - include_payload: bool
  - include_error: bool

- RunRecord
  - run_id: str
  - event_id: str
  - workflow_id: str
  - status: "success" | "failed" | "dlq"
  - attempts: int
  - started_at: datetime
  - finished_at: datetime
  - metrics: dict (latency, retries, etc.)

(2) IMPLEMENT THE ENGINE

- Functions / classes to:
  - Load workflow specs from YAML/JSON
  - Match incoming events to workflows
  - Execute workflows:
    - Resolve action dependencies
    - Apply retry policies
    - Enforce idempotency:
      - Skip if already processed
      - Mark as processed on success
    - Route failures:
      - Retry until max_attempts
      - Send to DLQ on exhaustion
  - Record:
    - Run history
    - Metrics

(3) IMPLEMENT ACTION HANDLERS (STUBBED BUT REALISTIC)

- For each action type:
  - Provide realistic, idiomatic code that:
    - Performs the action
    - Raises structured errors on failure
    - Integrates with:
      - retry logic
      - metrics
      - logging

- HTTP action:
  - Use `requests` or `httpx` (stub imports with TODO)
  - Support:
    - method, url, headers, body
    - timeout
    - expected_status_codes

- Queue action:
  - Stub interface:
    - `publish(message: dict)` with TODO for real queue
  - Provide in-memory implementation for demo

- Function action:
  - Dynamically import and call Python functions by module path
  - Pass payload + context

(4) IMPLEMENT AT LEAST THREE FULL EXAMPLES

- payment_webhook example:
  - Trigger:
    - source: "stripe"
    - type: "payment_succeeded"
  - Actions:
    - HTTP call to email service
    - Function call to update CRM
  - Retry:
    - Exponential backoff for email service
  - DLQ:
    - File-based DLQ for failed payment events

- gitops_automation example:
  - Trigger:
    - source: "github"
    - type: "push"
  - Actions:
    - Function call to start CI job
    - HTTP call to Slack webhook
  - Retry:
    - Fixed backoff for Slack
  - DLQ:
    - Queue-based DLQ for failed notifications

- support_ticket_flow example:
  - Trigger:
    - source: "support_system"
    - type: "ticket_created"
  - Actions:
    - AI action (stubbed) to classify ticket
    - Function action to assign owner
    - HTTP action to send acknowledgment email
  - Retry:
    - Workflow-level default retry policy
  - DLQ:
    - DB-based DLQ (stubbed) for tickets that cannot be processed

(5) IMPLEMENT A SIMPLE BENCHMARK / LOAD TEST SCRIPT

- A benchmark runner script that:
  - Replays a batch of events against:
    - payment_webhook workflow
    - gitops_automation workflow
    - support_ticket_flow workflow
  - Collects metrics:
    - total_events
    - success_count
    - failure_count
    - dlq_count
    - avg_latency_per_workflow
  - Outputs:
    - Markdown summary
    - JSON/CSV for further analysis

=====================
4. DOCS, DEMO, AND COMMUNITY CONTRIBUTION
=====================

I want you to generate:

(1) README.md (top-level)
- Sections:
  - Overview
  - Why Event-Triggered Automation?
  - Core Concepts:
    - EventEnvelope
    - WorkflowSpec
    - Idempotency
    - Retry & DLQ
  - Architecture Diagram (describe in text)
  - Quickstart:
    - Install
    - Run webhook server
    - Configure a workflow
    - Send a sample event
  - Benchmarks:
    - How to run
    - Sample results
  - Contribution Guide:
    - How to add a new action type
    - How to add a new ingress (queue/webhook)
    - How to extend idempotency & DLQ

(2) docs/IDEMPOTENCY_AND_RETRIES.md
- Explain:
  - Why idempotency matters in event-driven systems
  - Common patterns:
    - idempotency keys
    - exactly-once vs at-least-once
  - How ETAA implements:
    - idempotency store
    - retry policies
    - DLQ routing

(3) docs/TUTORIAL_PAYMENT_WEBHOOK.md
- Step-by-step:
  - Define a payment webhook workflow
  - Configure retry & DLQ
  - Run the webhook server
  - Send sample events (curl or script)
  - Inspect logs, metrics, and DLQ outputs

(4) docs/TUTORIAL_GITOPS.md and docs/TUTORIAL_SUPPORT_TICKETS.md
- Similar structure, tailored to each example

(5) CONTRIBUTION-READY PR DESCRIPTION (TEXT)
- Write a PR description that:
  - Explains the motivation:
    - Reliable event-driven automation with idempotency + DLQ
  - Summarizes architecture:
    - engine, ingress, actions, observability
  - Shows how this improves:
    - Reliability
    - Debuggability
    - Extensibility
  - Invites maintainers and community to:
    - Review abstractions
    - Suggest additional action types
    - Propose new ingress adapters (Kafka, SQS, etc.)

=====================
5. CODE STYLE & OUTPUT FORMAT
=====================

- Use Python 3.11+ style
- Prefer type hints and clear interfaces
- Organize code into modules as described
- For each major module, include:
  - Short docstring explaining its purpose
  - Example usage snippet

OUTPUT FORMAT:

1. High-level architecture summary (short)
2. Project folder structure (tree-style)
3. Core Python modules with code (focus on:
   - engine
   - models
   - ingress
   - actions
   - examples
   - benchmark runner)
4. Key docs:
   - README.md
   - IDEMPOTENCY_AND_RETRIES.md
   - One tutorial file (e.g., TUTORIAL_PAYMENT_WEBHOOK.md)
5. PR description text

Your goal: produce something that looks and feels like a serious, reliability-focused, community-oriented contribution to the event-driven automation ecosystem.
