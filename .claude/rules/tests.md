---
paths:
  - "tests/**/*.py"
---
# ETAA Testing Rules (`tests/`)
1. Tests run offline: `httpx.MockTransport` through `make_executor(transport)`, or respx.
   Provider tests inject fake `openai` / `anthropic` modules with `monkeypatch.setitem`.
2. Use the `make_executor` / `app_env` fixtures (isolated stores, no real backoff sleeps),
   not `build_application`, unless the test is about the CLI or the factory.
3. Files and databases go under `tmp_path`. Set env vars with `monkeypatch.setenv`.
4. Mark modules `pytest.mark.unit` or `pytest.mark.integration`.
5. Cover: idempotency (skip, release on failure), retry exhaustion → DLQ → replay, schema
   validation, and every security control (401 / 413 / 429, SSRF, allowlists).
