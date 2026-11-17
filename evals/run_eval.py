"""Evaluate the ticket classifier against a labelled set.

Runs the ``classify_ticket`` action from the support workflow over
``evals/tickets.jsonl`` and reports accuracy plus per-label precision/recall.
The default provider is the offline stub (keyword rules), so this needs no API
key and is deterministic; pass ``--provider anthropic`` or ``--provider openai``
to score a real model with the same prompt and label set.

    python -m evals.run_eval                       # offline baseline
    python -m evals.run_eval --provider anthropic  # needs ANTHROPIC_API_KEY
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import yaml

from etaa.actions import AiAction
from etaa.models import ActionKind, ActionSpec, EventEnvelope

ROOT = Path(__file__).resolve().parent
WORKFLOW = ROOT.parent / "examples" / "support_ticket_flow" / "workflow.yaml"
DATASET = ROOT / "tickets.jsonl"
RESULTS = ROOT / "results.json"
# Regression guard for the offline keyword baseline (62.5% when written). The rules
# are deliberately not tuned to this dataset. Use --min-accuracy 0.9 for a real model.
DEFAULT_MIN_ACCURACY = 0.6


def load_classifier_config(
    provider: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key_env: str | None = None,
    max_tokens: int | None = None,
) -> dict:
    """Return the AI action config used by the support workflow (optionally overridden)."""
    spec = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    config = next(a["config"] for a in spec["actions"] if a["id"] == "classify_ticket")
    if provider:
        config = {**config, "provider": provider}
        config.pop("model", None)
    if model:
        config = {**config, "model": model}
    if base_url:
        config = {**config, "base_url": base_url}
    if api_key_env:
        config = {**config, "api_key_env": api_key_env}
    if max_tokens:
        config = {**config, "max_tokens": max_tokens}
    return config


def load_dataset(path: Path = DATASET) -> list[dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


async def evaluate(config: dict[str, Any], dataset: list[dict[str, str]]) -> dict[str, Any]:
    """Classify every ticket and return accuracy, per-label metrics and the misses."""
    action = ActionSpec(id="classify_ticket", type=ActionKind.AI, config=config)
    classifier = AiAction()
    predictions: list[str] = []
    for i, row in enumerate(dataset):
        event = EventEnvelope(
            event_id=f"eval-{i}",
            source="eval",
            type="ticket_created",
            payload={"description": row["text"]},
        )
        result = await classifier.execute(action, event)
        predictions.append((result.response or {})["classification"])

    labels = sorted({row["label"] for row in dataset} | set(config.get("labels", [])))
    per_label: dict[str, dict[str, float]] = {}
    for label in labels:
        tp = sum(p == label == r["label"] for p, r in zip(predictions, dataset, strict=True))
        predicted = predictions.count(label)
        actual = sum(r["label"] == label for r in dataset)
        per_label[label] = {
            "precision": round(tp / predicted, 3) if predicted else 0.0,
            "recall": round(tp / actual, 3) if actual else 0.0,
            "support": actual,
        }
    misses = [
        {"text": r["text"], "expected": r["label"], "predicted": p}
        for p, r in zip(predictions, dataset, strict=True)
        if p != r["label"]
    ]
    return {
        "provider": config.get("provider", "stub"),
        "model": config.get("model", "stub"),
        "total": len(dataset),
        "correct": len(dataset) - len(misses),
        "accuracy": round((len(dataset) - len(misses)) / len(dataset), 3),
        "per_label": per_label,
        "misses": misses,
    }


def format_report(report: dict[str, Any]) -> str:
    lines = [
        f"Provider: {report['provider']}   "
        f"Accuracy: {report['correct']}/{report['total']} = {report['accuracy']:.1%}",
        "",
        "| Label | Precision | Recall | Tickets |",
        "|---|---|---|---|",
    ]
    for label, m in report["per_label"].items():
        lines.append(f"| {label} | {m['precision']:.2f} | {m['recall']:.2f} | {m['support']} |")
    if report["misses"]:
        lines += ["", "Misclassified:"]
        lines += [
            f"  expected {m['expected']:<9} got {m['predicted']:<9} {m['text']}"
            for m in report["misses"]
        ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the support-ticket classifier")
    parser.add_argument("--provider", choices=AiAction.PROVIDERS, help="Override the provider")
    parser.add_argument("--model", help="Override the model name")
    parser.add_argument("--base-url", help="OpenAI-compatible endpoint (e.g. Gemini)")
    parser.add_argument("--api-key-env", help="Env var holding the key (default OPENAI_API_KEY)")
    parser.add_argument("--max-tokens", type=int, help="Raise for reasoning models")
    parser.add_argument("--min-accuracy", type=float, default=DEFAULT_MIN_ACCURACY)
    args = parser.parse_args()

    config = load_classifier_config(
        args.provider, args.model, args.base_url, args.api_key_env, args.max_tokens
    )
    report = asyncio.run(evaluate(config, load_dataset()))
    print(format_report(report))
    RESULTS.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["accuracy"] < args.min_accuracy:
        print(f"\nFAIL: accuracy below the {args.min_accuracy:.0%} gate")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
