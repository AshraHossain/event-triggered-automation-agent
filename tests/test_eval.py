"""Tests for the classifier evaluation harness."""

from __future__ import annotations

import pytest

from evals.run_eval import evaluate, format_report, load_classifier_config, load_dataset

pytestmark = pytest.mark.unit


async def test_evaluate_scores_predictions_and_lists_misses():
    config = {
        "provider": "stub",
        "labels": ["billing", "general"],
        "default_label": "general",
        "stub_rules": {"billing": ["refund"]},
    }
    dataset = [
        {"text": "I want a refund", "label": "billing"},
        {"text": "hello there", "label": "general"},
        {"text": "my card was declined", "label": "billing"},  # no keyword → missed
    ]
    report = await evaluate(config, dataset)
    assert (report["correct"], report["total"]) == (2, 3)
    assert report["per_label"]["billing"] == {"precision": 1.0, "recall": 0.5, "support": 2}
    assert report["misses"][0]["predicted"] == "general"
    assert "2/3" in format_report(report)


async def test_offline_baseline_meets_its_gate_and_resists_injection():
    """The shipped rules stay above the regression gate on the shipped dataset."""
    dataset = load_dataset()
    report = await evaluate(load_classifier_config(), dataset)
    assert report["accuracy"] >= 0.6
    injected = [m for m in report["misses"] if "Ignore all previous" in m["text"]]
    assert injected == []  # the injection ticket is still classified as "general"


def test_provider_override_drops_the_model_pin():
    config = load_classifier_config(provider="anthropic")
    assert config["provider"] == "anthropic" and "model" not in config
    assert load_classifier_config(model="m1")["model"] == "m1"


def test_base_url_and_key_env_overrides():
    config = load_classifier_config(
        provider="openai", base_url="https://example.test/v1/", api_key_env="X_KEY"
    )
    assert config["base_url"] == "https://example.test/v1/" and config["api_key_env"] == "X_KEY"
    assert "base_url" not in load_classifier_config()
