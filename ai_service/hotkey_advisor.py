"""Hot-key scoring model and reproducible synthetic time-split evaluation."""

from __future__ import annotations

import json
import math
import os
import random
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

FEATURES = ("access_rate", "read_ratio", "node_load", "mean_latency_ms")
ARTIFACT_DIR = Path(os.environ.get("CACHE_AI_ARTIFACT_DIR", Path(__file__).resolve().parent / "artifacts"))
MODEL_PATH = ARTIFACT_DIR / "hotkey_model.joblib"
EVALUATION_PATH = ARTIFACT_DIR / "evaluation.json"
DATASET_DESCRIPTION = "Seeded synthetic cache trace; labels describe next-window hot state."


def generate_trace(seed: int = 19, keys: int = 40, windows: int = 300) -> tuple[np.ndarray, np.ndarray]:
    """Generate noisy, stateful per-key windows with a future-window label."""
    rng = random.Random(seed)
    rows: list[list[float]] = []
    labels: list[int] = []
    all_states: list[list[bool]] = []
    for _key in range(keys):
        state = rng.random() < 0.18
        history: list[bool] = []
        for _window in range(windows):
            if rng.random() < 0.04:
                state = not state
            history.append(state)
        all_states.append(history)
    for index in range(windows - 1):
        for key_index in range(keys):
            state = all_states[key_index][index]
            next_state = all_states[key_index][index + 1]
            if state:
                rate = rng.lognormvariate(math.log(22), 0.48)
                read_ratio = min(1.0, max(0.0, rng.gauss(0.87, 0.10)))
            else:
                rate = rng.lognormvariate(math.log(1.2), 0.85)
                read_ratio = min(1.0, max(0.0, rng.gauss(0.58, 0.24)))
            node_load = min(220.0, max(0.0, rng.gauss(45 + rate * 1.8, 18)))
            latency = min(500.0, max(0.05, rng.gauss(0.25 + node_load * 0.025 + rate * 0.012, 0.18)))
            rows.append([rate, read_ratio, node_load, latency])
            labels.append(int(next_state))
    return np.asarray(rows, dtype=float), np.asarray(labels, dtype=int)


def baseline_predict(features: np.ndarray) -> np.ndarray:
    """A transparent rate-and-read-ratio rule used as the comparison baseline."""
    return ((features[:, 0] >= 10.0) & (features[:, 1] >= 0.70)).astype(int)


def _scores(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    return {
        "accuracy": round(float(accuracy_score(actual, predicted)), 4),
        "precision": round(float(precision_score(actual, predicted, zero_division=0)), 4),
        "recall": round(float(recall_score(actual, predicted, zero_division=0)), 4),
        "f1": round(float(f1_score(actual, predicted, zero_division=0)), 4),
    }


def train_and_evaluate() -> tuple[Any, dict[str, Any]]:
    """Train on earlier trace windows and evaluate on the later time segment."""
    features, labels = generate_trace()
    split = int(len(labels) * 0.8)
    model = make_pipeline(StandardScaler(), LogisticRegression(
        max_iter=1000, class_weight="balanced", solver="liblinear"))
    with threadpool_limits(limits=1):
        model.fit(features[:split], labels[:split])
        train_probabilities = model.predict_proba(features[:split])[:, 1]
        probabilities = model.predict_proba(features[split:])[:, 1]
    thresholds = [step / 100 for step in range(20, 81, 2)]
    probability_threshold = max(thresholds, key=lambda threshold: (
        f1_score(labels[:split], train_probabilities >= threshold), -threshold))
    model_predictions = (probabilities >= probability_threshold).astype(int)
    baseline = baseline_predict(features[split:])
    report = {
        "dataset": DATASET_DESCRIPTION,
        "split": "first 80% of ordered time windows for training; final 20% held out",
        "rows": {"total": int(len(labels)), "train": int(split), "held_out": int(len(labels) - split)},
        "hot_fraction_held_out": round(float(labels[split:].mean()), 4),
        "probability_threshold": probability_threshold,
        "logistic_regression": _scores(labels[split:], model_predictions),
        "rule_baseline": _scores(labels[split:], baseline),
        "limitations": "Synthetic workload only; results do not estimate production accuracy.",
    }
    return model, report


def load_model() -> tuple[Any, dict[str, Any]]:
    import joblib

    if MODEL_PATH.exists() and EVALUATION_PATH.exists():
        return joblib.load(MODEL_PATH), json.loads(EVALUATION_PATH.read_text())
    model, report = train_and_evaluate()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    EVALUATION_PATH.write_text(json.dumps(report, indent=2) + "\n")
    return model, report

