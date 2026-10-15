"""Optional hot-key advisor API. Cache correctness does not depend on this service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import numpy as np
from fastapi import Body, FastAPI
from pydantic import BaseModel, Field
from threadpoolctl import threadpool_limits

from hotkey_advisor import FEATURES, load_model

model: Any = None
evaluation: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global model, evaluation
    model, evaluation = load_model()
    yield


app = FastAPI(title="Self-Healing Cache Hot Key Advisor", version="1.0.0", lifespan=lifespan)


class HotKeyObservation(BaseModel):
    node_id: str = Field(min_length=1, max_length=128)
    key: str = Field(min_length=1, max_length=4096)
    access_rate: float = Field(ge=0, le=1_000_000, description="Recent requests per second")
    read_ratio: float = Field(ge=0, le=1)
    node_load: float = Field(ge=0, le=10_000_000, description="Node requests per second")
    mean_latency_ms: float = Field(ge=0, le=60_000)
    accesses_60s: int = Field(ge=0)


def _recommend(observation: HotKeyObservation) -> dict[str, Any]:
    values = np.asarray([[getattr(observation, feature) for feature in FEATURES]], dtype=float)
    with threadpool_limits(limits=1):
        probability = float(model.predict_proba(values)[0, 1])
    hot = probability >= float(evaluation.get("probability_threshold", 0.5))
    read_heavy = observation.read_ratio >= 0.70
    if hot and read_heavy:
        recommendation = "consider_one_additional_replica"
        reason = "The model predicts sustained hot access and reads dominate writes."
    elif hot:
        recommendation = "consider_read_write_load_review"
        reason = "The key is predicted hot, but its write share may limit replica benefit."
    else:
        recommendation = "keep_current_replication"
        reason = "The predicted next-window hot-key probability is below the threshold."
    return {
        "node_id": observation.node_id,
        "key": observation.key,
        "hot_probability": round(probability, 4),
        "predicted_hot": hot,
        "recommendation": recommendation,
        "reason": reason,
        "observed": observation.model_dump(),
        "automatically_applied": False,
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "model_loaded": model is not None,
            "evaluation_dataset": evaluation.get("dataset")}


@app.get("/evaluation")
def get_evaluation() -> dict[str, Any]:
    return evaluation


@app.post("/predict")
def predict(observation: HotKeyObservation) -> dict[str, Any]:
    return _recommend(observation)


@app.post("/predict/batch")
def predict_batch(observations: list[HotKeyObservation] = Body(max_length=500)) -> dict[str, Any]:
    results = [_recommend(observation) for observation in observations]
    results.sort(key=lambda row: row["hot_probability"], reverse=True)
    return {"predictions": results, "count": len(results)}

