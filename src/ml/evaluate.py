import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                             r2_score, roc_auc_score, accuracy_score,
                             precision_score, recall_score)

from ml.data_loader import load_all_calendars
from ml.features import build_features, add_lag_features, add_wait_target

MODELS = Path(__file__).resolve().parents[2] / "models"


def _build_data():
    """Reproduce the training pipeline and return the temporal test split."""
    cal = load_all_calendars()
    cal = build_features(cal)
    cal = add_lag_features(cal)
    cal = add_wait_target(cal)

    # Recent trend feature (as in training)
    cal["trend"] = cal["price_lag1"] - cal["price_lag7"]

    split_date = cal["collected_date"].quantile(0.8)
    test = cal[cal["collected_date"] > split_date].copy()
    test["route"] = test["route"].astype("category")
    return test


def evaluate_and_save():
    """Run inference on the test set and persist metrics + importance."""
    meta = joblib.load(MODELS / "metadata.joblib")
    reg = joblib.load(MODELS / "price_regressor.joblib")
    clf = joblib.load(MODELS / "wait_classifier.joblib")
    threshold = meta.get("wait_threshold", 0.45)

    test = _build_data()

    # --- Regressor ---
    Xr = test[meta["price_features"]]
    yr = test["price"]
    price_pred = reg.predict(Xr)
    reg_metrics = {
        "mae": float(mean_absolute_error(yr, price_pred)),
        "rmse": float(np.sqrt(mean_squared_error(yr, price_pred))),
        "r2": float(r2_score(yr, price_pred)),
        "mape": float(np.mean(np.abs((yr - price_pred) / yr)) * 100),
    }

    # --- Classifier ---
    Xc = test[meta["clf_features"]]
    yc = test["should_wait"]
    proba = clf.predict_proba(Xc)[:, 1]
    pred = (proba >= threshold).astype(int)
    clf_metrics = {
        "auc": float(roc_auc_score(yc, proba)),
        "accuracy": float(accuracy_score(yc, pred)),
        "precision": float(precision_score(yc, pred, zero_division=0)),
        "recall": float(recall_score(yc, pred, zero_division=0)),
    }

    metrics = {
        "regressor": reg_metrics,
        "classifier": clf_metrics,
        "evaluated_at": pd.Timestamp.now().isoformat(),
        "n_test": int(len(test)),
        "trained_at": meta.get("trained_at", "unknown"),
    }
    (MODELS / "metrics.json").write_text(json.dumps(metrics, indent=2))

    # --- Feature importance (regressor) ---
    pd.DataFrame({
        "feature": meta["price_features"],
        "importance": reg.feature_importances_,
    }).to_csv(MODELS / "feature_importance.csv", index=False)

    print("Saved metrics.json + feature_importance.csv")
    print(json.dumps(metrics, indent=2))
    return metrics


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    evaluate_and_save()
