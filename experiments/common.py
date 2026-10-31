"""Shared helpers for the revision experiments (results/revision/)."""

import re
import sys
from pathlib import Path

import numpy as np
from scipy.stats import binomtest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT = ROOT / "results" / "revision"
OUT.mkdir(parents=True, exist_ok=True)
CACHE = OUT / "feature_cache.npz"
SEEDS = (0, 1, 2, 3, 4)

_PREFIX = re.compile(r"^\S+ \S+ \[[A-Z]+\s*\] \[[A-Z_]+\s*\] ", flags=re.M)


def strip_tags(text):
    """Remove the timestamp, level tag and layer tag that prefix every synthetic log line."""
    return _PREFIX.sub("", text)


def make_tfidf():
    return TfidfVectorizer(max_features=100, stop_words="english",
                           token_pattern=r"(?u)\b[A-Za-z_][A-Za-z0-9_]{2,}\b")


def load_features():
    """Return cached features: metric stats, level counts, raw and tag-free log text, labels."""
    if not CACHE.exists():
        from models.data_loader import MultimodalDataLoader
        loader = MultimodalDataLoader(noise_level=0.25, feature_dropout=0.08, seed=42)
        d = loader.build_feature_matrix(include_tfidf=False)
        text = [loader.extract_log_text(s) for s in d["sample_ids"]]
        np.savez_compressed(
            CACHE, X_metrics=d["X_metrics"].values, X_logs=d["X_logs"].values,
            metric_names=np.array(d["X_metrics"].columns), log_names=np.array(d["X_logs"].columns),
            text=np.array(text, dtype=object), ids=np.array(d["sample_ids"]),
            y_anomaly=d["y_anomaly"], y_layer=d["y_layer"], y_scenario=d["y_scenario"],
            y_root=d["y_root_cause"], y_severity=d["y_severity"])
    z = np.load(CACHE, allow_pickle=True)
    return {k: z[k] for k in z.files}


def binary_metrics(y, p, threshold=0.5):
    pred = (p >= threshold).astype(int)
    return {"f1": f1_score(y, pred, zero_division=0),
            "precision": precision_score(y, pred, zero_division=0),
            "recall": recall_score(y, pred, zero_division=0),
            "auc": roc_auc_score(y, p) if len(set(y)) == 2 else float("nan")}


def mcnemar_exact(y, pred_a, pred_b):
    """Exact two-sided McNemar test on paired predictions; returns (b, c, p)."""
    a_ok, b_ok = pred_a == y, pred_b == y
    b, c = int((a_ok & ~b_ok).sum()), int((~a_ok & b_ok).sum())
    p = binomtest(b, b + c, 0.5).pvalue if b + c else 1.0
    return b, c, p


def paired_bootstrap_f1(y, pred_a, pred_b, n=2000, seed=0):
    """95% percentile CI of F1(a) - F1(b), resampling the same samples for both."""
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(n):
        i = rng.integers(0, len(y), len(y))
        diffs.append(f1_score(y[i], pred_a[i], zero_division=0) - f1_score(y[i], pred_b[i], zero_division=0))
    return tuple(np.percentile(diffs, [2.5, 97.5]))
