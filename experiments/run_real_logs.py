"""Within-dataset detection on HDFS, BGL and OpenStack. Models are trained and tested on the same corpus."""

import re
import warnings

import numpy as np
import pandas as pd
from common import OUT, ROOT
from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig
from lightgbm import LGBMClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score
from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")
RAW = ROOT / "data" / "raw"


def miner():
    cfg = TemplateMinerConfig()
    cfg.drain_sim_th = 0.5
    cfg.drain_depth = 4
    cfg.profiling_enabled = False
    cfg.masking_instructions = [MaskingInstruction(r"\b\d+\.\d+\.\d+\.\d+(:\d+)?\b", "IP"),
                                MaskingInstruction(r"\b[0-9a-fA-F]{8,}\b|\b[0-9a-f-]{32,36}\b", "HEX"),
                                MaskingInstruction(r"\b\d+\b", "NUM")]
    return TemplateMiner(config=cfg)


def counts(seqs, n):
    X = np.zeros((len(seqs), n), dtype=np.float32)
    for i, s in enumerate(seqs):
        np.add.at(X[i], s, 1)
    return X


def load_bgl(window=100):
    """Non-overlapping chronological windows of 100 lines; a window is anomalous if it holds any alert line."""
    m, ids, labels = miner(), [], []
    with open(RAW / "BGL.log", encoding="utf8", errors="replace") as f:
        for line in f:
            tag, _, rest = line.partition(" ")
            parts = rest.split(" ", 8)
            content = parts[8] if len(parts) > 8 else rest
            ids.append(m.add_log_message(content.strip())["cluster_id"])
            labels.append(tag != "-")
    ids = np.array(ids) - 1
    labels = np.array(labels)
    n = len(ids) // window
    X = counts([ids[i * window:(i + 1) * window] for i in range(n)], ids.max() + 1)
    y = np.array([labels[i * window:(i + 1) * window].any() for i in range(n)]).astype(int)
    print("BGL lines", len(ids), "alert-line rate", labels.mean(), "templates", ids.max() + 1,
          "windows", n, "anomalous windows", y.mean(), flush=True)
    return X, y, None, "chronological"


def load_openstack():
    """Sessions are VM instances. Labels follow the source file, as in the public corpus."""
    m, sess = miner(), {}
    inst = re.compile(r"\[instance: ([0-9a-f-]{36})\]")
    for fn, lab in [("openstack_normal1.log", 0), ("openstack_normal2.log", 0), ("openstack_abnormal.log", 1)]:
        with open(RAW / fn, encoding="utf8", errors="replace") as f:
            for line in f:
                hit = inst.search(line)
                body = line.split(" ", 5)
                tid = m.add_log_message(body[5].strip() if len(body) > 5 else line.strip())["cluster_id"] - 1
                if hit:
                    sess.setdefault((fn, hit.group(1)), [lab, []])[1].append(tid)
    n_t = m.drain.clusters_counter
    seqs = [np.array(v[1]) for v in sess.values()]
    y = np.array([v[0] for v in sess.values()])
    print("OpenStack sessions", len(y), "anomalous", y.sum(), "templates", n_t, flush=True)
    return counts(seqs, n_t), y, None, "kfold"


def load_hdfs():
    """Full session matrix at the natural class ratio, without resampling."""
    df = pd.read_csv(RAW / "preprocessed" / "Event_occurrence_matrix.csv")
    cols = [c for c in df.columns if c.startswith("E")]
    y = (df["Label"] == "Fail").astype(int).values
    print("HDFS sessions", len(y), "anomalous", y.sum(), flush=True)
    return df[cols].values.astype(np.float32), y, None, "kfold"


def models(pos_weight):
    return {
        "Logistic Regression": make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced")),
        "KNN (k=5)": make_pipeline(StandardScaler(), KNeighborsClassifier(5, n_jobs=-1)),
        "Random Forest": RandomForestClassifier(200, class_weight="balanced", n_jobs=-1, random_state=0),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=0),
        "XGBoost": XGBClassifier(n_estimators=200, max_depth=8, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
                                 scale_pos_weight=pos_weight, n_jobs=-1, random_state=0, eval_metric="logloss"),
        "LightGBM": LGBMClassifier(n_estimators=200, class_weight="balanced", n_jobs=-1, random_state=0, verbose=-1),
        "MLP": make_pipeline(StandardScaler(), MLPClassifier((128, 64), max_iter=200, random_state=0)),
    }


def evaluate(name, X, y, mode, rows):
    if mode == "chronological":
        cut = int(0.8 * len(y))
        folds = [(np.arange(cut), np.arange(cut, len(y)))]
    else:
        folds = list(StratifiedKFold(5, shuffle=True, random_state=0).split(X, y))
    for model_name in models(1):
        f, pr, re_, ap = [], [], [], []
        for tr, te in folds:
            if len(tr) > 200000 and model_name in ("KNN (k=5)", "Gradient Boosting", "MLP"):
                tr = np.random.default_rng(0).choice(tr, 200000, replace=False)  # tractability on HDFS
            pw = (y[tr] == 0).sum() / max(1, (y[tr] == 1).sum())
            m = models(pw)[model_name].fit(X[tr], y[tr])
            p = m.predict_proba(X[te])[:, 1]
            hat = (p >= 0.5).astype(int)
            f.append(f1_score(y[te], hat, zero_division=0)); pr.append(precision_score(y[te], hat, zero_division=0))
            re_.append(recall_score(y[te], hat, zero_division=0)); ap.append(average_precision_score(y[te], p))
        rows.append({"dataset": name, "model": model_name, "split": mode, "n": len(y), "anomaly_rate": y.mean(),
                     "f1": np.mean(f), "f1_std": np.std(f), "precision": np.mean(pr), "recall": np.mean(re_), "pr_auc": np.mean(ap)})
        print(rows[-1], flush=True)
        pd.DataFrame(rows).to_csv(OUT / "real_logs.csv", index=False)


rows = []
for name, loader in [("OpenStack", load_openstack), ("BGL", load_bgl), ("HDFS", load_hdfs)]:
    X, y, _, mode = loader()
    evaluate(name, X, y, mode, rows)
