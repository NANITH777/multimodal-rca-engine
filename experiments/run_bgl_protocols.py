"""BGL sensitivity to the evaluation protocol: chronological vs random windows, and random single lines."""

import numpy as np
import pandas as pd
from common import OUT, ROOT
from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

cfg = TemplateMinerConfig()
cfg.drain_sim_th, cfg.drain_depth, cfg.profiling_enabled = 0.5, 4, False
cfg.masking_instructions = [MaskingInstruction(r"\b\d+\.\d+\.\d+\.\d+(:\d+)?\b", "IP"),
                            MaskingInstruction(r"\b[0-9a-fA-F]{8,}\b", "HEX"), MaskingInstruction(r"\b\d+\b", "NUM")]
m, ids, labels = TemplateMiner(config=cfg), [], []
with open(ROOT / "data" / "raw" / "BGL.log", encoding="utf8", errors="replace") as f:
    for line in f:
        tag, _, rest = line.partition(" ")
        p = rest.split(" ", 8)
        ids.append(m.add_log_message((p[8] if len(p) > 8 else rest).strip())["cluster_id"] - 1)
        labels.append(tag != "-")
ids, labels = np.array(ids), np.array(labels).astype(int)
nt = ids.max() + 1


def clf(y):
    return XGBClassifier(n_estimators=200, max_depth=8, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8, n_jobs=-1,
                         random_state=0, eval_metric="logloss", scale_pos_weight=(y == 0).sum() / max(1, y.sum()))


rows = []
for w in (20, 100):
    n = len(ids) // w
    X = np.zeros((n, nt), dtype=np.float32)
    for i in range(n):
        np.add.at(X[i], ids[i * w:(i + 1) * w], 1)
    y = np.array([labels[i * w:(i + 1) * w].max() for i in range(n)])
    cut = int(.8 * n)
    rows.append({"unit": f"{w}-line window", "split": "chronological", "n": n, "rate": y.mean(),
                 "f1": f1_score(y[cut:], clf(y[:cut]).fit(X[:cut], y[:cut]).predict(X[cut:])), "test_rate": y[cut:].mean()})
    tr, te = train_test_split(np.arange(n), test_size=.2, random_state=0, stratify=y)
    rows.append({"unit": f"{w}-line window", "split": "random", "n": n, "rate": y.mean(),
                 "f1": f1_score(y[te], clf(y[tr]).fit(X[tr], y[tr]).predict(X[te])), "test_rate": y[te].mean()})
    print(rows[-2:], flush=True)
s = np.random.default_rng(0).choice(len(ids), 50000, replace=False)
X = np.zeros((50000, nt), dtype=np.float32); X[np.arange(50000), ids[s]] = 1
y = labels[s]
tr, te = train_test_split(np.arange(50000), test_size=.2, random_state=0, stratify=y)
rows.append({"unit": "single line", "split": "random", "n": 50000, "rate": y.mean(),
             "f1": f1_score(y[te], clf(y[tr]).fit(X[tr], y[tr]).predict(X[te])), "test_rate": y[te].mean()})
print(rows[-1])
pd.DataFrame(rows).to_csv(OUT / "bgl_protocols.csv", index=False)
