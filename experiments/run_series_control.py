"""Control for the visual gain: replace the dashboard CNN by a 1-D CNN on the same normalised series.

If the fusion gain survives without any image, it comes from model diversity over the metric
signal, not from a visual modality. Also records how the RCA labels depend on one another.
"""

import warnings

import numpy as np
import pandas as pd
import torch
from common import OUT, ROOT, SEEDS, binary_metrics, load_features, make_tfidf, mcnemar_exact, paired_bootstrap_f1, strip_tags
from run_visual import best_weights
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from torch import nn

warnings.filterwarnings("ignore")
LEN, CH = 60, 8


def read_series(sid):
    df = pd.read_csv(ROOT / "data" / "multimodal_dataset" / "metrics" / f"{sid}.csv")
    out = np.zeros((CH, LEN), dtype=np.float32)
    for i, c in enumerate([c for c in df.columns if c != "timestamp"][:CH]):
        v = df[c].values.astype(float)[:LEN]
        rng = v.max() - v.min()
        out[i, :len(v)] = (v - v.min()) / rng if rng > 0 else 0
    return out


def train_cnn1d(x_tr, y_tr, x_va, y_va, x_te, seed, epochs=25):
    torch.manual_seed(seed)
    net = nn.Sequential(
        nn.Conv1d(CH, 16, 5, padding=2), nn.BatchNorm1d(16), nn.ReLU(), nn.MaxPool1d(2),
        nn.Conv1d(16, 32, 5, padding=2), nn.BatchNorm1d(32), nn.ReLU(), nn.MaxPool1d(2),
        nn.Conv1d(32, 64, 3, padding=1), nn.BatchNorm1d(64), nn.ReLU(), nn.MaxPool1d(2),
        nn.AdaptiveAvgPool1d(5), nn.Flatten(), nn.Dropout(0.3), nn.Linear(320, 1))
    opt = torch.optim.Adam(net.parameters(), 1e-3, weight_decay=1e-4)
    X, Y, Xv, Xt = map(torch.from_numpy, (x_tr, y_tr.astype(np.float32), x_va, x_te))

    def predict(t):
        net.eval()
        with torch.no_grad():
            return torch.sigmoid(net(t)).squeeze(1).numpy()

    best, best_state, wait = 1e9, None, 0
    gen = torch.Generator().manual_seed(seed)
    for _ in range(epochs):
        net.train()
        perm = torch.randperm(len(X), generator=gen)
        for i in range(0, len(X), 64):
            b = perm[i:i + 64]
            opt.zero_grad()
            nn.functional.binary_cross_entropy_with_logits(net(X[b]).squeeze(1), Y[b]).backward()
            opt.step()
        pv = np.clip(predict(Xv), 1e-6, 1 - 1e-6)
        loss = -np.mean(y_va * np.log(pv) + (1 - y_va) * np.log(1 - pv))
        if loss < best:
            best, wait = loss, 0
            best_state = {k: v.clone() for k, v in net.state_dict().items()}
        else:
            wait += 1
            if wait >= 5:
                break
    net.load_state_dict(best_state)
    return predict(Xv), predict(Xt)


d = load_features()
ids = [str(s) for s in d["ids"]]
y, Xm, Xl = d["y_anomaly"], d["X_metrics"], d["X_logs"]
clean = [strip_tags(str(t)) for t in d["text"]]

anom = y == 1
lab = pd.DataFrame({"layer": d["y_layer"][anom], "scenario": d["y_scenario"][anom],
                    "root": d["y_root"][anom], "severity": d["y_severity"][anom]})
dep = [{"given": g, "target": t, "max_values_per_group": int(lab.groupby(g)[t].nunique().max()),
        "n_groups": lab[g].nunique(), "n_target": lab[t].nunique()}
       for g in ("scenario", "layer") for t in ("layer", "root", "severity") if g != t]
pd.DataFrame(dep).to_csv(OUT / "label_dependencies.csv", index=False)
print(pd.DataFrame(dep), flush=True)

S = np.stack([read_series(s) for s in ids])
runs, tests = [], []
for seed in SEEDS:
    idx = np.arange(len(y))
    trv, te = train_test_split(idx, test_size=0.2, random_state=seed, stratify=y)
    tr, va = train_test_split(trv, test_size=0.125, random_state=seed, stratify=y[trv])
    v = make_tfidf().fit([clean[i] for i in tr])
    T = lambda ii: v.transform([clean[i] for i in ii]).toarray()
    rf = lambda: RandomForestClassifier(n_estimators=200, n_jobs=-1, random_state=seed)
    P = {}
    A = rf().fit(Xm[tr], y[tr]); P["M"] = (A.predict_proba(Xm[va])[:, 1], A.predict_proba(Xm[te])[:, 1])
    A = rf().fit(np.hstack([Xl[tr], T(tr)]), y[tr])
    P["L"] = (A.predict_proba(np.hstack([Xl[va], T(va)]))[:, 1], A.predict_proba(np.hstack([Xl[te], T(te)]))[:, 1])
    P["S"] = train_cnn1d(S[tr], y[tr], S[va], y[va], S[te], seed)
    preds = {}
    for combo in ["M", "L", "S", "M+S", "L+S"]:
        mods = combo.split("+")
        w = best_weights([P[m][0] for m in mods], y[va]) if len(mods) > 1 else np.array([1.0])
        p = sum(a * P[m][1] for a, m in zip(w, mods))
        preds[combo] = (p >= 0.5).astype(int)
        runs.append({"seed": seed, "combination": combo, "weights": "/".join(f"{a:.1f}" for a in w),
                     **binary_metrics(y[te], p)})
        print(runs[-1], flush=True)
    for a, b in [("M+S", "M"), ("L+S", "L")]:
        bb, cc, pv = mcnemar_exact(y[te], preds[a], preds[b])
        lo, hi = paired_bootstrap_f1(y[te], preds[a], preds[b], seed=seed)
        tests.append({"seed": seed, "comparison": f"{a} vs {b}", "a_only_correct": bb, "b_only_correct": cc,
                      "mcnemar_p": pv, "df1_low": lo, "df1_high": hi})
    pd.DataFrame(runs).to_csv(OUT / "series_control_runs.csv", index=False)
    pd.DataFrame(tests).to_csv(OUT / "series_control_tests.csv", index=False)
print(pd.DataFrame(runs).groupby("combination")[["f1", "precision", "recall"]].agg(["mean", "std"]))
