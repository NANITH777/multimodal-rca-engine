"""Loader noise touches metric statistics only. Re-measure the modality gaps with clean metric statistics."""

import warnings

import numpy as np
import pandas as pd
from common import OUT, ROOT, SEEDS, binary_metrics, load_features, make_tfidf, mcnemar_exact, strip_tags
from models.data_loader import MultimodalDataLoader
from run_visual import best_weights
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

warnings.filterwarnings("ignore")
LEN, CH = 60, 8

d = load_features()
y, Xm, Xl = d["y_anomaly"], d["X_metrics"], d["X_logs"]
ids = [str(s) for s in d["ids"]]
clean = [strip_tags(str(t)) for t in d["text"]]
c = MultimodalDataLoader(noise_level=0.0, feature_dropout=0.0, seed=42).build_feature_matrix(include_tfidf=False)
assert list(c["sample_ids"]) == ids
Xc = c["X_metrics"].values
print("log block unchanged by the loader noise:", np.allclose(c["X_logs"].values, Xl), flush=True)

# The 1-D CNN of the series control, imported late because that module runs its experiment at import time.
import torch  # noqa: E402
from torch import nn  # noqa: E402


def read_series(sid):
    df = pd.read_csv(ROOT / "data" / "multimodal_dataset" / "metrics" / f"{sid}.csv")
    out = np.zeros((CH, LEN), dtype=np.float32)
    for i, col in enumerate([k for k in df.columns if k != "timestamp"][:CH]):
        v = df[col].values.astype(float)[:LEN]
        rng = v.max() - v.min()
        out[i, :len(v)] = (v - v.min()) / rng if rng > 0 else 0
    return out


def cnn(x_tr, y_tr, x_va, y_va, x_te, seed, epochs=25):
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

    best, state, wait = 1e9, None, 0
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
            best, wait, state = loss, 0, {k: v.clone() for k, v in net.state_dict().items()}
        else:
            wait += 1
            if wait >= 5:
                break
    net.load_state_dict(state)
    return predict(Xv), predict(Xt)


S = np.stack([read_series(s) for s in ids])
runs, tests = [], []
for seed in SEEDS:
    trv, te = train_test_split(np.arange(len(y)), test_size=0.2, random_state=seed, stratify=y)
    tr, va = train_test_split(trv, test_size=0.125, random_state=seed, stratify=y[trv])
    v = make_tfidf().fit([clean[i] for i in tr])
    T = lambda ii: v.transform([clean[i] for i in ii]).toarray()
    rf = lambda: RandomForestClassifier(n_estimators=200, n_jobs=-1, random_state=seed)
    P = {}
    A = rf().fit(Xc[tr], y[tr]); P["Mc"] = (A.predict_proba(Xc[va])[:, 1], A.predict_proba(Xc[te])[:, 1])
    A = rf().fit(np.hstack([Xl[tr], T(tr)]), y[tr])
    P["L"] = (A.predict_proba(np.hstack([Xl[va], T(va)]))[:, 1], A.predict_proba(np.hstack([Xl[te], T(te)]))[:, 1])
    P["S"] = cnn(S[tr], y[tr], S[va], y[va], S[te], seed)
    preds = {}
    for combo in ["Mc", "Mc+L", "Mc+S"]:
        mods = combo.split("+")
        w = best_weights([P[m][0] for m in mods], y[va]) if len(mods) > 1 else np.array([1.0])
        p = sum(a * P[m][1] for a, m in zip(w, mods))
        preds[combo] = (p >= 0.5).astype(int)
        runs.append({"seed": seed, "combination": combo, "weights": "/".join(f"{a:.1f}" for a in w), **binary_metrics(y[te], p)})
        print(runs[-1], flush=True)
    bb, cc, pv = mcnemar_exact(y[te], preds["Mc+S"], preds["Mc"])
    tests.append({"seed": seed, "comparison": "Mc+S vs Mc", "a_only_correct": bb, "b_only_correct": cc, "mcnemar_p": pv})
    pd.DataFrame(runs).to_csv(OUT / "noise_sensitivity_runs.csv", index=False)
    pd.DataFrame(tests).to_csv(OUT / "noise_sensitivity_tests.csv", index=False)
print(pd.DataFrame(runs).groupby("combination").f1.agg(["mean", "std"]))
print(pd.DataFrame(tests))
