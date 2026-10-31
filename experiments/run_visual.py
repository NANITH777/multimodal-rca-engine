"""Sanitised dashboards: rendering, a pixel CNN, and the seven-subset modality ablation on the full test split."""

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from common import OUT, ROOT, SEEDS, binary_metrics, load_features, make_tfidf, mcnemar_exact, paired_bootstrap_f1, strip_tags

warnings.filterwarnings("ignore")
SAN = ROOT / "data" / "dashboards_sanitized"
PIX = OUT / "sanitized_pixels.npy"
H, W = 80, 200


def render_one(sid):
    """Same overlay and normalisation as the compact dashboard, without title, badge, legend or shading."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from dataset_generator.dashboard_generator import GRAFANA_COLORS, METRIC_COLORS
    path = SAN / f"{sid}.png"
    if path.exists():
        return
    df = pd.read_csv(ROOT / "data" / "multimodal_dataset" / "metrics" / f"{sid}.csv")
    fig, ax = plt.subplots(figsize=(10, 4))
    fig.patch.set_facecolor(GRAFANA_COLORS["bg"])
    ax.set_facecolor(GRAFANA_COLORS["panel_bg"])
    for i, c in enumerate(col for col in df.columns if col != "timestamp"):
        v = df[c].values.astype(float)
        rng = v.max() - v.min()
        ax.plot((v - v.min()) / rng if rng > 0 else v * 0, color=METRIC_COLORS[i % len(METRIC_COLORS)], lw=1.0, alpha=0.85)
    ax.grid(True, alpha=0.15)
    ax.set_xticks([]); ax.set_yticks([])
    fig.savefig(path, dpi=100, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)


def render_all(ids):
    from joblib import Parallel, delayed
    from PIL import Image
    SAN.mkdir(parents=True, exist_ok=True)
    Parallel(n_jobs=12, batch_size=50)(delayed(render_one)(s) for s in ids)
    arr = np.zeros((len(ids), 3, H, W), dtype=np.uint8)
    for i, s in enumerate(ids):
        arr[i] = np.asarray(Image.open(SAN / f"{s}.png").convert("RGB").resize((W, H), Image.BILINEAR)).transpose(2, 0, 1)
    np.save(PIX, arr)


def train_cnn(x_tr, y_tr, x_va, y_va, x_te, seed, epochs=25):
    import torch
    from torch import nn
    torch.manual_seed(seed)
    net = nn.Sequential(
        nn.Conv2d(3, 16, 3, padding=1), nn.BatchNorm2d(16), nn.ReLU(), nn.MaxPool2d(2),
        nn.Conv2d(16, 32, 3, padding=1), nn.BatchNorm2d(32), nn.ReLU(), nn.MaxPool2d(2),
        nn.Conv2d(32, 64, 3, padding=1), nn.BatchNorm2d(64), nn.ReLU(), nn.MaxPool2d(2),
        nn.AdaptiveAvgPool2d((2, 5)), nn.Flatten(), nn.Dropout(0.3), nn.Linear(640, 1))
    opt = torch.optim.Adam(net.parameters(), 1e-3, weight_decay=1e-4)
    to_t = lambda a: torch.from_numpy(a.astype(np.float32) / 255.0)
    X, Y = to_t(x_tr), torch.from_numpy(y_tr.astype(np.float32))
    Xv = to_t(x_va); Xt = to_t(x_te)

    def predict(t):
        net.eval()
        with torch.no_grad():
            return np.concatenate([torch.sigmoid(net(t[i:i + 256])).squeeze(1).numpy() for i in range(0, len(t), 256)])

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


def best_weights(ps_val, y_val, step=0.1):
    """Grid-search simplex weights on validation F1; ties go to the most even weighting."""
    from sklearn.metrics import f1_score
    n = len(ps_val)
    grid = np.round(np.arange(0, 1 + 1e-9, step), 2)
    best, best_w = -1, None
    def walk(prefix):
        nonlocal best, best_w
        if len(prefix) == n - 1:
            w = np.array(prefix + [round(1 - sum(prefix), 2)])
            if w[-1] < 0:
                return
            f = f1_score(y_val, (sum(a * p for a, p in zip(w, ps_val)) >= 0.5).astype(int))
            if f > best + 1e-9 or (abs(f - best) <= 1e-9 and w.max() < best_w.max()):
                best, best_w = f, w
            return
        for g in grid:
            walk(prefix + [float(g)])
    walk([])
    return best_w


def main():
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    d = load_features()
    ids = [str(s) for s in d["ids"]]
    if not PIX.exists():
        render_all(ids)
    pix = np.load(PIX, mmap_mode="r")
    y, Xm, Xl = d["y_anomaly"], d["X_metrics"], d["X_logs"]
    clean = [strip_tags(str(t)) for t in d["text"]]
    summary, detail = [], []
    for seed in SEEDS:
        idx = np.arange(len(y))
        trv, te = train_test_split(idx, test_size=0.2, random_state=seed, stratify=y)
        tr, va = train_test_split(trv, test_size=0.125, random_state=seed, stratify=y[trv])
        v = make_tfidf().fit([clean[i] for i in tr])
        T = lambda ii: v.transform([clean[i] for i in ii]).toarray()
        rf = lambda: RandomForestClassifier(n_estimators=200, n_jobs=-1, random_state=seed)
        P = {}
        A = rf().fit(Xm[tr], y[tr]); P["M"] = (A.predict_proba(Xm[va])[:, 1], A.predict_proba(Xm[te])[:, 1])
        Lt = np.hstack([Xl[tr], T(tr)]); Lv = np.hstack([Xl[va], T(va)]); Le = np.hstack([Xl[te], T(te)])
        A = rf().fit(Lt, y[tr]); P["L"] = (A.predict_proba(Lv)[:, 1], A.predict_proba(Le)[:, 1])
        P["V"] = train_cnn(np.asarray(pix[tr]), y[tr], np.asarray(pix[va]), y[va], np.asarray(pix[te]), seed)
        preds = {}
        for combo in ["M", "L", "V", "M+L", "M+V", "L+V", "M+L+V"]:
            mods = combo.split("+")
            w = best_weights([P[m][0] for m in mods], y[va]) if len(mods) > 1 else np.array([1.0])
            p = sum(a * P[m][1] for a, m in zip(w, mods))
            preds[combo] = (p >= 0.5).astype(int)
            summary.append({"seed": seed, "combination": combo, "weights": "/".join(f"{a:.1f}" for a in w),
                            **binary_metrics(y[te], p)})
            print(summary[-1], flush=True)
        for a, b in [("M+L", "L"), ("M+L", "M"), ("M+V", "M"), ("L+V", "L"), ("M+L+V", "M+L")]:
            bb, cc, pv = mcnemar_exact(y[te], preds[a], preds[b])
            lo, hi = paired_bootstrap_f1(y[te], preds[a], preds[b], seed=seed)
            detail.append({"seed": seed, "comparison": f"{a} vs {b}", "a_only_correct": bb, "b_only_correct": cc,
                           "mcnemar_p": pv, "df1_low": lo, "df1_high": hi})
        pd.DataFrame(summary).to_csv(OUT / "ablation_runs.csv", index=False)
        pd.DataFrame(detail).to_csv(OUT / "ablation_tests.csv", index=False)
    t = pd.DataFrame(summary).groupby("combination")[["f1", "precision", "recall", "auc"]].agg(["mean", "std"])
    t.to_csv(OUT / "ablation_summary.csv")
    print(t)


if __name__ == "__main__":
    main()
