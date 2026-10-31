"""Single-protocol results for the detector sections: detailed metrics, overfitting checks, CV, RCA accuracy, SHAP."""

import json
import warnings

import numpy as np
import pandas as pd
import shap
from common import OUT, ROOT, SEEDS, load_features, make_tfidf, strip_tags
from lightgbm import LGBMClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")
d = load_features()
Xm, Xl, y = d["X_metrics"], d["X_logs"], d["y_anomaly"]
ids = [str(s) for s in d["ids"]]
clean = [strip_tags(str(t)) for t in d["text"]]


def xy(tr, *others):
    v = make_tfidf().fit([clean[i] for i in tr])
    return [np.hstack([Xm[i], Xl[i], v.transform([clean[j] for j in i]).toarray()]) for i in (tr, *others)], v


def xgb(s):
    return XGBClassifier(n_estimators=200, max_depth=8, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
                         n_jobs=-1, random_state=s, eval_metric="logloss")


MODELS = {
    "Random Forest": lambda s: RandomForestClassifier(200, n_jobs=-1, random_state=s),
    "XGBoost": xgb,
    "LightGBM": lambda s: LGBMClassifier(n_estimators=200, n_jobs=-1, random_state=s, verbose=-1),
    "Gradient Boosting": lambda s: GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=s),
    "MLP": lambda s: make_pipeline(StandardScaler(), MLPClassifier((128, 64), max_iter=300, random_state=s)),
    "KNN (k=5)": lambda s: make_pipeline(StandardScaler(), KNeighborsClassifier(5, n_jobs=-1)),
    "Logistic Regression": lambda s: make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, random_state=s)),
}

# 1. Stratified 5-fold CV for all models, with TF-IDF refitted per fold.
rows = []
folds = list(StratifiedKFold(5, shuffle=True, random_state=0).split(Xm, y))
prep = [xy(tr, te)[0] for tr, te in folds]
for name, make in MODELS.items():
    f, auc = [], []
    for (tr, te), (A, B) in zip(folds, prep):
        m = make(0).fit(A, y[tr]); p = m.predict_proba(B)[:, 1]
        f.append(f1_score(y[te], p >= .5)); auc.append(roc_auc_score(y[te], p))
    rows.append({"model": name, "f1": np.mean(f), "std": np.std(f, ddof=1), "auc": np.mean(auc)})
    print(rows[-1], flush=True)
pd.DataFrame(rows).to_csv(OUT / "cv_models.csv", index=False)

# 2. Held-out metrics, confusion matrices and train/validation/test gaps on a 70/10/20 split.
rows, gaps = [], []
for s in SEEDS:
    idx = np.arange(len(y))
    trv, te = train_test_split(idx, test_size=.2, random_state=s, stratify=y)
    tr, va = train_test_split(trv, test_size=.125, random_state=s, stratify=y[trv])
    (A, V, B), _ = xy(tr, va, te)
    regs = {"XGBoost, regularised": lambda s_: XGBClassifier(
        n_estimators=500, max_depth=5, learning_rate=.1, subsample=.8, colsample_bytree=.8, gamma=1.0, reg_alpha=.1,
        reg_lambda=2.0, min_child_weight=5, n_jobs=-1, random_state=s_, eval_metric="logloss", early_stopping_rounds=20)}
    for name, make in [(k, MODELS[k]) for k in ("Random Forest", "XGBoost", "LightGBM")] + list(regs.items()):
        m = make(s)
        if "regularised" in name:
            m.fit(A, y[tr], eval_set=[(V, y[va])], verbose=False)
        else:
            m.fit(A, y[tr])
        p = m.predict_proba(B)[:, 1]; hat = p >= .5
        tn, fp, fn, tp = confusion_matrix(y[te], hat).ravel()
        rows.append({"seed": s, "model": name, "accuracy": accuracy_score(y[te], hat), "precision": precision_score(y[te], hat),
                     "recall": recall_score(y[te], hat), "f1": f1_score(y[te], hat), "auc": roc_auc_score(y[te], p),
                     "tn": tn, "fp": fp, "fn": fn, "tp": tp,
                     "train_f1": f1_score(y[tr], m.predict(A)), "val_f1": f1_score(y[va], m.predict(V)),
                     "best_iter": getattr(m, "best_iteration", None) if "regularised" in name else None})
    print(s, flush=True)
    pd.DataFrame(rows).to_csv(OUT / "detector_details.csv", index=False)

# 3. RCA accuracy with M+L on anomalous samples, and the residual confusion.
anom = np.where(y == 1)[0]
rows = []
for task, tgt in {"Layer (11)": d["y_layer"], "Scenario (22)": d["y_scenario"], "Root cause (5)": d["y_root"], "Severity (3)": d["y_severity"]}.items():
    acc, f, errs = [], [], []
    for s in SEEDS:
        tr, te = train_test_split(anom, test_size=.2, random_state=s, stratify=tgt[anom])
        (A, B), _ = xy(tr, te)
        h = RandomForestClassifier(200, n_jobs=-1, random_state=s).fit(A, tgt[tr]).predict(B)
        acc.append(accuracy_score(tgt[te], h)); f.append(f1_score(tgt[te], h, average="weighted")); errs.append(int((h != tgt[te]).sum()))
    rows.append({"task": task, "accuracy": np.mean(acc), "f1_w": np.mean(f), "std": np.std(f, ddof=1), "mean_errors": np.mean(errs), "n_test": len(te)})
    print(rows[-1], flush=True)
pd.DataFrame(rows).to_csv(OUT / "rca_accuracy.csv", index=False)

# 4. Measured metric-feature statistics for one layer, per injected pattern (raw series, no loader noise).
pat = {}
for x in ids[:]:
    pass
labels = {s: json.load(open(ROOT / "data/multimodal_dataset/labels" / f"{s}.json", encoding="utf8")) for s in ids}
rows = []
target = {"Normal": lambda l: l["layer"] == "cdn" and not l["is_anomaly"], "CDN\\_001": lambda l: l["scenario_id"] == "CDN_001",
          "CDN\\_002": lambda l: l["scenario_id"] == "CDN_002"}
for name, cond in target.items():
    vals = {"mean": [], "std": [], "slope": [], "autocorr": []}
    for s, l in labels.items():
        if cond(l):
            v = pd.read_csv(ROOT / "data/multimodal_dataset/metrics" / f"{s}.csv")["request_count"].values.astype(float)
            vals["mean"].append(v.mean()); vals["std"].append(v.std())
            vals["slope"].append(np.polyfit(np.arange(len(v)), v, 1)[0]); vals["autocorr"].append(np.corrcoef(v[:-1], v[1:])[0, 1])
    rows.append({"sample": name, "n": len(vals["mean"]), **{k: np.mean(a) for k, a in vals.items()}})
pd.DataFrame(rows).to_csv(OUT / "metric_examples.csv", index=False)
print(rows, flush=True)

# 5. Top-20 SHAP and one local explanation for FW_002 on the XGBoost detector.
tr, te = train_test_split(np.arange(len(y)), test_size=.2, random_state=0, stratify=y)
(A, B), v = xy(tr, te)
names = list(d["metric_names"]) + list(d["log_names"]) + [f"tfidf:{w}" for w in v.get_feature_names_out()]
m = xgb(0).fit(A, y[tr])
ex = shap.TreeExplainer(m)
pos = next(i for i, j in enumerate(te) if labels[ids[j]]["scenario_id"] == "FW_002" and m.predict(B[[i]])[0] == 1)
vals = ex.shap_values(B[[pos]])[0]
o = np.argsort(-np.abs(vals))[:8]
pd.DataFrame({"sample": ids[te[pos]], "feature": np.array(names)[o], "shap": vals[o], "value": B[pos][o], "base": float(ex.expected_value)}).to_csv(OUT / "shap_local_fw002.csv", index=False)
print(pd.read_csv(OUT / "shap_local_fw002.csv"))
