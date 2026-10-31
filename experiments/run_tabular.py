"""Tabular experiments: leakage audit, model suite, RCA tasks, unseen-fault generalisation, SHAP."""

import warnings

import numpy as np
import pandas as pd
from common import OUT, SEEDS, binary_metrics, load_features, make_tfidf, strip_tags
from lightgbm import LGBMClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import LeaveOneGroupOut, train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")
d = load_features()
Xm, Xl, y = d["X_metrics"], d["X_logs"], d["y_anomaly"]
raw = [str(t) for t in d["text"]]
clean = [strip_tags(t) for t in raw]


def rf(seed=0):
    return RandomForestClassifier(n_estimators=200, n_jobs=-1, random_state=seed)


MODELS = {
    "Logistic Regression": lambda s: make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, random_state=s)),
    "KNN (k=5)": lambda s: make_pipeline(StandardScaler(), KNeighborsClassifier(5, n_jobs=-1)),
    "Random Forest": rf,
    "Gradient Boosting": lambda s: GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=s),
    "XGBoost": lambda s: XGBClassifier(n_estimators=200, max_depth=8, learning_rate=0.1, subsample=0.8,
                                       colsample_bytree=0.8, n_jobs=-1, random_state=s, eval_metric="logloss"),
    "LightGBM": lambda s: LGBMClassifier(n_estimators=200, n_jobs=-1, random_state=s, verbose=-1),
    "MLP": lambda s: make_pipeline(StandardScaler(), MLPClassifier((128, 64), max_iter=300, random_state=s)),
}


def features(tr, te, modality, texts=clean):
    """Assemble a feature block; TF-IDF is fitted on the training indices only."""
    parts_tr, parts_te = [], []
    if "M" in modality:
        parts_tr.append(Xm[tr]); parts_te.append(Xm[te])
    if "C" in modality:  # level counts and ratios
        parts_tr.append(Xl[tr]); parts_te.append(Xl[te])
    if "T" in modality:  # template TF-IDF
        v = make_tfidf().fit([texts[i] for i in tr])
        parts_tr.append(v.transform([texts[i] for i in tr]).toarray())
        parts_te.append(v.transform([texts[i] for i in te]).toarray())
    return np.hstack(parts_tr), np.hstack(parts_te)


def repeated(target, modality, make=rf, mask=None, texts=clean, average="binary"):
    idx = np.arange(len(target)) if mask is None else np.where(mask)[0]
    scores = []
    for s in SEEDS:
        tr, te = train_test_split(idx, test_size=0.2, random_state=s, stratify=target[idx])
        A, B = features(tr, te, modality, texts)
        scores.append(f1_score(target[te], make(s).fit(A, target[tr]).predict(B), average=average))
    return np.mean(scores), np.std(scores)


# 1. Leakage audit and RCA tasks ------------------------------------------------------
anom = y == 1
tasks = {"Layer (11)": d["y_layer"], "Scenario (22)": d["y_scenario"],
         "Root cause (5)": d["y_root"], "Severity (3)": d["y_severity"]}
rows = []
for name, tgt in tasks.items():
    for label, mod, txt in [("M", "M", clean), ("L-tagged", "CT", raw), ("L", "CT", clean),
                            ("T only", "T", clean), ("M+L", "MCT", clean)]:
        m, s = repeated(tgt, mod, mask=anom, texts=txt, average="weighted")
        rows.append({"task": name, "features": label, "f1_w": m, "std": s})
        print(rows[-1], flush=True)
pd.DataFrame(rows).to_csv(OUT / "rca_tasks.csv", index=False)

# 2. Binary detection by modality and a one-feature baseline ----------------------------
rows = []
for label, mod, txt in [("M", "M", clean), ("L-counts", "C", clean), ("L-templates", "T", clean),
                        ("L", "CT", clean), ("L-tagged", "CT", raw), ("M+L", "MCT", clean)]:
    m, s = repeated(y, mod, texts=txt)
    rows.append({"features": label, "f1": m, "std": s})
    print(rows[-1], flush=True)
sev = list(d["log_names"]).index("log_severity_score")
fs = []
for s_ in SEEDS:
    tr, te = train_test_split(np.arange(len(y)), test_size=0.2, random_state=s_, stratify=y)
    st = DecisionTreeClassifier(max_depth=1).fit(Xl[tr][:, [sev]], y[tr])
    fs.append(f1_score(y[te], st.predict(Xl[te][:, [sev]])))
rows.append({"features": "one threshold on log_severity_score", "f1": np.mean(fs), "std": np.std(fs)})
pd.DataFrame(rows).to_csv(OUT / "binary_modalities.csv", index=False)

# 3. Seven-model suite ------------------------------------------------------------------
rows = []
for name, make in MODELS.items():
    m, s = repeated(y, "MCT", make=make)
    rows.append({"model": name, "f1": m, "std": s})
    print(rows[-1], flush=True)
pd.DataFrame(rows).to_csv(OUT / "model_suite.csv", index=False)

# 4. Generalisation to unseen fault scenarios and unseen layers -------------------------
rng = np.random.default_rng(0)
layer, scen = d["y_layer"], d["y_scenario"]
none_code = list(pd.Series(scen[~anom]).unique())
rows = []
for mod in ["M", "CT", "MCT"]:
    per = []
    for sc in np.unique(scen[anom]):
        te_a = np.where(scen == sc)[0]
        lay = layer[te_a[0]]
        # Normal test samples come from the same layer as the held-out fault.
        normals = np.where(~anom & (layer == lay))[0]
        te_n = rng.choice(normals, min(len(te_a), len(normals)), replace=False)
        te = np.r_[te_a, te_n]
        tr = np.setdiff1d(np.arange(len(y)), te)
        A, B = features(tr, te, mod)
        p = rf().fit(A, y[tr]).predict_proba(B)[:, 1]
        per.append({"held_out": int(sc), **binary_metrics(y[te], p)})
    t = pd.DataFrame(per)
    rows.append({"protocol": "leave-one-scenario-out", "features": mod, "f1": t.f1.mean(), "std": t.f1.std(),
                 "worst_f1": t.f1.min(), "recall": t.recall.mean(), "precision": t.precision.mean()})
    print(rows[-1], flush=True)
    per = []
    for tr, te in LeaveOneGroupOut().split(Xm, y, layer):
        A, B = features(tr, te, mod)
        per.append(binary_metrics(y[te], rf().fit(A, y[tr]).predict_proba(B)[:, 1]))
    t = pd.DataFrame(per)
    rows.append({"protocol": "leave-one-layer-out", "features": mod, "f1": t.f1.mean(), "std": t.f1.std(),
                 "worst_f1": t.f1.min(), "recall": t.recall.mean(), "precision": t.precision.mean()})
    print(rows[-1], flush=True)
pd.DataFrame(rows).to_csv(OUT / "unseen_generalisation.csv", index=False)

# 5. SHAP attribution per feature family ------------------------------------------------
import shap  # noqa: E402

tr, te = train_test_split(np.arange(len(y)), test_size=0.2, random_state=0, stratify=y)
v = make_tfidf().fit([clean[i] for i in tr])
vocab = [f"tfidf:{w}" for w in v.get_feature_names_out()]
names = list(d["metric_names"]) + list(d["log_names"]) + vocab
A = np.hstack([Xm[tr], Xl[tr], v.transform([clean[i] for i in tr]).toarray()])
B = np.hstack([Xm[te], Xl[te], v.transform([clean[i] for i in te]).toarray()])
model = MODELS["XGBoost"](0).fit(A, y[tr])
vals = np.abs(shap.TreeExplainer(model).shap_values(B[:1000])).mean(0)
imp = pd.DataFrame({"feature": names, "mean_abs_shap": vals}).sort_values("mean_abs_shap", ascending=False)
imp["family"] = imp.feature.map(lambda f: "templates" if f.startswith("tfidf") else "levels" if f.startswith("log_") else "metrics")
imp.to_csv(OUT / "shap_importance.csv", index=False)
print(imp.head(15), imp.groupby("family").mean_abs_shap.sum())
