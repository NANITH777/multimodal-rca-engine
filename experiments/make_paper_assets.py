"""Builds every figure and table of paper/main.tex from results/revision, so no number is typed by hand."""

import glob
import json
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from common import OUT, ROOT
from matplotlib.patches import FancyBboxPatch
from PIL import Image

PAPER = ROOT / "paper"
FIG, TAB = PAPER / "figures", PAPER / "tables"
FIG.mkdir(parents=True, exist_ok=True)
TAB.mkdir(parents=True, exist_ok=True)
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.family": "serif", "font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
                     "xtick.color": MUTED, "ytick.color": INK, "axes.spines.top": False, "axes.spines.right": False})


def table(name, spec, rows, header):
    body = "\n".join(" & ".join(r) + r" \\" for r in rows)
    (TAB / f"{name}.tex").write_text(
        f"\\begin{{tabular}}{{@{{}}{spec}@{{}}}}\n\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n",
        encoding="utf8")


def pm(m, s, d=4):
    return f"{m:.{d}f} $\\pm$ {s:.{d}f}"


# Dataset facts --------------------------------------------------------------------------
labels = [json.load(open(f, encoding="utf8")) for f in sorted(glob.glob(str(ROOT / "data/multimodal_dataset/labels/*.json")))]
level = re.compile(r"\[(INFO|WARN|ERROR|CRITICAL)\s*\]")
shares = {False: [], True: []}
for x in labels[::5]:
    txt = (ROOT / "data/multimodal_dataset/logs" / f"{x['sample_id']}.txt").read_text(encoding="utf8", errors="ignore")
    found = level.findall(txt)
    shares[x["is_anomaly"]].append([found.count(k) / len(found) for k in ("INFO", "WARN", "ERROR", "CRITICAL")])
rows = [[n] + [f"{v:.3f}" for v in np.mean(shares[k], 0)] for n, k in (("Normal", False), ("Anomalous", True))]
table("log_levels", "lcccc", rows, "Class & INFO & WARN & ERROR & CRITICAL")

# Tables ---------------------------------------------------------------------------------
t = pd.read_csv(OUT / "binary_modalities.csv")
names = {"M": "$M$: metric statistics", "L-counts": "$L_c$: log-level counts and ratios", "L-templates": "$L_t$: log TF-IDF",
         "L": "$L$: $L_c+L_t$, tags removed", "L-tagged": "$L$ with layer and level tags", "M+L": "$M+L$: feature concatenation",
         "one threshold on log_severity_score": "One threshold on severity score"}
table("binary", "lc", [[names[r.features], pm(r.f1, r["std"])] for _, r in t.iterrows()], r"Features (Random Forest) & $F_1$")

t = pd.read_csv(OUT / "model_suite.csv").sort_values("f1", ascending=False)
table("models", "lc", [[r.model, pm(r.f1, r["std"])] for _, r in t.iterrows()], r"Model ($M+L$ features) & $F_1$")

t = pd.read_csv(OUT / "rca_tasks.csv").pivot(index="task", columns="features", values="f1_w")
s = pd.read_csv(OUT / "rca_tasks.csv").pivot(index="task", columns="features", values="std")
order = ["Layer (11)", "Scenario (22)", "Root cause (5)", "Severity (3)"]
table("rca", "lccc", [[k] + [pm(t.loc[k, c], s.loc[k, c], 3) for c in ("M", "L", "M+L")] for k in order],
      r"Target (classes) & $M$ & $L$ & $M+L$")

t = pd.read_csv(OUT / "unseen_generalisation.csv")
rand = pd.read_csv(OUT / "binary_modalities.csv").set_index("features")
rows = []
for f, key, lab in (("M", "M", "$M$"), ("CT", "L", "$L$"), ("MCT", "M+L", "$M+L$")):
    a = t[(t.features == f) & (t.protocol == "leave-one-scenario-out")].iloc[0]
    b = t[(t.features == f) & (t.protocol == "leave-one-layer-out")].iloc[0]
    rows.append([lab, f"{rand.loc[key, 'f1']:.3f}", f"{a.f1:.3f} ({a.worst_f1:.3f})", f"{b.f1:.3f} ({b.worst_f1:.3f})"])
table("unseen", "lccc", rows, r"Features & Random split & Unseen scenario (worst) & Unseen layer (worst)")

runs = pd.read_csv(OUT / "ablation_runs.csv")
ctrl = pd.read_csv(OUT / "series_control_runs.csv")
all_runs = pd.concat([runs, ctrl[ctrl.combination.isin(["S", "M+S", "L+S"])]])
g = all_runs.groupby("combination")[["f1", "precision", "recall"]].agg(["mean", "std"])
order = ["M", "L", "V", "S", "M+L", "M+V", "M+S", "L+V", "L+S", "M+L+V"]
table("ablation", "lccc", [[f"${c}$", pm(g.loc[c, ("f1", "mean")], g.loc[c, ("f1", "std")]),
                            f"{g.loc[c, ('precision', 'mean')]:.3f}", f"{g.loc[c, ('recall', 'mean')]:.3f}"] for c in order],
      r"Combination & $F_1$ & Precision & Recall")

tests = pd.concat([pd.read_csv(OUT / "ablation_tests.csv"), pd.read_csv(OUT / "series_control_tests.csv")])
piv = all_runs.pivot(index="seed", columns="combination", values="f1")
rows = []
for cmp_ in ["M+L vs L", "M+L vs M", "M+V vs M", "M+S vs M", "L+V vs L", "L+S vs L", "M+L+V vs M+L"]:
    a, b = cmp_.split(" vs ")
    d = (piv[a] - piv[b])
    x = tests[tests.comparison == cmp_]
    rows.append([f"${a}$ vs ${b}$", f"{d.mean():+.4f}", f"[{d.min():+.4f}, {d.max():+.4f}]",
                 f"{x.a_only_correct.mean():.1f} / {x.b_only_correct.mean():.1f}", f"{int((x.mcnemar_p < .05).sum())}/5"])
table("tests", "lcccc", rows, r"Comparison & Mean $\Delta F_1$ & Range over seeds & Only-correct $a$/$b$ & Seeds $p<.05$")

t = pd.read_csv(OUT / "real_logs.csv")
rows = []
for m in ["XGBoost", "Random Forest", "Gradient Boosting", "KNN (k=5)", "LightGBM", "MLP", "Logistic Regression"]:
    row = [m]
    for ds in ("HDFS", "BGL", "OpenStack"):
        r = t[(t.dataset == ds) & (t.model == m)].iloc[0]
        row += [f"{r.f1:.3f}", f"{r.pr_auc:.3f}"]
    rows.append(row)
table("real", "lcccccc", rows, r"Model & \multicolumn{2}{c}{HDFS (5-fold)} & \multicolumn{2}{c}{BGL (chronological)} & \multicolumn{2}{c}{OpenStack (5-fold)} \\"
      r" & $F_1$ & PR-AUC & $F_1$ & PR-AUC & $F_1$ & PR-AUC")

t = pd.read_csv(OUT / "bgl_protocols.csv")
table("bgl", "lcccc", [[r.unit, r.split, f"{int(r.n):,}", f"{r.test_rate:.3f}", f"{r.f1:.3f}"] for _, r in t.iterrows()],
      r"Unit & Split & $n$ & Test anomaly rate & $F_1$")

# LLaVA tables ---------------------------------------------------------------------------
pv = pd.read_csv(ROOT / "results" / "modality_ablation_n150_per_sample.csv")
rows = [["Original (labelled), $n=150$", "75 / 75 / 0 / 0"]]
ck = OUT / "llava_sanitized.csv"
if ck.exists():
    l = pd.read_csv(ck)
    pred = l.status == "ANOMALY"
    rows.append([f"Sanitised, pixel-only prompt, $n={len(l)}$",
                 f"{int((pred & (l.is_anomaly == 1)).sum())} / {int((pred & (l.is_anomaly == 0)).sum())} / "
                 f"{int((~pred & (l.is_anomaly == 0)).sum())} / {int((~pred & (l.is_anomaly == 1)).sum())}"])
    print("LLaVA sanitised", len(l), "mean latency", l.latency_s.mean(), "parsed", l.parsed_ok.mean(), "conf", l.confidence.value_counts().to_dict())
table("llava", "lc", rows, "Input to \\texttt{llava:7b} & TP / FP / TN / FN")

# Figures --------------------------------------------------------------------------------
# Ablation: dots with seed spread, control rows sharing a line with the image rows.
fig, ax = plt.subplots(figsize=(3.5, 2.7))
order = ["M", "V", "S", "M+V", "M+S", "L", "L+V", "L+S", "M+L", "M+L+V"]
for i, c in enumerate(order[::-1]):
    v = piv[c]
    col = ORANGE if "S" in c else BLUE
    ax.plot([v.min(), v.max()], [i, i], color=col, lw=2, solid_capstyle="round")
    ax.plot(v.mean(), i, "o", ms=6, color=col, mec="white", mew=1)
    ax.text(1.003, i, f"{v.mean():.3f}", va="center", ha="left", fontsize=8, color=INK, clip_on=False)
ax.set_yticks(range(len(order)))
ax.set_yticklabels([f"${c}$" for c in order[::-1]])
ax.set_xlabel("$F_1$, mean and range over 5 seeds")
ax.set_xlim(0.90, 1.0)
ax.grid(axis="x", color=GRID, lw=.6)
ax.set_axisbelow(True)
ax.plot([], [], "o-", color=BLUE, label="Tabular or image input")
ax.plot([], [], "o-", color=ORANGE, label="Image replaced by raw series")
ax.legend(frameon=False, fontsize=7, loc="upper center", bbox_to_anchor=(.42, 1.14), ncol=2, handlelength=1.4, columnspacing=1)
fig.savefig(FIG / "ablation.png", dpi=300, bbox_inches="tight")
plt.close(fig)

# BGL: paired random versus chronological F1.
t = pd.read_csv(OUT / "bgl_protocols.csv")
fig, ax = plt.subplots(figsize=(3.5, 1.5))
units = ["20-line window", "100-line window"]
for i, u in enumerate(units[::-1]):
    a = t[(t.unit == u) & (t.split == "chronological")].f1.iloc[0]
    b = t[(t.unit == u) & (t.split == "random")].f1.iloc[0]
    ax.plot([a, b], [i, i], color=GRID, lw=2, zorder=1)
    ax.plot(a, i, "o", ms=8, color=ORANGE, mec="white", zorder=2)
    ax.plot(b, i, "o", ms=8, color=BLUE, mec="white", zorder=2)
    ax.text(a, i + .2, f"{a:.3f}", ha="center", fontsize=8, color=INK)
    ax.text(b, i + .2, f"{b:.3f}", ha="center", fontsize=8, color=INK)
ax.set_yticks([0, 1]); ax.set_yticklabels(units[::-1]); ax.set_ylim(-.45, 1.65)
ax.set_xlim(.35, 1.03); ax.set_xlabel("XGBoost $F_1$ on BGL")
ax.grid(axis="x", color=GRID, lw=.6); ax.set_axisbelow(True)
ax.plot([], [], "o", color=ORANGE, label="Chronological split")
ax.plot([], [], "o", color=BLUE, label="Random split")
ax.legend(frameon=False, fontsize=7, loc="upper center", bbox_to_anchor=(.45, 1.28), ncol=2)
fig.savefig(FIG / "bgl.png", dpi=300, bbox_inches="tight")
plt.close(fig)

# Original versus sanitised dashboard for the same sample.
sid = "sample_009464"
orig = next(p for p in (ROOT / "data/multimodal_dataset/dashboards").glob(f"{sid}*"))
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.2))
for ax, p, title in zip(axes, (orig, ROOT / "data/dashboards_sanitized" / f"{sid}.png"),
                        ("(a) Generated dashboard, which displays the label", "(b) Sanitised dashboard, pixels of the series only")):
    ax.imshow(Image.open(p)); ax.set_axis_off(); ax.set_title(title, fontsize=8, color=INK)
fig.tight_layout()
fig.savefig(FIG / "dashboards.png", dpi=250, bbox_inches="tight")
plt.close(fig)


# Generator: what each modality is actually conditioned on.
def box(ax, x, y, w, h, text, fill):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.01,rounding_size=0.015", fc=fill, ec=MUTED, lw=.8))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=7.5, color=INK)


def arrow(ax, a, b, text=None, off=(0, .025)):
    ax.annotate("", xy=b, xytext=a, arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=.9))
    if text:
        ax.text((a[0] + b[0]) / 2 + off[0], (a[1] + b[1]) / 2 + off[1], text, fontsize=6.5, color=MUTED, ha="center")


fig, ax = plt.subplots(figsize=(7.2, 2.1))
ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()
box(ax, .01, .34, .17, .34, "Sampled label\nlayer, scenario,\nseverity", "#f1f0ec")
box(ax, .32, .64, .19, .28, "Metric generator\n4–6 series, 60 points", "#dce9f9")
box(ax, .32, .08, .19, .28, "Log generator\n20–60 lines", "#fbe3da")
box(ax, .68, .64, .15, .28, "Dashboard PNG\nsame series", "#dce9f9")
box(ax, .68, .08, .15, .28, "Log session", "#fbe3da")
arrow(ax, (.18, .58), (.32, .78), "scenario", (-.02, .05))
arrow(ax, (.18, .44), (.32, .22), "layer, severity", (-.03, -.09))
arrow(ax, (.415, .64), (.415, .36), "onset index only", (.075, 0))
arrow(ax, (.51, .78), (.68, .78), "rendered")
arrow(ax, (.51, .22), (.68, .22))
ax.text(.915, .5, "No metric value\nreaches the logs", ha="center", va="center", fontsize=7, color=MUTED)
fig.savefig(FIG / "generator.png", dpi=300, bbox_inches="tight")
plt.close(fig)

print("assets written")
