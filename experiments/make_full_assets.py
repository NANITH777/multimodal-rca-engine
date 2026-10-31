"""Tables and figures for the detector, explanation and vision sections of paper/main.tex."""

import json
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from common import OUT, ROOT
from matplotlib.patches import FancyBboxPatch

sys.path.insert(0, str(ROOT))
from dataset_generator.log_generator import LOG_TEMPLATES  # noqa: E402

PAPER = ROOT / "paper"
FIG, TAB = PAPER / "figures", PAPER / "tables"
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#6250d6"]
plt.rcParams.update({"font.family": "serif", "font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
                     "xtick.color": MUTED, "ytick.color": INK, "axes.spines.top": False, "axes.spines.right": False})


def table(name, spec, rows, header):
    body = "\n".join(" & ".join(map(str, r)) + r" \\" for r in rows)
    (TAB / f"{name}.tex").write_text(
        f"\\begin{{tabular}}{{@{{}}{spec}@{{}}}}\n\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n", encoding="utf8")


esc = lambda s: str(s).replace("_", r"\_").replace("%", r"\%").replace("&", r"\&").replace("#", r"\#")

# Detector tables ------------------------------------------------------------------------
t = pd.read_csv(OUT / "cv_models.csv").sort_values("f1", ascending=False)
table("cv", "lcc", [[r.model, f"{r.f1:.4f} $\\pm$ {r['std']:.4f}", f"{r.auc:.4f}"] for _, r in t.iterrows()], r"Model & $F_1$ & AUC")

d = pd.read_csv(OUT / "detector_details.csv")
g = d.groupby("model").mean(numeric_only=True)
s = d.groupby("model").f1.std()
order = ["Random Forest", "XGBoost", "XGBoost, regularised", "LightGBM"]
table("detector", "lccccc", [[m, f"{g.loc[m, 'accuracy']:.4f}", f"{g.loc[m, 'precision']:.4f}", f"{g.loc[m, 'recall']:.4f}",
                              f"{g.loc[m, 'f1']:.4f} $\\pm$ {s[m]:.4f}", f"{g.loc[m, 'auc']:.4f}"] for m in order],
      r"Model & Accuracy & Precision & Recall & $F_1$ & AUC")
table("confusion", "lcccc", [[m, f"{g.loc[m, 'tn']:.1f}", f"{g.loc[m, 'fp']:.1f}", f"{g.loc[m, 'fn']:.1f}", f"{g.loc[m, 'tp']:.1f}"] for m in order],
      r"Model & TN & FP & FN & TP")
table("gaps", "lcccc", [[m, f"{g.loc[m, 'train_f1']:.4f}", f"{g.loc[m, 'val_f1']:.4f}", f"{g.loc[m, 'f1']:.4f}", f"{g.loc[m, 'train_f1'] - g.loc[m, 'f1']:.4f}"] for m in order],
      r"Model & Train & Validation & Test & Train $-$ test")
print("regularised best iter", g.loc["XGBoost, regularised", "best_iter"])

t = pd.read_csv(OUT / "rca_accuracy.csv")
table("rca_acc", "lccc", [[r.task, f"{r.accuracy:.4f}", f"{r.f1_w:.4f}", f"{r.mean_errors:.1f}"] for _, r in t.iterrows()],
      r"Target (classes) & Accuracy & Weighted $F_1$ & Errors of 1,000")

t = pd.read_csv(OUT / "metric_examples.csv")
table("metric_examples", "lccccc", [[r["sample"], int(r.n), f"{r['mean']:.0f}", f"{r['std']:.0f}", f"{r.slope:.2f}", f"{r['autocorr']:.2f}"] for _, r in t.iterrows()],
      r"Samples & $n$ & Mean & Std.\ dev. & Slope & Lag-1 autocorr.")

t = pd.read_csv(OUT / "shap_local_fw002.csv")
table("shap_local", "lcc", [[f"\\texttt{{{esc(r.feature)}}}", f"{r.value:.3f}", f"{r.shap:+.3f}"] for _, r in t.iterrows()], r"Feature & Value & SHAP (log-odds)")
print("shap local", t["sample"].iloc[0], t.base.iloc[0])

# Real log templates, shortened to the first template per level.
rows = []
for layer, name in (("cdn", "CDN"), ("linux_vm", "Linux VM")):
    for lvl in ("INFO", "WARN", "ERROR", "CRITICAL"):
        tpl = LOG_TEMPLATES[layer][lvl][0]
        tpl = re.sub(r"\{([^{}]+)\}", lambda m: r"\texttt{<" + m.group(1) + ">}", tpl.replace("%", r"\%").replace("_", r"\_").replace("#", r"\#"))
        rows.append([name, lvl, tpl])
(TAB / "log_examples.tex").write_text(
    "\\begin{tabularx}{\\columnwidth}{@{}llX@{}}\n\\toprule\nLayer & Level & Template \\\\\n\\midrule\n"
    + "\n".join(" & ".join(r) + r" \\" for r in rows) + "\n\\bottomrule\n\\end{tabularx}\n", encoding="utf8")

# Vision tables --------------------------------------------------------------------------
v = pd.read_csv(ROOT / "results" / "vlm_benchmark_results.csv")
print(v.groupby("backend")[["correct", "latency_seconds", "visual_confidence"]].mean(), v.groupby("backend").size())
rows = []
for b, lab in (("gemini", "\\texttt{gemini-3.5-flash}"), ("ollama", "\\texttt{llava:7b}")):
    x = v[v.backend.str.contains(b[:4])]
    if len(x):
        rows.append([lab, len(x), f"{x.correct.mean():.2f}", f"{x.parsed_ok.mean():.2f}", f"{x.latency_seconds.mean():.1f}", f"{x.visual_confidence.mean():.2f}"])
table("vlm_small", "lccccc", rows, r"Engine & $n$ & Accuracy & Parsed & Latency (s) & Confidence")

a = pd.read_csv(ROOT / "results" / "modality_ablation_n150_summary.csv").set_index("combination")
c = pd.read_csv(ROOT / "results" / "modality_ablation_n150_bootstrap_ci.csv").set_index("combination")
table("n150", "lcccc", [[f"${k}$", f"{a.loc[k, 'precision']:.3f}", f"{a.loc[k, 'recall']:.3f}", f"{a.loc[k, 'f1']:.3f}", f"[{c.loc[k, 'ci_low']:.3f}, {c.loc[k, 'ci_high']:.3f}]"] for k in a.index],
      r"Combination & Precision & Recall & $F_1$ & 95\% CI")

# Language-model benchmark ---------------------------------------------------------------
l = pd.read_csv(OUT / "llm_benchmark.csv")
rows = []
for m, x in l.groupby("model"):
    rows.append([f"\\texttt{{{m}}}", len(x), f"{x.latency_s.mean():.1f}", f"{(x.parse_level == 1).mean():.2f}", f"{(x.parse_level > 0).mean():.2f}", f"{x.category_ok.mean():.2f}", f"{x.gold_coverage.mean():.2f}", f"{x.risky.mean():.2f}"])
    print(m, len(x), x.latency_s.median(), x.groupby("repeat").category_ok.mean().to_dict())
    print(x.category.value_counts().to_dict())
table("llm", "lccccccc", rows, r"Engine & $n$ & Latency (s) & Direct parse & Any parse & Category & Coverage & Risky")

# Figures --------------------------------------------------------------------------------
sh = pd.read_csv(OUT / "shap_importance.csv").head(20)[::-1]
fam = {"levels": BLUE, "metrics": ORANGE, "templates": "#1baf7a"}
fig, ax = plt.subplots(figsize=(3.5, 3.6))
ax.barh(range(len(sh)), sh.mean_abs_shap, color=[fam[f] for f in sh.family], height=.72)
ax.set_yticks(range(len(sh))); ax.set_yticklabels(sh.feature, fontsize=6.5, family="monospace")
ax.set_xlabel("Mean $|$SHAP$|$ (log-odds)"); ax.set_xscale("log")
ax.grid(axis="x", color=GRID, lw=.6); ax.set_axisbelow(True)
for k, lab in (("levels", "Log levels"), ("metrics", "Metric statistics"), ("templates", "TF-IDF terms")):
    ax.barh([0], [0], color=fam[k], label=lab)
ax.legend(frameon=False, fontsize=7, loc="lower right")
fig.savefig(FIG / "shap.png", dpi=300, bbox_inches="tight"); plt.close(fig)

labels = {}
for p in sorted((ROOT / "data/multimodal_dataset/labels").glob("*.json")):
    x = json.load(open(p, encoding="utf8")); labels[x["sample_id"]] = x
normal = next(k for k, x in labels.items() if x["layer"] == "cdn" and not x["is_anomaly"])
anom = next(k for k, x in labels.items() if x["scenario_id"] == "CDN_001")
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.1), sharey=True)
for ax, sid, title in zip(axes, (normal, anom), ("(a) Normal sample", "(b) CDN\\_001, Sudden Traffic Spike".replace("\\", ""))):
    df = pd.read_csv(ROOT / "data/multimodal_dataset/metrics" / f"{sid}.csv")
    for i, col in enumerate(k for k in df.columns if k != "timestamp"):
        y = df[col].values.astype(float); r = y.max() - y.min()
        ax.plot((y - y.min()) / r if r else y * 0, lw=1.2, color=SERIES[i], label=col)
    if labels[sid]["anomaly_start_idx"] is not None:
        ax.axvline(labels[sid]["anomaly_start_idx"], color=MUTED, ls="--", lw=.8)
        ax.text(labels[sid]["anomaly_start_idx"] + 1, 1.02, "onset", fontsize=7, color=MUTED)
    ax.set_title(title, fontsize=8, color=INK, loc="left"); ax.set_xlabel("Point (60 s)")
    ax.grid(axis="y", color=GRID, lw=.6)
axes[0].set_ylabel("Min-max normalised")
axes[1].legend(frameon=False, fontsize=6.5, ncol=1, loc="center left", bbox_to_anchor=(1.0, .5))
fig.tight_layout()
fig.savefig(FIG / "series.png", dpi=300, bbox_inches="tight"); plt.close(fig)

y_ = yaml.safe_load(open(ROOT / "configs/anomaly_scenarios.yaml", encoding="utf8"))["layers"]
cats = ["network", "resource", "software", "hardware", "config"]
mat = pd.DataFrame(0, index=[v_["name"].split(" (")[0] for v_ in y_.values()], columns=cats)
for v_ in y_.values():
    for s_ in v_["scenarios"]:
        mat.loc[v_["name"].split(" (")[0], s_["root_cause_category"]] += 1
print(mat.sum().to_dict())
fig, ax = plt.subplots(figsize=(3.5, 2.7))
ax.imshow(mat.values, cmap=matplotlib.colors.LinearSegmentedColormap.from_list("b", ["#f7f7f5", "#a9c8ee", BLUE]), vmin=0, vmax=2, aspect="auto")
for i in range(mat.shape[0]):
    for j in range(mat.shape[1]):
        if mat.values[i, j]:
            ax.text(j, i, mat.values[i, j], ha="center", va="center", fontsize=8, color="white" if mat.values[i, j] == 2 else INK)
ax.set_xticks(range(5)); ax.set_xticklabels(cats, rotation=30, ha="right", fontsize=7.5)
ax.set_yticks(range(len(mat))); ax.set_yticklabels(mat.index, fontsize=7.5)
ax.tick_params(length=0)
for sp in ax.spines.values():
    sp.set_visible(False)
fig.savefig(FIG / "matrix.png", dpi=300, bbox_inches="tight"); plt.close(fig)


def box(ax, x, y, w, h, text, fill, dashed=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.005,rounding_size=0.012", fc=fill, ec=MUTED, lw=.8, ls="--" if dashed else "-"))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=7, color=INK)


def arrow(ax, a, b):
    ax.annotate("", xy=b, xytext=a, arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=.9))


fig, ax = plt.subplots(figsize=(7.2, 2.5))
ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()
for y0, txt, fill in ((.70, "Metric series", "#dce9f9"), (.40, "Log session", "#fbe3da"), (.10, "Dashboard PNG", "#e5e3f8")):
    box(ax, .01, y0, .13, .2, txt, fill)
box(ax, .20, .70, .16, .2, "13 statistics\n$\\times$ 8 channels", "#f1f0ec")
box(ax, .20, .40, .16, .2, "Level counts,\nTF-IDF / Drain3", "#f1f0ec")
box(ax, .20, .10, .16, .2, "CNN or vision\nmodel", "#f1f0ec")
box(ax, .42, .40, .15, .5, "Binary detector\n(per modality,\nlate fusion)", "#f1f0ec")
box(ax, .63, .70, .16, .2, "Classifiers: layer,\nscenario, category,\nseverity", "#f1f0ec")
box(ax, .63, .40, .16, .2, "TreeSHAP\nattribution", "#f1f0ec")
box(ax, .84, .55, .15, .35, "Language model\ndraft explanation\nand steps", "#f1f0ec")
box(ax, .84, .10, .15, .28, "Human operator\ndecides and acts", "#ffffff", dashed=True)
for yy in (.8, .5, .2):
    arrow(ax, (.145, yy), (.195, yy))
arrow(ax, (.365, .8), (.415, .75)); arrow(ax, (.365, .5), (.415, .55)); arrow(ax, (.365, .2), (.415, .42))
arrow(ax, (.575, .75), (.625, .8)); arrow(ax, (.575, .55), (.625, .5))
arrow(ax, (.795, .8), (.835, .75)); arrow(ax, (.915, .545), (.915, .39))
ax.annotate("", xy=(.20, .05), xytext=(.20, .05))
fig.savefig(FIG / "architecture.png", dpi=300, bbox_inches="tight"); plt.close(fig)
print("full assets written")
