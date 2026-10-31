"""Objective benchmark of the language-model layer against the scenario catalogue, with no human ratings.

The prompt withholds the catalogue's root cause category and remediation, so both can be scored.
Usage: python run_llm_benchmark.py model[,model...] [repeats]. Resumes from its checkpoint.
"""

import json
import re
import sys
import time

import pandas as pd
import requests
import yaml
from common import OUT, ROOT

SYSTEM = """You are a Site Reliability Engineer. Reply with ONLY a JSON object with the keys:
"explanation" (2 sentences), "root_cause_category" (exactly one of network, software, resource, config, hardware),
"remediation_steps" (ordered list of at most 5 short actions, with shell commands where useful),
"estimated_resolution_time" (string), "confidence_score" (float 0-1)."""
RISKY = re.compile(r"rm\s+-rf|\breboot\b|\bshutdown\b|drop_caches|swapoff|kubectl\s+delete|iptables\s+-F|kill\s+-9|"
                   r"\bmkfs\b|\bfsck\b|systemctl\s+(restart|stop)|DROP\s+TABLE", re.I)
STOP = set("the a an of on in to and or for with from by if is are check apply trigger increase send alerts".split())

layers = yaml.safe_load(open(ROOT / "configs" / "anomaly_scenarios.yaml", encoding="utf8"))["layers"]
scen = [(v["name"], s) for v in layers.values() for s in v["scenarios"]]


def parse(txt):
    """Direct parse, then fenced or first balanced object; returns (object, level)."""
    try:
        return json.loads(txt), 1
    except Exception:
        pass
    start = txt.find("{")
    depth = 0
    for i in range(max(start, 0), len(txt)):
        depth += txt[i] == "{"
        depth -= txt[i] == "}"
        if start >= 0 and depth == 0:
            try:
                return json.loads(txt[start:i + 1]), 2
            except Exception:
                break
    return None, 0


def tokens(s):
    return {w for w in re.findall(r"[a-z]{3,}", s.lower()) if w not in STOP}


models = sys.argv[1].split(",")
repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 2
ck = OUT / "llm_benchmark.csv"
rows = pd.read_csv(ck).to_dict("records") if ck.exists() else []
done = {(r["model"], r["scenario"], r["repeat"]) for r in rows}
for model in models:
    for rep in range(repeats):
        for layer, s in scen:
            if (model, s["id"], rep) in done:
                continue
            user = (f"Detector output. Layer: {layer}. Scenario: {s['name']}. Severity: {s['severity']}. "
                    f"Metrics that deviated: {', '.join(s['affected_metrics'])}. Log ERROR and CRITICAL lines increased after onset.")
            t0 = time.time()
            r = requests.post("http://localhost:11434/api/chat", timeout=900, json={
                "model": model, "stream": False, "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
                "options": {"temperature": 0.1, "seed": rep, "num_ctx": 4096}}).json()
            txt = r.get("message", {}).get("content", "")
            obj, lvl = parse(txt)
            steps = obj.get("remediation_steps", []) if isinstance(obj, dict) else []
            rem = " ".join(json.dumps(x) if not isinstance(x, str) else x for x in steps) if isinstance(steps, list) else str(steps)
            gold = tokens(" ".join(s["remediation"]))
            cat = str(obj.get("root_cause_category", "")).strip().lower() if isinstance(obj, dict) else ""
            rows.append({"model": model, "scenario": s["id"], "repeat": rep, "latency_s": round(time.time() - t0, 1), "parse_level": lvl,
                         "schema_ok": isinstance(obj, dict) and all(k in obj for k in ("explanation", "root_cause_category", "remediation_steps")),
                         "category": cat, "gold_category": s["root_cause_category"], "category_ok": cat == s["root_cause_category"],
                         "gold_coverage": len(gold & tokens(rem)) / max(1, len(gold)), "n_steps": len(steps) if isinstance(steps, list) else 0,
                         "risky": bool(RISKY.search(rem)), "raw": txt[:1500]})
            pd.DataFrame(rows).to_csv(ck, index=False)
            print(model, s["id"], rep, rows[-1]["latency_s"], lvl, cat, rows[-1]["risky"], flush=True)
print(pd.DataFrame(rows).groupby("model")[["latency_s", "schema_ok", "category_ok", "gold_coverage", "risky"]].mean())
