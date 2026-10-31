"""LLaVA on the same N=150 test images: original vs sanitised dashboards, with a pixel-only prompt.

Usage: python run_llava_sanitized.py [n_per_class]. Resumes from its checkpoint.
"""

import base64
import json
import re
import sys
import time

import pandas as pd
import requests
from common import OUT, ROOT

PROMPT = """You are a Site Reliability Engineer reading a monitoring chart with several normalised \
time series on one panel. Decide from the shape of the lines alone whether they show anomalous \
behaviour (spike, gradual rise, sustained plateau, oscillation, step change) or normal noisy behaviour \
with slow drift and occasional small spikes. Roughly half of the charts you will see are normal.
Respond with ONLY a JSON object: {"visual_status": "ANOMALY" or "NORMAL", "visual_confidence": <float 0-1>}"""

n_per = int(sys.argv[1]) if len(sys.argv) > 1 else 30
base = pd.read_csv(ROOT / "results" / "modality_ablation_n150_per_sample.csv")
sample = pd.concat([g.head(n_per) for _, g in base.groupby("is_anomaly")])
ck = OUT / "llava_sanitized.csv"
done = pd.read_csv(ck) if ck.exists() else pd.DataFrame(columns=["sample_id"])
rows = done.to_dict("records")
for _, r in sample.iterrows():
    if r.sample_id in set(done.sample_id):
        continue
    img = base64.b64encode((ROOT / "data" / "dashboards_sanitized" / f"{r.sample_id}.png").read_bytes()).decode()
    t0 = time.time()
    txt = requests.post("http://localhost:11434/api/generate", timeout=900, json={
        "model": "llava:7b", "prompt": PROMPT, "images": [img], "stream": False,
        "format": "json", "options": {"temperature": 0, "seed": 0}}).json().get("response", "")
    try:
        js = json.loads(txt); status = str(js.get("visual_status", "")).upper(); conf = float(js.get("visual_confidence", 0))
        ok = status in ("ANOMALY", "NORMAL")
    except Exception:
        m = re.search(r"ANOMALY|NORMAL", txt.upper()); status = m.group(0) if m else ""; conf = 0.0; ok = False
    rows.append({"sample_id": r.sample_id, "is_anomaly": r.is_anomaly, "status": status, "confidence": conf,
                 "parsed_ok": ok, "latency_s": round(time.time() - t0, 1), "raw": txt[:300]})
    pd.DataFrame(rows).to_csv(ck, index=False)
    print(len(rows), r.sample_id, r.is_anomaly, status, rows[-1]["latency_s"], flush=True)
