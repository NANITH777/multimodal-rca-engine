"""Builds zenodo/dataset.zip and zenodo/code_and_results.zip for upload. Secrets, caches and third-party logs stay out."""

import re
import zipfile

from common import ROOT

OUT = ROOT / "zenodo"
OUT.mkdir(exist_ok=True)
KEY = re.compile(rb"AIza[0-9A-Za-z_-]{20,}|sk-[A-Za-z0-9_-]{20,}|(?:api[_-]?key|token|secret)\s*[=:]\s*['\"][^'\"\s]{16,}", re.I)
SKIP_DIRS = {"__pycache__", ".ipynb_checkpoints", ".git"}
SKIP_NAMES = {".env", "feature_cache.npz", "sanitized_pixels.npy"}
CODE = ["configs", "dataset_generator", "models", "src", "experiments", "dataset_analysis", "notebooks", "results"]
TOP = ["README.md", "requirements.txt", "demo_vlm.py", "demo_gemini.py"]

flagged = []


def add(z, path):
    data = path.read_bytes()
    if KEY.search(data):
        flagged.append(str(path.relative_to(ROOT)))  # the match itself is never printed
        return
    z.writestr(str(path.relative_to(ROOT)).replace("\\", "/"), data)


with zipfile.ZipFile(OUT / "code_and_results.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for name in TOP:
        if (ROOT / name).exists():
            add(z, ROOT / name)
    for d in CODE:
        for p in sorted((ROOT / d).rglob("*")):
            if p.is_file() and p.name not in SKIP_NAMES and not p.name.startswith(".env") and not SKIP_DIRS & set(p.parts) and p.suffix != ".log":
                add(z, p)
    n_code = len(z.namelist())

with zipfile.ZipFile(OUT / "dataset.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for p in sorted((ROOT / "data" / "multimodal_dataset").rglob("*")):
        if p.is_file():
            z.write(p, str(p.relative_to(ROOT)).replace("\\", "/"))
    n_data = len(z.namelist())

print("code files", n_code, "MB", round((OUT / "code_and_results.zip").stat().st_size / 1e6, 1))
print("dataset files", n_data, "MB", round((OUT / "dataset.zip").stat().st_size / 1e6, 1))
print("excluded because a possible secret was found:", flagged)
