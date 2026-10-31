"""Builds paper/overleaf.zip: one self-contained main.tex with tables inlined, plus the figures it uses."""

import re
import zipfile

from common import ROOT

PAPER = ROOT / "paper"
src = (PAPER / "main.tex").read_text(encoding="utf8")
src = re.sub(r"\\newcommand\{\\tabfile\}.*\n", "", src)
src = re.sub(r"\\tabfile\{(.+?)\}", lambda m: (PAPER / "tables" / f"{m.group(1)}.tex").read_text(encoding="utf8").strip(), src)
src = src.lstrip("\ufeff").split("\n", 1)[1] if src.startswith("%") else src
assert src.startswith("\\documentclass"), src[:40]
figs = re.findall(r"\\includegraphics\[.*?\]\{(.+?)\}", src)
print("non-ascii:", sorted({c for c in src if ord(c) > 127}))
with zipfile.ZipFile(PAPER / "overleaf.zip", "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("main.tex", src.encode("utf8"))
    for f in figs:
        z.write(PAPER / "figures" / f, f"figures/{f}")
print("files:", ["main.tex"] + figs, "bytes", (PAPER / "overleaf.zip").stat().st_size)
