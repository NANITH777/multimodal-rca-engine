import sys
import traceback
import os

results = []
results.append(f"Python: {sys.executable}")
results.append(f"Version: {sys.version}")
results.append(f"Paths:")
for p in sys.path:
    results.append(f"  {p}")
results.append("")
try:
    import drain3
    results.append(f"drain3 OK: {drain3.__file__}")
except Exception as e:
    results.append(f"IMPORT FAILED: {type(e).__name__}: {e}")
    import io
    buf = io.StringIO()
    traceback.print_exc(file=buf)
    results.append(buf.getvalue())
    results.append("--- Checking if drain3 files exist ---")
    user_site = os.path.join(os.environ.get('APPDATA',''), 'Python', 'Python313', 'site-packages', 'drain3')
    results.append(f"User site drain3 dir: {user_site}")
    results.append(f"Exists: {os.path.exists(user_site)}")
    if os.path.exists(user_site):
        for f in os.listdir(user_site):
            results.append(f"  {f}")

out = "\n".join(results)
with open(r"d:\multimodal-rca-engine\_drain_diag.txt", "w", encoding="utf-8") as f:
    f.write(out)
print(out)
