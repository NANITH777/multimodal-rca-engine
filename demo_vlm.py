"""
Demonstration script for the VLM (Vision-Language Model) integration in the
Multimodal RCA Engine. Runs the dual-engine visual analysis (Gemini Vision
and/or local Ollama LLaVA) directly on generated dashboard PNG images and
compares the visual verdict against ground truth.

Usage:
    python demo_vlm.py                 # both engines, if available
    python demo_vlm.py --backend gemini
    python demo_vlm.py --backend ollama
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

DATASET_DIR = Path(__file__).parent / "data" / "multimodal_dataset"

# One normal and one clearly anomalous sample, picked from metadata.csv
EXAMPLE_SAMPLES = [
    {"sample_id": "sample_000001", "expected": "NORMAL"},
    {"sample_id": "sample_005001", "expected": "ANOMALY"},
]


def load_ground_truth(sample_id):
    label_path = DATASET_DIR / "labels" / f"{sample_id}.json"
    if not label_path.exists():
        return None
    with open(label_path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_backend(backend_name):
    from models.vlm_engine import VLMEngine

    print(f"\n{'=' * 65}")
    print(f"  Backend: {backend_name.upper()}")
    print(f"{'=' * 65}")

    try:
        engine = VLMEngine(backend=backend_name)
    except (ImportError, ValueError) as e:
        print(f"  Skipped ({e})")
        return

    for sample in EXAMPLE_SAMPLES:
        sid = sample["sample_id"]
        image_path = DATASET_DIR / "dashboards" / f"{sid}.png"
        if not image_path.exists():
            print(f"\n[{sid}] dashboard image not found at {image_path} — run "
                  f"dataset_generator/generate.py first.")
            continue

        gt = load_ground_truth(sid)
        gt_status = "ANOMALY" if gt and gt.get("is_anomaly") else "NORMAL"
        gt_scenario = gt.get("scenario_name") if gt else None

        print(f"\n--- {sid} (ground truth: {gt_status}"
              f"{', ' + gt_scenario if gt_scenario else ''}) ---")

        result = engine.analyze_dashboard(image_path)

        if "error" in result:
            print(f"  ERROR: {result['error']}")
            continue

        match = "MATCH" if result.get("visual_status") == gt_status else "MISMATCH"
        print(f"  visual_status      : {result.get('visual_status')}  [{match}]")
        print(f"  visual_confidence  : {result.get('visual_confidence')}")
        print(f"  visual_pattern     : {result.get('visual_pattern')}")
        print(f"  visual_explanation : {result.get('visual_explanation')}")
        print(f"  latency_seconds    : {result.get('latency_seconds')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["gemini", "ollama", "both"], default="both")
    args = parser.parse_args()

    print("=" * 65)
    print("  Multimodal RCA Engine — VLM (Vision) Integration Demonstration")
    print("=" * 65)

    backends = ["gemini", "ollama"] if args.backend == "both" else [args.backend]
    for b in backends:
        run_backend(b)

    print("\n" + "=" * 65)
    print("  VLM demonstration completed.")
    print("=" * 65)
