"""
Generate Multimodal RCA Dataset — Entry Point Script.

Usage:
    # Small test (verify everything works)
    python generate.py --test

    # Medium dataset (1000 samples for quick experiments)
    python generate.py --size medium

    # Full dataset (100K samples for training)
    python generate.py --size full

    # Custom size
    python generate.py --total 50000 --anomaly-ratio 0.5

    # With full-panel dashboards (larger images, slower)
    python generate.py --size full --dashboard full
"""

import argparse
import sys
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataset_generator.dataset_builder import DatasetBuilder


PRESETS = {
    "test":   {"total": 165, "anomaly_ratio": 0.50, "progress_interval": 50},
    "small":  {"total": 1000, "anomaly_ratio": 0.50, "progress_interval": 100},
    "medium": {"total": 10000, "anomaly_ratio": 0.50, "progress_interval": 500},
    "large":  {"total": 50000, "anomaly_ratio": 0.50, "progress_interval": 1000},
    "full":   {"total": 100000, "anomaly_ratio": 0.50, "progress_interval": 2000},
    "mega":   {"total": 200000, "anomaly_ratio": 0.50, "progress_interval": 5000},
}


def main():
    parser = argparse.ArgumentParser(
        description="Generate Multimodal RCA Training Dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Preset sizes:
  test    →     165 samples  (quick verification)
  small   →   1,000 samples  (~1 min)
  medium  →  10,000 samples  (~10 min)
  large   →  50,000 samples  (~45 min)
  full    → 100,000 samples  (~90 min)
  mega    → 200,000 samples  (~3 hours)
        """
    )

    parser.add_argument("--size", type=str, default=None,
                        choices=list(PRESETS.keys()),
                        help="Preset dataset size")
    parser.add_argument("--total", type=int, default=None,
                        help="Custom total number of samples")
    parser.add_argument("--anomaly-ratio", type=float, default=0.50,
                        help="Fraction of anomalous samples (default: 0.50)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--dashboard", type=str, default="compact",
                        choices=["compact", "full"],
                        help="Dashboard mode: compact (overlay) or full (multi-panel)")
    parser.add_argument("--output", type=str, default=None,
                        help="Custom output directory")
    parser.add_argument("--test", action="store_true",
                        help="Run small test generation")
    parser.add_argument("--no-dashboard", action="store_true",
                        help="Skip dashboard image generation (faster)")

    args = parser.parse_args()

    # Determine parameters
    if args.test:
        preset = PRESETS["test"]
    elif args.size:
        preset = PRESETS[args.size]
    elif args.total:
        preset = {"total": args.total, "anomaly_ratio": args.anomaly_ratio,
                  "progress_interval": max(100, args.total // 50)}
    else:
        print("⚠️  No size specified. Use --size or --total. Defaulting to 'small'.")
        preset = PRESETS["small"]

    # Override from args
    if args.total:
        preset["total"] = args.total
    if args.anomaly_ratio != 0.50:
        preset["anomaly_ratio"] = args.anomaly_ratio

    # Build
    output_dir = args.output if args.output else None
    builder = DatasetBuilder(
        output_dir=output_dir,
        seed=args.seed,
        dashboard_mode=args.dashboard
    )

    start = time.time()

    if args.test:
        metadata_path = builder.build_small_test(samples_per_class=5)
    else:
        metadata_path = builder.build_dataset(
            total_samples=preset["total"],
            anomaly_ratio=preset["anomaly_ratio"],
            progress_interval=preset["progress_interval"]
        )

    elapsed = time.time() - start
    print(f"\n🎯 Done in {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"📂 Dataset at: {builder.output_dir}")
    print(f"📋 Metadata:   {metadata_path}")


if __name__ == "__main__":
    main()
