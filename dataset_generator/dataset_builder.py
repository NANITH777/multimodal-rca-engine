"""
Dataset Builder — Main Orchestrator for the Multimodal RCA Engine.
Assembles logs, metrics, dashboards, and labels into a unified multimodal training dataset.
"""

import json
import csv
import time
import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from .metric_generator import MetricGenerator, load_scenarios
from .log_generator import LogGenerator
from .dashboard_generator import DashboardGenerator


class DatasetBuilder:
    """
    Orchestrates the generation of a massive multimodal RCA training dataset.
    
    Each sample contains:
      - logs/*.txt       → Synthetic log session (text)
      - metrics/*.csv    → Time-series metrics (tabular)
      - dashboards/*.png → Grafana-style dashboard (image)
      - labels/*.json    → RCA labels (structured)
    """

    def __init__(self, output_dir=None, seed=42, dashboard_mode="compact"):
        """
        Args:
            output_dir: Root output directory for the dataset
            seed: Random seed for reproducibility
            dashboard_mode: 'full' (multi-panel) or 'compact' (single-panel overlay)
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / "data" / "multimodal_dataset"
        self.output_dir = Path(output_dir)
        self.seed = seed
        self.dashboard_mode = dashboard_mode

        # Sub-directories
        self.logs_dir = self.output_dir / "logs"
        self.metrics_dir = self.output_dir / "metrics"
        self.dashboards_dir = self.output_dir / "dashboards"
        self.labels_dir = self.output_dir / "labels"

        # Ensure directories exist
        for d in [self.logs_dir, self.metrics_dir, self.dashboards_dir, self.labels_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Generators
        self.metric_gen = MetricGenerator(seed=seed)
        self.log_gen = LogGenerator(seed=seed)
        self.dashboard_gen = DashboardGenerator(dpi=100)

        # Load scenarios
        self.scenarios_config = load_scenarios()

        # Metadata accumulator
        self.metadata_rows = []

    def _get_all_layers(self):
        """Get all layer configurations from scenarios config."""
        return self.scenarios_config.get("layers", {})

    def _generate_single_sample(self, sample_id, layer_key, layer_config,
                                 scenario_config=None, base_time=None):
        """
        Generate a single multimodal sample (log + metrics + dashboard + label).

        Args:
            sample_id: Unique sample identifier string
            layer_key: Layer key (e.g., 'cdn', 'firewall')
            layer_config: Layer configuration dict
            scenario_config: Scenario dict (None for normal)
            base_time: Starting timestamp

        Returns:
            dict: Metadata row for this sample
        """
        is_anomaly = scenario_config is not None
        layer_name = layer_config.get("name", layer_key)

        # 1) Generate Metrics
        metrics_df, meta = self.metric_gen.generate_sample(
            layer_config, scenario_config, base_time
        )
        metrics_path = self.metrics_dir / f"{sample_id}.csv"
        metrics_df.to_csv(metrics_path, index=False)

        anomaly_start_idx = meta.get("anomaly_start_idx")

        # 2) Generate Logs
        log_text = self.log_gen.generate_session_for_scenario(
            layer_key,
            scenario_config=scenario_config,
            anomaly_start_idx=anomaly_start_idx,
            base_time=base_time
        )
        log_path = self.logs_dir / f"{sample_id}.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_text)

        # 3) Generate Dashboard Image
        scenario_name = scenario_config["name"] if scenario_config else None
        severity = scenario_config.get("severity") if scenario_config else None

        dashboard_path = self.dashboards_dir / f"{sample_id}.png"

        try:
            if self.dashboard_mode == "full":
                self.dashboard_gen.generate_dashboard(
                    metrics_df, layer_name,
                    scenario_name=scenario_name,
                    is_anomaly=is_anomaly,
                    anomaly_start_idx=anomaly_start_idx,
                    severity=severity,
                    output_path=dashboard_path
                )
            else:
                self.dashboard_gen.generate_compact_dashboard(
                    metrics_df, layer_name,
                    scenario_name=scenario_name,
                    is_anomaly=is_anomaly,
                    anomaly_start_idx=anomaly_start_idx,
                    severity=severity,
                    output_path=dashboard_path
                )
        except Exception as e:
            print(f"  ⚠️ Dashboard generation failed for {sample_id}: {e}")
            dashboard_path = None

        # 4) Generate Label
        label_data = {
            "sample_id": sample_id,
            "layer": layer_key,
            "layer_name": layer_name,
            "is_anomaly": is_anomaly,
            "scenario_id": meta.get("scenario_id"),
            "scenario_name": scenario_name,
            "root_cause": meta.get("root_cause"),
            "root_cause_category": meta.get("root_cause_category"),
            "severity": severity,
            "remediation": meta.get("remediation", []),
            "anomaly_start_idx": anomaly_start_idx,
            "num_log_lines": len(log_text.split("\n")),
            "num_metric_points": len(metrics_df),
            "metrics": list(metrics_df.columns[1:]),  # Exclude timestamp
            "timestamp_start": str(metrics_df["timestamp"].iloc[0]),
            "timestamp_end": str(metrics_df["timestamp"].iloc[-1]),
        }

        label_path = self.labels_dir / f"{sample_id}.json"
        with open(label_path, "w", encoding="utf-8") as f:
            json.dump(label_data, f, indent=2, ensure_ascii=False, default=str)

        # 5) Metadata row
        row = {
            "sample_id": sample_id,
            "layer": layer_key,
            "layer_name": layer_name,
            "is_anomaly": int(is_anomaly),
            "scenario_id": meta.get("scenario_id", ""),
            "scenario_name": scenario_name or "",
            "root_cause_category": meta.get("root_cause_category", ""),
            "severity": severity or "",
            "log_path": f"logs/{sample_id}.txt",
            "metrics_path": f"metrics/{sample_id}.csv",
            "dashboard_path": f"dashboards/{sample_id}.png" if dashboard_path else "",
            "label_path": f"labels/{sample_id}.json",
        }

        return row

    def build_dataset(self, total_samples=100000, anomaly_ratio=0.50,
                      progress_interval=500):
        """
        Build the complete multimodal dataset.

        Args:
            total_samples: Total number of samples to generate
            anomaly_ratio: Fraction of anomalous samples (0.0 - 1.0)
            progress_interval: Print progress every N samples

        Returns:
            Path to metadata.csv
        """
        print(f"\n{'='*70}")
        print(f"  🚀 MULTIMODAL RCA DATASET GENERATOR")
        print(f"{'='*70}")
        print(f"  Total samples:    {total_samples:,}")
        print(f"  Anomaly ratio:    {anomaly_ratio*100:.0f}%")
        print(f"  Normal samples:   {int(total_samples * (1 - anomaly_ratio)):,}")
        print(f"  Anomaly samples:  {int(total_samples * anomaly_ratio):,}")
        print(f"  Dashboard mode:   {self.dashboard_mode}")
        print(f"  Output dir:       {self.output_dir}")
        print(f"{'='*70}\n")

        layers = self._get_all_layers()
        layer_keys = list(layers.keys())
        n_layers = len(layer_keys)

        n_anomaly = int(total_samples * anomaly_ratio)
        n_normal = total_samples - n_anomaly

        # Calculate per-layer distribution
        # Normal samples: evenly distributed across all layers
        normal_per_layer = n_normal // n_layers
        normal_remainder = n_normal % n_layers

        # Anomaly samples: evenly distributed across all scenarios
        all_scenarios = []
        for lk in layer_keys:
            for sc in layers[lk].get("scenarios", []):
                all_scenarios.append((lk, sc))

        n_scenarios = len(all_scenarios)
        anomaly_per_scenario = n_anomaly // n_scenarios
        anomaly_remainder = n_anomaly % n_scenarios

        print(f"  📊 Distribution:")
        print(f"     {n_layers} layers, {n_scenarios} anomaly scenarios")
        print(f"     ~{normal_per_layer} normal samples per layer")
        print(f"     ~{anomaly_per_scenario} anomaly samples per scenario")
        print()

        self.metadata_rows = []
        sample_counter = 0
        start_time = time.time()

        # ---- Generate Normal Samples ----
        print(f"  📝 Generating {n_normal:,} normal samples...")
        for li, layer_key in enumerate(layer_keys):
            layer_config = layers[layer_key]
            count = normal_per_layer + (1 if li < normal_remainder else 0)

            for i in range(count):
                sample_counter += 1
                sample_id = f"sample_{sample_counter:06d}"

                # Vary the seed per sample
                self.metric_gen.rng = np.random.default_rng(self.seed + sample_counter)

                row = self._generate_single_sample(
                    sample_id, layer_key, layer_config,
                    scenario_config=None
                )
                self.metadata_rows.append(row)

                if sample_counter % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = sample_counter / elapsed
                    eta = (total_samples - sample_counter) / rate
                    print(f"     [{sample_counter:>7,}/{total_samples:,}] "
                          f"{sample_counter/total_samples*100:5.1f}% | "
                          f"{rate:.1f} samples/s | ETA: {eta:.0f}s")

        # ---- Generate Anomaly Samples ----
        print(f"\n  🔴 Generating {n_anomaly:,} anomaly samples...")
        for si, (layer_key, scenario) in enumerate(all_scenarios):
            layer_config = layers[layer_key]
            count = anomaly_per_scenario + (1 if si < anomaly_remainder else 0)

            for i in range(count):
                sample_counter += 1
                sample_id = f"sample_{sample_counter:06d}"

                self.metric_gen.rng = np.random.default_rng(self.seed + sample_counter)

                row = self._generate_single_sample(
                    sample_id, layer_key, layer_config,
                    scenario_config=scenario
                )
                self.metadata_rows.append(row)

                if sample_counter % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = sample_counter / elapsed
                    eta = (total_samples - sample_counter) / rate
                    print(f"     [{sample_counter:>7,}/{total_samples:,}] "
                          f"{sample_counter/total_samples*100:5.1f}% | "
                          f"{rate:.1f} samples/s | ETA: {eta:.0f}s")

        # ---- Save Metadata CSV ----
        metadata_path = self.output_dir / "metadata.csv"
        with open(metadata_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.metadata_rows[0].keys())
            writer.writeheader()
            writer.writerows(self.metadata_rows)

        # ---- Summary ----
        elapsed = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"  ✅ DATASET GENERATION COMPLETE")
        print(f"{'='*70}")
        print(f"  Total samples generated: {sample_counter:,}")
        print(f"  Time elapsed:            {elapsed:.1f}s ({elapsed/60:.1f}min)")
        print(f"  Generation rate:         {sample_counter/elapsed:.1f} samples/s")
        print(f"  Metadata saved to:       {metadata_path}")
        print()

        # Print distribution summary
        self._print_distribution_summary()

        return metadata_path

    def _print_distribution_summary(self):
        """Print a summary of the dataset distribution."""
        df = pd.DataFrame(self.metadata_rows)

        print(f"  📊 Dataset Distribution:")
        print(f"  {'─'*50}")
        print(f"  {'Category':<30s} {'Count':>8s} {'%':>7s}")
        print(f"  {'─'*50}")

        # Normal vs Anomaly
        normal_count = len(df[df["is_anomaly"] == 0])
        anomaly_count = len(df[df["is_anomaly"] == 1])
        total = len(df)
        print(f"  {'Normal':<30s} {normal_count:>8,} {normal_count/total*100:>6.1f}%")
        print(f"  {'Anomaly':<30s} {anomaly_count:>8,} {anomaly_count/total*100:>6.1f}%")
        print(f"  {'─'*50}")

        # Per layer
        print(f"\n  Per-Layer Breakdown:")
        for layer in sorted(df["layer"].unique()):
            layer_df = df[df["layer"] == layer]
            n = len(layer_df)
            n_anom = len(layer_df[layer_df["is_anomaly"] == 1])
            print(f"  {layer:<25s} {n:>6,} total ({n_anom:>5,} anomaly)")

        # Per scenario
        anomaly_df = df[df["is_anomaly"] == 1]
        if not anomaly_df.empty:
            print(f"\n  Per-Scenario Breakdown:")
            for sid in sorted(anomaly_df["scenario_id"].unique()):
                sc_df = anomaly_df[anomaly_df["scenario_id"] == sid]
                name = sc_df["scenario_name"].iloc[0]
                print(f"  {sid:<12s} {name:<35s} {len(sc_df):>6,}")

        print(f"  {'─'*50}\n")

    def build_small_test(self, samples_per_class=5):
        """
        Generate a very small test dataset to verify everything works.

        Args:
            samples_per_class: Samples per normal layer + per anomaly scenario

        Returns:
            Path to metadata.csv
        """
        print(f"\n🧪 Generating SMALL TEST dataset ({samples_per_class} per class)...\n")

        layers = self._get_all_layers()
        self.metadata_rows = []
        sample_counter = 0

        for layer_key, layer_config in layers.items():
            # Normal samples
            for i in range(samples_per_class):
                sample_counter += 1
                sample_id = f"test_{sample_counter:05d}"
                self.metric_gen.rng = np.random.default_rng(self.seed + sample_counter)
                row = self._generate_single_sample(
                    sample_id, layer_key, layer_config, scenario_config=None
                )
                self.metadata_rows.append(row)

            # Anomaly samples
            for scenario in layer_config.get("scenarios", []):
                for i in range(samples_per_class):
                    sample_counter += 1
                    sample_id = f"test_{sample_counter:05d}"
                    self.metric_gen.rng = np.random.default_rng(self.seed + sample_counter)
                    row = self._generate_single_sample(
                        sample_id, layer_key, layer_config, scenario_config=scenario
                    )
                    self.metadata_rows.append(row)

        # Save metadata
        metadata_path = self.output_dir / "metadata.csv"
        with open(metadata_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.metadata_rows[0].keys())
            writer.writeheader()
            writer.writerows(self.metadata_rows)

        print(f"✅ Test dataset generated: {sample_counter} samples")
        print(f"   Metadata: {metadata_path}")
        self._print_distribution_summary()

        return metadata_path
