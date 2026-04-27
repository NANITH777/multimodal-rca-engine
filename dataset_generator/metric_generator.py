"""
Metric Generator Module for the Multimodal RCA Engine.
Generates realistic synthetic time-series metrics for all 11 infrastructure layers.
Based on anomaly scenarios defined in configs/anomaly_scenarios.yaml (from data.pdf).
"""

import numpy as np
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta


# ============================================
# Baseline Metric Profiles (Normal Operation)
# ============================================

METRIC_BASELINES = {
    # CDN
    "request_count":        {"mean": 1200, "std": 150, "min": 200, "max": 5000},
    "response_time_ms":     {"mean": 45, "std": 8, "min": 10, "max": 500},
    "cache_hit_ratio":      {"mean": 0.85, "std": 0.05, "min": 0.0, "max": 1.0},
    "traffic_mbps":         {"mean": 250, "std": 40, "min": 10, "max": 2000},
    "error_rate":           {"mean": 0.02, "std": 0.008, "min": 0.0, "max": 1.0},

    # Firewall
    "connection_attempts":  {"mean": 800, "std": 120, "min": 50, "max": 10000},
    "blocked_connections":  {"mean": 30, "std": 10, "min": 0, "max": 5000},
    "allowed_traffic_mbps": {"mean": 180, "std": 30, "min": 10, "max": 1000},
    "bandwidth_mbps":       {"mean": 200, "std": 35, "min": 10, "max": 2000},

    # Proxy
    "request_rate":         {"mean": 500, "std": 80, "min": 20, "max": 5000},
    "load_average":         {"mean": 1.5, "std": 0.5, "min": 0.1, "max": 20.0},
    "connection_count":     {"mean": 150, "std": 30, "min": 5, "max": 2000},

    # K8s Ingress
    "lb_utilization_pct":   {"mean": 45, "std": 10, "min": 0, "max": 100},
    "connection_health_pct":{"mean": 98, "std": 1.5, "min": 0, "max": 100},

    # K8s Deployments
    "pod_count":            {"mean": 8, "std": 2, "min": 1, "max": 50},
    "cpu_usage_pct":        {"mean": 35, "std": 10, "min": 1, "max": 100},
    "memory_usage_pct":     {"mean": 45, "std": 12, "min": 5, "max": 100},
    "pod_healthy_pct":      {"mean": 98, "std": 2, "min": 0, "max": 100},
    "restart_count":        {"mean": 0.5, "std": 0.8, "min": 0, "max": 50},

    # Application
    "qps":                  {"mean": 350, "std": 60, "min": 10, "max": 3000},
    "transaction_count":    {"mean": 2000, "std": 400, "min": 100, "max": 15000},
    "availability_pct":     {"mean": 99.5, "std": 0.3, "min": 80, "max": 100},

    # Database
    "query_response_ms":    {"mean": 15, "std": 5, "min": 1, "max": 500},
    "active_sessions":      {"mean": 25, "std": 8, "min": 1, "max": 500},
    "disk_io_mbps":         {"mean": 50, "std": 15, "min": 1, "max": 500},
    "slow_query_count":     {"mean": 3, "std": 2, "min": 0, "max": 200},

    # Storage
    "disk_usage_pct":       {"mean": 55, "std": 10, "min": 5, "max": 100},
    "iops":                 {"mean": 800, "std": 150, "min": 50, "max": 10000},
    "latency_ms":           {"mean": 5, "std": 2, "min": 0.5, "max": 200},
    "disk_error_count":     {"mean": 0.2, "std": 0.5, "min": 0, "max": 100},

    # Network
    "network_in_mbps":      {"mean": 120, "std": 25, "min": 5, "max": 1000},
    "network_out_mbps":     {"mean": 100, "std": 20, "min": 5, "max": 1000},
    "packet_loss_pct":      {"mean": 0.1, "std": 0.05, "min": 0.0, "max": 100},
    "connection_errors":    {"mean": 2, "std": 1.5, "min": 0, "max": 500},

    # Linux VM
    "process_count":        {"mean": 180, "std": 30, "min": 50, "max": 1000},
    "network_io_mbps":      {"mean": 80, "std": 20, "min": 1, "max": 500},

    # Linux Host
    "temperature_c":        {"mean": 55, "std": 5, "min": 25, "max": 105},
    "fan_speed_rpm":        {"mean": 2500, "std": 300, "min": 800, "max": 6000},
}


class MetricGenerator:
    """
    Generates realistic synthetic time-series metrics for cloud infrastructure monitoring.
    Supports normal baseline generation and anomaly injection based on scenario configs.
    """

    def __init__(self, window_size=60, resolution_sec=60, seed=None):
        """
        Args:
            window_size: Number of data points per window (default: 60 = 1 hour at 1min resolution)
            resolution_sec: Seconds between data points
            seed: Random seed for reproducibility
        """
        self.window_size = window_size
        self.resolution_sec = resolution_sec
        self.rng = np.random.default_rng(seed)

    # ============================================
    # Normal Baseline Generation
    # ============================================

    def _generate_base_signal(self, mean, std, size):
        """Generate a base signal with realistic temporal patterns.
        
        Includes natural micro-spikes and higher variance to create
        overlap with anomalous patterns and realistic difficulty.
        """
        # Higher variance base gaussian noise (realistic noisy systems)
        signal = self.rng.normal(mean, std * 1.5, size)

        # Add slow drift (diurnal-like pattern)
        t = np.linspace(0, 2 * np.pi, size)
        drift = mean * 0.12 * np.sin(t + self.rng.uniform(0, 2 * np.pi))
        signal += drift

        # Add micro-fluctuations (autocorrelation)
        for i in range(1, size):
            signal[i] = 0.6 * signal[i] + 0.4 * signal[i - 1]

        # Add random natural micro-spikes (normal systems have spikes too!)
        n_spikes = self.rng.integers(0, 4)  # 0-3 natural spikes per window
        for _ in range(n_spikes):
            spike_idx = self.rng.integers(0, size)
            spike_magnitude = self.rng.uniform(1.2, 2.0)
            spike_width = self.rng.integers(1, 4)
            for j in range(max(0, spike_idx - spike_width), min(size, spike_idx + spike_width)):
                dist = abs(j - spike_idx)
                signal[j] *= (1 + (spike_magnitude - 1) * np.exp(-0.5 * dist))

        return signal

    def generate_normal_metrics(self, metric_names, base_time=None):
        """
        Generate normal (non-anomalous) time-series for given metrics.

        Args:
            metric_names: List of metric names
            base_time: Starting timestamp (default: random recent time)

        Returns:
            pd.DataFrame with timestamp + metric columns
        """
        if base_time is None:
            days_ago = self.rng.integers(1, 30)
            hour = self.rng.integers(0, 24)
            base_time = datetime(2026, 3, 1) + timedelta(days=int(days_ago), hours=int(hour))

        timestamps = [base_time + timedelta(seconds=i * self.resolution_sec) for i in range(self.window_size)]
        data = {"timestamp": timestamps}

        for metric in metric_names:
            profile = METRIC_BASELINES.get(metric, {"mean": 50, "std": 10, "min": 0, "max": 100})
            signal = self._generate_base_signal(profile["mean"], profile["std"], self.window_size)
            signal = np.clip(signal, profile["min"], profile["max"])
            data[metric] = np.round(signal, 4)

        return pd.DataFrame(data)

    # ============================================
    # Anomaly Injection Patterns
    # ============================================

    def _inject_sudden_spike(self, signal, multiplier_range, inject_point=None):
        """Inject a sudden spike anomaly."""
        if inject_point is None:
            inject_point = self.rng.integers(self.window_size // 4, 3 * self.window_size // 4)

        multiplier = self.rng.uniform(*multiplier_range)
        spike_width = self.rng.integers(3, 12)

        start = max(0, inject_point - spike_width // 2)
        end = min(self.window_size, inject_point + spike_width // 2)

        # Create spike envelope
        for i in range(start, end):
            dist = abs(i - inject_point)
            decay = np.exp(-0.3 * dist)
            signal[i] *= (1 + (multiplier - 1) * decay)

        return signal, inject_point

    def _inject_gradual_rise(self, signal, multiplier_range):
        """Inject a gradual increase over time."""
        start_point = self.rng.integers(self.window_size // 6, self.window_size // 3)
        multiplier = self.rng.uniform(*multiplier_range)

        for i in range(start_point, self.window_size):
            progress = (i - start_point) / (self.window_size - start_point)
            signal[i] *= (1 + (multiplier - 1) * progress ** 1.5)

        return signal, start_point

    def _inject_gradual_drop(self, signal, multiplier_range):
        """Inject a gradual decrease (for metrics like cache_hit_ratio)."""
        start_point = self.rng.integers(self.window_size // 6, self.window_size // 3)
        multiplier = self.rng.uniform(*multiplier_range)

        for i in range(start_point, self.window_size):
            progress = (i - start_point) / (self.window_size - start_point)
            signal[i] *= (multiplier + (1 - multiplier) * (1 - progress ** 1.5))

        return signal, start_point

    def _inject_sudden_drop(self, signal, multiplier_range):
        """Inject a sudden drop."""
        inject_point = self.rng.integers(self.window_size // 4, 3 * self.window_size // 4)
        multiplier = self.rng.uniform(*multiplier_range)

        for i in range(inject_point, self.window_size):
            decay = np.exp(-0.1 * (i - inject_point))
            signal[i] *= (multiplier + (1 - multiplier) * decay)

        return signal, inject_point

    def _inject_sustained_high(self, signal, multiplier_range):
        """Inject sustained elevated values."""
        start_point = self.rng.integers(5, self.window_size // 4)
        multiplier = self.rng.uniform(*multiplier_range)

        # Quick ramp up, then sustained
        ramp_length = self.rng.integers(3, 8)
        for i in range(start_point, self.window_size):
            if i < start_point + ramp_length:
                progress = (i - start_point) / ramp_length
                signal[i] *= (1 + (multiplier - 1) * progress)
            else:
                signal[i] *= multiplier
                # Add some noise to sustained part
                signal[i] += self.rng.normal(0, signal[i] * 0.05)

        return signal, start_point

    def _inject_oscillation(self, signal, multiplier_range):
        """Inject oscillating pattern (unstable behavior)."""
        start_point = self.rng.integers(self.window_size // 6, self.window_size // 3)
        amplitude = self.rng.uniform(*multiplier_range)
        freq = self.rng.uniform(0.3, 0.8)

        for i in range(start_point, self.window_size):
            phase = (i - start_point) * freq
            signal[i] *= (1 + (amplitude - 1) * abs(np.sin(phase)))

        return signal, start_point

    def _inject_step_increase(self, signal, additive_range):
        """Inject a step increase (e.g., restart count jumping)."""
        step_point = self.rng.integers(self.window_size // 4, 3 * self.window_size // 4)
        step_value = self.rng.uniform(*additive_range)

        for i in range(step_point, self.window_size):
            signal[i] += step_value

        return signal, step_point

    def _inject_additive_anomaly(self, signal, additive_range, pattern):
        """Inject additive anomaly (for metrics like error_rate where multiplicative doesn't make sense)."""
        add_value = self.rng.uniform(*additive_range)

        if pattern == "sudden_spike":
            inject_point = self.rng.integers(self.window_size // 4, 3 * self.window_size // 4)
            spike_width = self.rng.integers(3, 12)
            start = max(0, inject_point - spike_width // 2)
            end = min(self.window_size, inject_point + spike_width // 2)
            for i in range(start, end):
                dist = abs(i - inject_point)
                signal[i] += add_value * np.exp(-0.3 * dist)
            return signal, inject_point

        elif pattern == "gradual_rise":
            start_point = self.rng.integers(self.window_size // 6, self.window_size // 3)
            for i in range(start_point, self.window_size):
                progress = (i - start_point) / (self.window_size - start_point)
                signal[i] += add_value * progress ** 1.5
            return signal, start_point

        elif pattern == "sustained_high":
            start_point = self.rng.integers(5, self.window_size // 4)
            for i in range(start_point, self.window_size):
                signal[i] += add_value + self.rng.normal(0, add_value * 0.1)
            return signal, start_point

        elif pattern == "step_increase":
            return self._inject_step_increase(signal, additive_range)

        return signal, 0

    # ============================================
    # Main Anomaly Generation
    # ============================================

    PATTERN_INJECTORS = {
        "sudden_spike": "_inject_sudden_spike",
        "gradual_rise": "_inject_gradual_rise",
        "gradual_drop": "_inject_gradual_drop",
        "sudden_drop": "_inject_sudden_drop",
        "sustained_high": "_inject_sustained_high",
        "oscillation": "_inject_oscillation",
        "step_increase": "_inject_step_increase",
    }

    def generate_anomaly_metrics(self, metric_names, scenario_config, base_time=None):
        """
        Generate anomalous time-series based on a scenario configuration.

        Args:
            metric_names: List of all metric names for this layer
            scenario_config: Scenario dict from anomaly_scenarios.yaml
            base_time: Starting timestamp

        Returns:
            tuple: (pd.DataFrame, anomaly_start_index)
        """
        # Start with normal baseline
        df = self.generate_normal_metrics(metric_names, base_time)
        affected = scenario_config.get("affected_metrics", {})
        anomaly_start = self.window_size  # Will be updated

        for metric_name, config in affected.items():
            if metric_name not in df.columns:
                continue

            signal = df[metric_name].values.copy()
            pattern = config.get("pattern", "gradual_rise")

            if "additive" in config:
                signal, start = self._inject_additive_anomaly(
                    signal, config["additive"], pattern
                )
            elif "multiplier" in config:
                injector_name = self.PATTERN_INJECTORS.get(pattern)
                if injector_name:
                    injector = getattr(self, injector_name)
                    signal, start = injector(signal, config["multiplier"])
                else:
                    signal, start = self._inject_gradual_rise(signal, config["multiplier"])
            else:
                continue

            # Clip to valid range
            profile = METRIC_BASELINES.get(metric_name, {"min": 0, "max": 99999})
            signal = np.clip(signal, profile["min"], profile["max"])
            df[metric_name] = np.round(signal, 4)

            anomaly_start = min(anomaly_start, start)

        return df, anomaly_start

    def generate_sample(self, layer_config, scenario_config=None, base_time=None):
        """
        Generate a complete metric sample (normal or anomalous).

        Args:
            layer_config: Layer dict from anomaly_scenarios.yaml
            scenario_config: If None, generates normal; otherwise injects anomaly
            base_time: Starting timestamp

        Returns:
            tuple: (pd.DataFrame, dict with metadata)
        """
        metric_names = layer_config["metrics"]

        if scenario_config is None:
            df = self.generate_normal_metrics(metric_names, base_time)
            meta = {
                "is_anomaly": False,
                "anomaly_start_idx": None,
                "scenario_id": None,
            }
        else:
            df, anomaly_start = self.generate_anomaly_metrics(
                metric_names, scenario_config, base_time
            )
            meta = {
                "is_anomaly": True,
                "anomaly_start_idx": int(anomaly_start),
                "scenario_id": scenario_config["id"],
                "root_cause": scenario_config["root_cause"],
                "root_cause_category": scenario_config["root_cause_category"],
                "severity": scenario_config["severity"],
                "remediation": scenario_config["remediation"],
            }

        return df, meta


def load_scenarios(config_path=None):
    """Load anomaly scenarios from YAML config."""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "configs" / "anomaly_scenarios.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
