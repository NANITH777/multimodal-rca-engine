"""
Data Loader Module — Loads the multimodal RCA dataset for training/evaluation.
Handles train/val/test splitting and feature extraction from all modalities.
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer


class MultimodalDataLoader:
    """
    Loads and preprocesses the multimodal RCA dataset.
    Extracts features from logs (TF-IDF), metrics (statistical), and labels.
    
    Applies realistic noise injection to simulate real-world conditions:
    - Gaussian noise on metric features (sensor/measurement imprecision)
    - Random perturbation on log counts (logging inconsistencies)
    - Feature dropout (missing/unavailable data)
    """

    def __init__(self, dataset_dir=None, noise_level=0.25, feature_dropout=0.08, seed=42):
        if dataset_dir is None:
            dataset_dir = Path(__file__).parent.parent / "data" / "multimodal_dataset"
        self.dataset_dir = Path(dataset_dir)
        self.metadata = None
        self.label_encoders = {}
        self.scaler = StandardScaler()
        self.tfidf = TfidfVectorizer(max_features=100, stop_words='english',
                                      token_pattern=r'(?u)\b[A-Za-z_][A-Za-z0-9_]{2,}\b')
        # Noise parameters for realistic simulation
        self.noise_level = noise_level       # Gaussian noise std multiplier
        self.feature_dropout = feature_dropout  # Probability of zeroing a feature
        self.rng = np.random.RandomState(seed)

    def load_metadata(self):
        """Load the metadata CSV index."""
        metadata_path = self.dataset_dir / "metadata.csv"
        self.metadata = pd.read_csv(metadata_path)
        print(f" Loaded metadata: {len(self.metadata):,} samples")
        print(f"   Normal:  {(self.metadata['is_anomaly']==0).sum():,}")
        print(f"   Anomaly: {(self.metadata['is_anomaly']==1).sum():,}")
        return self.metadata

    # ============================================
    # Feature Extraction: Metrics
    # ============================================

    def extract_metric_features(self, sample_id):
        """
        Extract statistical features from a single metrics CSV.
        
        IMPORTANT: Features use POSITIONAL names (metric_0, metric_1...) 
        instead of actual metric names. This prevents the model from 
        identifying the layer just by knowing which metrics are present.
        The model must learn from the actual statistical patterns.
        """
        metrics_path = self.dataset_dir / "metrics" / f"{sample_id}.csv"
        try:
            df = pd.read_csv(metrics_path)
        except Exception:
            return None

        features = {}
        metric_cols = [c for c in df.columns if c != 'timestamp']

        # Use positional names: metric_0, metric_1, ... (max 8 metrics per layer)
        for pos, col in enumerate(metric_cols):
            values = df[col].values.astype(float)
            n = len(values)
            prefix = f"m{pos}"

            # Normalize to [0,1] range so all layers are comparable
            vmin, vmax = np.min(values), np.max(values)
            if vmax - vmin > 1e-8:
                values_norm = (values - vmin) / (vmax - vmin)
            else:
                values_norm = np.zeros_like(values)

            # Statistical features on normalized values
            features[f"{prefix}_mean"] = np.mean(values_norm)
            features[f"{prefix}_std"] = np.std(values_norm)
            features[f"{prefix}_min"] = np.min(values_norm)
            features[f"{prefix}_max"] = np.max(values_norm)
            features[f"{prefix}_range"] = np.max(values_norm) - np.min(values_norm)
            features[f"{prefix}_median"] = np.median(values_norm)
            features[f"{prefix}_skew"] = float(pd.Series(values_norm).skew())
            features[f"{prefix}_kurtosis"] = float(pd.Series(values_norm).kurtosis())

            # Trend (slope)
            x = np.arange(n)
            if np.std(values_norm) > 0:
                slope = np.polyfit(x, values_norm, 1)[0]
            else:
                slope = 0.0
            features[f"{prefix}_slope"] = slope

            # Spike score
            if n > 5:
                rolling_mean = np.convolve(values_norm, np.ones(5)/5, mode='valid')
                padded = np.concatenate([values_norm[:2], rolling_mean, values_norm[-2:]])
                deviations = np.abs(values_norm - padded[:n])
                features[f"{prefix}_spike"] = np.max(deviations) / (np.std(values_norm) + 1e-6)
            else:
                features[f"{prefix}_spike"] = 0.0

            # Change point score
            half = n // 2
            first_half = np.mean(values_norm[:half])
            second_half = np.mean(values_norm[half:])
            features[f"{prefix}_change"] = (second_half - first_half) / (np.std(values_norm) + 1e-6)

            # Volatility
            diffs = np.diff(values_norm)
            features[f"{prefix}_volatility"] = np.std(diffs) if len(diffs) > 0 else 0.0

            # Autocorrelation at lag 1
            if n > 2 and np.std(values_norm) > 0:
                features[f"{prefix}_autocorr"] = np.corrcoef(values_norm[:-1], values_norm[1:])[0, 1]
            else:
                features[f"{prefix}_autocorr"] = 0.0

        # Pad missing metrics (some layers have 4, others 6)
        for pos in range(len(metric_cols), 8):
            prefix = f"m{pos}"
            for stat in ["mean","std","min","max","range","median","skew","kurtosis",
                         "slope","spike","change","volatility","autocorr"]:
                features[f"{prefix}_{stat}"] = 0.0

        return features

    # ============================================
    # Feature Extraction: Logs
    # ============================================

    def extract_log_features_simple(self, sample_id):
        """
        Extract simple features from a log file with realistic noise.
        Simulates real-world logging inconsistencies:
        - Random line count perturbation (logs can be truncated/duplicated)
        - Noise on level counts (some parsers misclassify levels)
        """
        log_path = self.dataset_dir / "logs" / f"{sample_id}.txt"
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception:
            return None

        total = len(lines)
        info_count = sum(1 for l in lines if '[INFO' in l)
        warn_count = sum(1 for l in lines if '[WARN' in l)
        error_count = sum(1 for l in lines if '[ERROR' in l)
        critical_count = sum(1 for l in lines if '[CRITICAL' in l)

        # Add realistic noise: log parsers aren't perfect
        # Some lines get misclassified between adjacent levels
        noise_lines = max(1, int(total * 0.12))  # ~12% noise
        info_count += self.rng.randint(-noise_lines, noise_lines + 1)
        warn_count += self.rng.randint(-noise_lines // 2, noise_lines // 2 + 1)
        error_count += self.rng.randint(-noise_lines // 3, noise_lines // 3 + 1)
        critical_count += self.rng.randint(-noise_lines // 4, noise_lines // 4 + 1)

        # Ensure non-negative
        info_count = max(0, info_count)
        warn_count = max(0, warn_count)
        error_count = max(0, error_count)
        critical_count = max(0, critical_count)
        noisy_total = max(1, info_count + warn_count + error_count + critical_count)

        return {
            "log_total_lines": total,
            "log_info_count": info_count,
            "log_warn_count": warn_count,
            "log_error_count": error_count,
            "log_critical_count": critical_count,
            "log_error_ratio": (error_count + critical_count) / noisy_total,
            "log_warn_ratio": warn_count / noisy_total,
            "log_critical_ratio": critical_count / noisy_total,
            "log_severity_score": (warn_count * 1 + error_count * 2 + critical_count * 3) / noisy_total,
        }

    def extract_log_text(self, sample_id):
        """Read raw log text for TF-IDF processing."""
        log_path = self.dataset_dir / "logs" / f"{sample_id}.txt"
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""

    # ============================================
    # Feature Extraction: Labels
    # ============================================

    def load_label(self, sample_id):
        """Load a single label JSON file."""
        label_path = self.dataset_dir / "labels" / f"{sample_id}.json"
        try:
            with open(label_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    # ============================================
    # Full Dataset Loading
    # ============================================

    def build_feature_matrix(self, max_samples=None, include_tfidf=True):
        """
        Build the complete feature matrix from all modalities.

        Args:
            max_samples: Limit number of samples (None = all)
            include_tfidf: Whether to include TF-IDF features from logs

        Returns:
            dict with keys: X_metrics, X_logs, X_tfidf, y_anomaly,
                           y_root_cause, y_severity, y_layer, sample_ids
        """
        if self.metadata is None:
            self.load_metadata()

        df = self.metadata
        if max_samples:
            df = df.head(max_samples)

        print(f"\n Building feature matrix for {len(df):,} samples...")

        metric_features_list = []
        log_features_list = []
        log_texts = []
        labels = {
            "y_anomaly": [],
            "y_root_cause": [],
            "y_severity": [],
            "y_layer": [],
            "y_scenario": [],
        }
        valid_ids = []
        skipped = 0

        for idx, row in df.iterrows():
            sid = row["sample_id"]

            # Extract metric features
            mf = self.extract_metric_features(sid)
            if mf is None:
                skipped += 1
                continue

            # Extract log features
            lf = self.extract_log_features_simple(sid)
            if lf is None:
                skipped += 1
                continue

            metric_features_list.append(mf)
            log_features_list.append(lf)
            valid_ids.append(sid)

            if include_tfidf:
                log_texts.append(self.extract_log_text(sid))

            # Labels
            labels["y_anomaly"].append(int(row["is_anomaly"]))
            labels["y_layer"].append(row["layer"])

            rc = row.get("root_cause_category", "none")
            labels["y_root_cause"].append(rc if pd.notna(rc) and rc != "" else "none")

            sev = row.get("severity", "none")
            labels["y_severity"].append(sev if pd.notna(sev) and sev != "" else "none")

            sc = row.get("scenario_id", "none")
            labels["y_scenario"].append(sc if pd.notna(sc) and sc != "" else "none")

            if (idx + 1) % 2000 == 0:
                print(f"   Processed {idx+1:,}/{len(df):,} samples...")

        print(f"    Extracted features for {len(valid_ids):,} samples (skipped {skipped})")

        # Convert to DataFrames/arrays
        X_metrics = pd.DataFrame(metric_features_list).fillna(0)
        X_logs = pd.DataFrame(log_features_list).fillna(0)

        # Apply Gaussian noise to metric features (measurement imprecision)
        print(f"    Applying noise (level={self.noise_level}, dropout={self.feature_dropout})...")
        metric_noise = self.rng.normal(0, self.noise_level, X_metrics.shape)
        metric_stds = X_metrics.std().values
        metric_stds[metric_stds == 0] = 1.0
        X_metrics = X_metrics + metric_noise * metric_stds

        # Feature dropout: randomly zero out features (missing sensors)
        dropout_mask = self.rng.random(X_metrics.shape) < self.feature_dropout
        X_metrics.values[dropout_mask] = 0.0

        # TF-IDF
        X_tfidf = None
        if include_tfidf and log_texts:
            print(f"    Building TF-IDF matrix...")
            X_tfidf = self.tfidf.fit_transform(log_texts)
            print(f"      TF-IDF shape: {X_tfidf.shape}")

        # Encode categorical labels
        for key in ["y_root_cause", "y_severity", "y_layer", "y_scenario"]:
            le = LabelEncoder()
            labels[key] = le.fit_transform(labels[key])
            self.label_encoders[key] = le
            print(f"    {key}: {len(le.classes_)} classes → {list(le.classes_)}")

        labels["y_anomaly"] = np.array(labels["y_anomaly"])

        result = {
            "X_metrics": X_metrics,
            "X_logs": X_logs,
            "X_tfidf": X_tfidf,
            "sample_ids": valid_ids,
            **labels,
        }

        print(f"\n Feature Matrix Summary:")
        print(f"   Metric features: {X_metrics.shape}")
        print(f"   Log features:    {X_logs.shape}")
        if X_tfidf is not None:
            print(f"   TF-IDF features: {X_tfidf.shape}")
        print(f"   Total samples:   {len(valid_ids):,}")

        return result

    def split_data(self, data, test_size=0.2, val_size=0.1, random_state=42):
        """
        Split data into train / validation / test sets.

        Returns:
            dict with train/val/test splits for all features and labels
        """
        n = len(data["sample_ids"])
        indices = np.arange(n)

        # First split: train+val vs test
        train_val_idx, test_idx = train_test_split(
            indices, test_size=test_size, random_state=random_state,
            stratify=data["y_anomaly"]
        )

        # Second split: train vs val
        val_frac = val_size / (1 - test_size)
        train_idx, val_idx = train_test_split(
            train_val_idx, test_size=val_frac, random_state=random_state,
            stratify=data["y_anomaly"][train_val_idx]
        )

        splits = {}
        for split_name, idx in [("train", train_idx), ("val", val_idx), ("test", test_idx)]:
            split = {}
            split["X_metrics"] = data["X_metrics"].iloc[idx].reset_index(drop=True)
            split["X_logs"] = data["X_logs"].iloc[idx].reset_index(drop=True)
            if data["X_tfidf"] is not None:
                split["X_tfidf"] = data["X_tfidf"][idx]
            split["y_anomaly"] = data["y_anomaly"][idx]
            split["y_root_cause"] = data["y_root_cause"][idx]
            split["y_severity"] = data["y_severity"][idx]
            split["y_layer"] = data["y_layer"][idx]
            split["y_scenario"] = data["y_scenario"][idx]
            split["sample_ids"] = [data["sample_ids"][i] for i in idx]
            splits[split_name] = split

        print(f"\n Data Split:")
        print(f"   Train: {len(train_idx):,} ({len(train_idx)/n*100:.0f}%)")
        print(f"   Val:   {len(val_idx):,} ({len(val_idx)/n*100:.0f}%)")
        print(f"   Test:  {len(test_idx):,} ({len(test_idx)/n*100:.0f}%)")

        return splits
