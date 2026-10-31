"""
Anomaly Detection & RCA Classification Models.
Implements multiple ML models for the Multimodal RCA pipeline.
"""

import numpy as np
import pandas as pd
import json
import time
from pathlib import Path
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier,
    IsolationForest,
)
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    accuracy_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler
from scipy.sparse import issparse, hstack

try:
    from xgboost import XGBClassifier
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False


class AnomalyDetector:
    """
    Binary classifier: Normal (0) vs Anomaly (1).
    Combines metric features + log features + TF-IDF for detection.
    """

    def __init__(self, model_type="random_forest"):
        self.model_type = model_type
        self.model = self._create_model(model_type)
        self.scaler = StandardScaler()
        self.feature_names = None
        self.results = {}

    def _create_model(self, model_type):
        if model_type == "random_forest":
            return RandomForestClassifier(
                n_estimators=200, max_depth=20, min_samples_split=5,
                class_weight="balanced", random_state=42, n_jobs=-1
            )
        elif model_type == "xgboost" and HAS_XGBOOST:
            return XGBClassifier(
                n_estimators=200, max_depth=8, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8,
                eval_metric="logloss", random_state=42, n_jobs=-1
            )
        elif model_type == "gradient_boosting":
            return GradientBoostingClassifier(
                n_estimators=150, max_depth=6, learning_rate=0.1,
                subsample=0.8, random_state=42
            )
        elif model_type == "logistic_regression":
            return LogisticRegression(
                max_iter=1000, class_weight="balanced", random_state=42
            )
        else:
            return RandomForestClassifier(
                n_estimators=200, max_depth=20, random_state=42, n_jobs=-1
            )

    def _combine_features(self, split, use_tfidf=True):
        """Combine metric + log + TF-IDF features into a single matrix."""
        X_metric = split["X_metrics"].values
        X_log = split["X_logs"].values
        X_dense = np.hstack([X_metric, X_log])

        if use_tfidf and "X_tfidf" in split and split["X_tfidf"] is not None:
            from scipy.sparse import csr_matrix
            X_dense_sparse = csr_matrix(X_dense)
            X = hstack([X_dense_sparse, split["X_tfidf"]])
        else:
            X = X_dense

        return X

    def train(self, train_split, val_split=None, use_tfidf=True):
        """
        Train the anomaly detection model.

        Args:
            train_split: Training data dict
            val_split: Validation data dict (for evaluation)
            use_tfidf: Include TF-IDF features
        """
        print(f"\n{'='*60}")
        print(f"  Training Anomaly Detector ({self.model_type})")
        print(f"{'='*60}")

        X_train = self._combine_features(train_split, use_tfidf)
        y_train = train_split["y_anomaly"]

        # Scale dense features
        if issparse(X_train):
            X_train_dense = X_train.toarray()
        else:
            X_train_dense = X_train

        print(f"  Features: {X_train.shape[1]} | Samples: {X_train.shape[0]:,}")
        print(f"  Labels: {np.bincount(y_train)}")

        start = time.time()
        self.model.fit(X_train_dense if not issparse(X_train) else X_train, y_train)
        train_time = time.time() - start
        print(f"  Training time: {train_time:.1f}s")

        # Evaluate on validation
        if val_split:
            self.evaluate(val_split, "Validation", use_tfidf)

    def predict(self, split, use_tfidf=True):
        """Predict anomaly labels."""
        X = self._combine_features(split, use_tfidf)
        if issparse(X):
            return self.model.predict(X)
        return self.model.predict(X)

    def predict_proba(self, split, use_tfidf=True):
        """Predict anomaly probabilities."""
        X = self._combine_features(split, use_tfidf)
        if hasattr(self.model, 'predict_proba'):
            return self.model.predict_proba(X)[:, 1]
        return self.predict(split, use_tfidf).astype(float)

    def evaluate(self, split, split_name="Test", use_tfidf=True):
        """Evaluate the model on a data split."""
        y_true = split["y_anomaly"]
        y_pred = self.predict(split, use_tfidf)

        acc = accuracy_score(y_true, y_pred)
        prec = precision_score(y_true, y_pred, zero_division=0)
        rec = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)

        try:
            y_prob = self.predict_proba(split, use_tfidf)
            auc = roc_auc_score(y_true, y_prob)
        except Exception:
            auc = 0.0

        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])

        results = {
            "accuracy": acc, "precision": prec, "recall": rec,
            "f1": f1, "auc_roc": auc, "confusion_matrix": cm.tolist()
        }
        self.results[split_name] = results

        print(f"\n   {split_name} Results — Anomaly Detection ({self.model_type}):")
        print(f"  {'─'*45}")
        print(f"  {'Accuracy':<20s} {acc:.4f}")
        print(f"  {'Precision':<20s} {prec:.4f}")
        print(f"  {'Recall':<20s} {rec:.4f}")
        print(f"  {'F1 Score':<20s} {f1:.4f}")
        print(f"  {'AUC-ROC':<20s} {auc:.4f}")
        print(f"  {'─'*45}")
        print(f"  Confusion Matrix:")
        print(f"             Pred Normal  Pred Anomaly")
        if cm.shape[0] >= 2:
            print(f"  Actual N   {cm[0][0]:>8}     {cm[0][1]:>8}")
            print(f"  Actual A   {cm[1][0]:>8}     {cm[1][1]:>8}")
        else:
            print(f"  {cm}")

        return results

    def get_feature_importance(self, feature_names=None):
        """Get feature importances (for tree-based models)."""
        if hasattr(self.model, 'feature_importances_'):
            importances = self.model.feature_importances_
            if feature_names is None:
                feature_names = [f"feature_{i}" for i in range(len(importances))]
            fi = pd.DataFrame({
                "feature": feature_names[:len(importances)],
                "importance": importances
            }).sort_values("importance", ascending=False)
            return fi
        return None


class RCAClassifier:
    """
    Multi-class classifier for Root Cause Analysis.
    Predicts: root_cause_category, severity, layer, and scenario.
    """

    def __init__(self, model_type="random_forest"):
        self.model_type = model_type
        self.models = {}
        self.results = {}
        self.targets = {
            "root_cause": "Root Cause Category",
            "severity": "Severity Level",
            "layer": "Infrastructure Layer",
            "scenario": "Anomaly Scenario",
        }

    def _create_model(self):
        if self.model_type == "random_forest":
            return RandomForestClassifier(
                n_estimators=200, max_depth=20, class_weight="balanced",
                random_state=42, n_jobs=-1
            )
        elif self.model_type == "xgboost" and HAS_XGBOOST:
            return XGBClassifier(
                n_estimators=200, max_depth=8, learning_rate=0.1,
                random_state=42, n_jobs=-1
            )
        elif self.model_type == "gradient_boosting":
            return GradientBoostingClassifier(
                n_estimators=150, max_depth=6, random_state=42
            )
        else:
            return RandomForestClassifier(
                n_estimators=200, random_state=42, n_jobs=-1
            )

    def _combine_features(self, split):
        """Combine metric + log features."""
        X_metric = split["X_metrics"].values
        X_log = split["X_logs"].values
        return np.hstack([X_metric, X_log])

    def train(self, train_split, val_split=None, targets=None):
        """
        Train multi-class classifiers for RCA.

        Args:
            train_split: Training data
            val_split: Validation data
            targets: List of target names to train (default: all)
        """
        if targets is None:
            targets = list(self.targets.keys())

        X_train = self._combine_features(train_split)

        # Filter to anomaly-only samples for RCA tasks
        anomaly_mask = train_split["y_anomaly"] == 1
        X_train_anom = X_train[anomaly_mask]

        print(f"\n{'='*60}")
        print(f"  Training RCA Classifiers ({self.model_type})")
        print(f"  Anomaly samples for RCA: {X_train_anom.shape[0]:,}")
        print(f"{'='*60}")

        for target in targets:
            y_key = f"y_{target}"
            if y_key not in train_split:
                continue

            y_train = train_split[y_key][anomaly_mask]
            n_classes = len(np.unique(y_train))

            if n_classes < 2:
                print(f"\n   Skipping {target}: only {n_classes} class(es)")
                continue

            print(f"\n   Training: {self.targets[target]} ({n_classes} classes)")

            model = self._create_model()
            start = time.time()
            model.fit(X_train_anom, y_train)
            elapsed = time.time() - start

            self.models[target] = model
            print(f"     Trained in {elapsed:.1f}s")

            # Evaluate on validation
            if val_split:
                self.evaluate(val_split, target, f"Val-{target}")

    def predict(self, split, target):
        """Predict for a specific target."""
        X = self._combine_features(split)
        if target in self.models:
            return self.models[target].predict(X)
        return None

    def evaluate(self, split, target, split_name="Test"):
        """Evaluate a specific RCA classifier."""
        y_key = f"y_{target}"
        if y_key not in split or target not in self.models:
            return None

        # Filter to anomaly samples
        anomaly_mask = split["y_anomaly"] == 1
        X = self._combine_features(split)[anomaly_mask]
        y_true = split[y_key][anomaly_mask]

        if len(y_true) == 0:
            return None

        y_pred = self.models[target].predict(X)

        acc = accuracy_score(y_true, y_pred)
        f1_macro = f1_score(y_true, y_pred, average="macro", zero_division=0)
        f1_weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)

        results = {
            "accuracy": acc,
            "f1_macro": f1_macro,
            "f1_weighted": f1_weighted,
        }
        self.results[split_name] = results

        print(f"\n   {split_name} — {self.targets.get(target, target)}:")
        print(f"  {'─'*40}")
        print(f"  {'Accuracy':<20s} {acc:.4f}")
        print(f"  {'F1 (macro)':<20s} {f1_macro:.4f}")
        print(f"  {'F1 (weighted)':<20s} {f1_weighted:.4f}")

        return results

    def evaluate_all(self, split, split_name="Test"):
        """Evaluate all trained classifiers."""
        all_results = {}
        for target in self.models:
            r = self.evaluate(split, target, f"{split_name}-{target}")
            if r:
                all_results[target] = r
        return all_results


class RemediationEngine:
    """
    Recommends remediation actions based on predicted anomaly type.
    Uses the scenario definitions from anomaly_scenarios.yaml.
    """

    def __init__(self, scenarios_path=None):
        import yaml
        if scenarios_path is None:
            scenarios_path = Path(__file__).parent.parent / "configs" / "anomaly_scenarios.yaml"

        with open(scenarios_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Build lookup: scenario_id -> remediation actions
        self.remediation_map = {}
        self.scenario_info = {}
        for layer_key, layer_config in config.get("layers", {}).items():
            for scenario in layer_config.get("scenarios", []):
                sid = scenario["id"]
                self.remediation_map[sid] = scenario.get("remediation", [])
                self.scenario_info[sid] = {
                    "name": scenario["name"],
                    "layer": layer_key,
                    "root_cause": scenario.get("root_cause", ""),
                    "severity": scenario.get("severity", ""),
                }

    def get_remediation(self, scenario_id):
        """Get remediation actions for a predicted scenario."""
        return {
            "scenario_id": scenario_id,
            "info": self.scenario_info.get(scenario_id, {}),
            "actions": self.remediation_map.get(scenario_id, []),
        }

    def predict_remediation(self, rca_classifier, split, label_encoder):
        """
        Predict remediation actions for the entire split.

        Args:
            rca_classifier: Trained RCAClassifier
            split: Data split
            label_encoder: LabelEncoder for scenario IDs

        Returns:
            List of remediation dicts
        """
        if "scenario" not in rca_classifier.models:
            return []

        predictions = rca_classifier.predict(split, "scenario")
        scenario_ids = label_encoder.inverse_transform(predictions)

        remediations = []
        for sid in scenario_ids:
            remediations.append(self.get_remediation(sid))

        return remediations
