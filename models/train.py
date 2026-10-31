"""
Training Pipeline — Train and evaluate all RCA models.

Usage:
    python models/train.py
    python models/train.py --max-samples 5000
    python models/train.py --model xgboost
"""

import sys
import json
import time
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.data_loader import MultimodalDataLoader
from models.rca_models import AnomalyDetector, RCAClassifier, RemediationEngine


def train_full_pipeline(max_samples=None, model_type="random_forest", save_results=True):
    """
    Run the complete training and evaluation pipeline.

    Steps:
        1. Load multimodal dataset
        2. Extract features from all modalities
        3. Split into train/val/test
        4. Train Anomaly Detector
        5. Train RCA multi-class classifiers
        6. Evaluate all models
        7. Demo remediation engine
        8. Save results
    """
    print("\n" + "=" * 70)
    print("  MULTIMODAL RCA ENGINE — TRAINING PIPELINE")
    print("=" * 70)
    total_start = time.time()

    # ──────────────────────────────────────────
    # Step 1: Load Dataset
    # ──────────────────────────────────────────
    print("\n[1/7] Loading dataset...")
    loader = MultimodalDataLoader()
    loader.load_metadata()

    # ──────────────────────────────────────────
    # Step 2: Extract Features
    # ──────────────────────────────────────────
    print("\n[2/7] Extracting features from all modalities...")
    data = loader.build_feature_matrix(max_samples=max_samples, include_tfidf=True)

    # ──────────────────────────────────────────
    # Step 3: Split Data
    # ──────────────────────────────────────────
    print("\n[3/7] Splitting data (70% train / 10% val / 20% test)...")
    splits = loader.split_data(data, test_size=0.2, val_size=0.1)

    # ──────────────────────────────────────────
    # Step 4: Train Anomaly Detector
    # ──────────────────────────────────────────
    print("\n[4/7] Training Anomaly Detector...")
    detector = AnomalyDetector(model_type=model_type)
    detector.train(splits["train"], splits["val"], use_tfidf=True)

    # Final test evaluation
    print("\n" + "-" * 60)
    print("  FINAL TEST EVALUATION — Anomaly Detection")
    print("-" * 60)
    anomaly_results = detector.evaluate(splits["test"], "TEST", use_tfidf=True)

    # Feature importance (dense features only)
    metric_names = list(data["X_metrics"].columns) + list(data["X_logs"].columns)
    tfidf_names = [f"tfidf_{i}" for i in range(300)]
    all_names = metric_names + tfidf_names
    fi = detector.get_feature_importance(all_names)
    if fi is not None:
        print(f"\n  Top 15 Most Important Features:")
        print(f"  {'─'*45}")
        for _, row in fi.head(15).iterrows():
            bar = "█" * int(row["importance"] * 100)
            print(f"  {row['feature']:<35s} {row['importance']:.4f} {bar}")

    # ──────────────────────────────────────────
    # Step 5: Train RCA Classifiers
    # ──────────────────────────────────────────
    print("\n[5/7] Training RCA Classifiers...")
    rca = RCAClassifier(model_type=model_type)
    rca.train(splits["train"], splits["val"],
              targets=["root_cause", "severity", "layer", "scenario"])

    # Final test evaluation for RCA
    print("\n" + "-" * 60)
    print("  FINAL TEST EVALUATION — RCA Classification")
    print("-" * 60)
    rca_results = rca.evaluate_all(splits["test"], "TEST")

    # ──────────────────────────────────────────
    # Step 6: Remediation Engine Demo
    # ──────────────────────────────────────────
    print("\n[6/7] Remediation Engine Demo...")
    remediation = RemediationEngine()

    if "scenario" in rca.models and "y_scenario" in loader.label_encoders:
        le_scenario = loader.label_encoders["y_scenario"]

        # Get predictions on test anomalies
        test_anomaly_mask = splits["test"]["y_anomaly"] == 1
        X_test_anom = np.hstack([
            splits["test"]["X_metrics"].values[test_anomaly_mask],
            splits["test"]["X_logs"].values[test_anomaly_mask]
        ])

        # Predict scenarios
        pred_scenarios = rca.models["scenario"].predict(X_test_anom)
        pred_scenario_ids = le_scenario.inverse_transform(pred_scenarios)

        # Show 5 example remediations
        print(f"\n  Example Remediation Predictions (5 samples):")
        print(f"  {'─'*60}")
        for i in range(min(5, len(pred_scenario_ids))):
            sid = pred_scenario_ids[i]
            rem = remediation.get_remediation(sid)
            info = rem.get("info", {})
            print(f"\n  Sample {i+1}:")
            print(f"    Predicted Scenario: {sid} — {info.get('name', '?')}")
            print(f"    Layer: {info.get('layer', '?')}")
            print(f"    Root Cause: {info.get('root_cause', '?')}")
            print(f"    Severity: {info.get('severity', '?')}")
            print(f"    Remediation Actions:")
            for action in rem.get("actions", []):
                print(f"      -> {action}")

    # ──────────────────────────────────────────
    # Step 7: Save Results
    # ──────────────────────────────────────────
    if save_results:
        print("\n[7/7] Saving results...")
        results_dir = Path(__file__).parent.parent / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        all_results = {
            "model_type": model_type,
            "dataset_size": len(data["sample_ids"]),
            "anomaly_detection": anomaly_results,
            "rca_classification": rca_results,
            "training_time_seconds": round(time.time() - total_start, 1),
        }

        results_path = results_dir / "model_results.json"
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"  Saved to: {results_path}")

    # ──────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────
    total_time = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  TRAINING COMPLETE")
    print(f"{'='*70}")
    print(f"  Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"\n  ANOMALY DETECTION:")
    print(f"    Accuracy:  {anomaly_results['accuracy']:.4f}")
    print(f"    F1 Score:  {anomaly_results['f1']:.4f}")
    print(f"    AUC-ROC:   {anomaly_results['auc_roc']:.4f}")

    if rca_results:
        print(f"\n  RCA CLASSIFICATION:")
        for target, res in rca_results.items():
            print(f"    {target}: Accuracy={res['accuracy']:.4f}, F1={res['f1_weighted']:.4f}")

    print(f"\n{'='*70}\n")

    return {
        "detector": detector,
        "rca": rca,
        "remediation": remediation,
        "loader": loader,
        "splits": splits,
        "results": all_results if save_results else None,
    }


def compare_models(max_samples=None):
    """Train and compare multiple model types."""
    model_types = ["random_forest", "gradient_boosting"]
    if HAS_XGBOOST:
        model_types.append("xgboost")

    all_results = {}
    for mt in model_types:
        print(f"\n\n{'#'*70}")
        print(f"  MODEL: {mt.upper()}")
        print(f"{'#'*70}")
        result = train_full_pipeline(max_samples=max_samples, model_type=mt, save_results=False)
        all_results[mt] = result["results"] if result.get("results") else result

    # Comparison table
    print(f"\n\n{'='*70}")
    print(f"  MODEL COMPARISON")
    print(f"{'='*70}")
    print(f"  {'Model':<25s} {'Accuracy':>10s} {'F1':>10s} {'AUC-ROC':>10s}")
    print(f"  {'─'*55}")
    for mt, res in all_results.items():
        if isinstance(res, dict) and "anomaly_detection" in res:
            ad = res["anomaly_detection"]
            print(f"  {mt:<25s} {ad['accuracy']:>10.4f} {ad['f1']:>10.4f} {ad['auc_roc']:>10.4f}")

    return all_results


# Import check for XGBoost
try:
    from xgboost import XGBClassifier
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Multimodal RCA Models")
    parser.add_argument("--max-samples", type=int, default=None,
                        help="Limit dataset size for faster testing")
    parser.add_argument("--model", type=str, default="random_forest",
                        choices=["random_forest", "xgboost", "gradient_boosting", "logistic_regression"],
                        help="Model type")
    parser.add_argument("--compare", action="store_true",
                        help="Compare all model types")

    args = parser.parse_args()

    if args.compare:
        compare_models(max_samples=args.max_samples)
    else:
        train_full_pipeline(max_samples=args.max_samples, model_type=args.model)
