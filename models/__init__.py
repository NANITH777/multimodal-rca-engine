"""
Models Package for the Multimodal RCA Engine.
ML/DL pipeline for anomaly detection, root cause analysis, and self-remediation.

Pipeline:
  1. DataLoader     → Load multimodal dataset
  2. AnomalyDetector → Detect anomalies (binary classification)
  3. RCAClassifier   → Classify root cause category
  4. SeverityPredictor → Predict severity level
  5. RemediationEngine → Suggest remediation actions
  6. MultimodalFusion → Combine all modalities
"""
