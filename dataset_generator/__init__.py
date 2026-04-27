"""
Dataset Generator Package for the Multimodal RCA Engine.
Generates a massive multimodal training dataset with:
  - Synthetic time-series metrics (11 infrastructure layers)
  - Synthetic log entries (multi-layer, multi-level)
  - Dashboard visualizations (Grafana-style images)
  - RCA labels (root cause, severity, remediation)
"""

from .metric_generator import MetricGenerator, load_scenarios
from .log_generator import LogGenerator
from .dashboard_generator import DashboardGenerator
from .dataset_builder import DatasetBuilder
