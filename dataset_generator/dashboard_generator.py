"""
Dashboard Generator Module for the Multimodal RCA Engine.
Generates Grafana-style dark-themed monitoring dashboard images from metric data.
Each dashboard shows multiple metric panels for a given infrastructure layer.
"""

import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server-side rendering
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyBboxPatch
from pathlib import Path
from datetime import datetime


# ============================================
# Grafana Dark Theme Configuration
# ============================================

GRAFANA_COLORS = {
    "bg": "#0b0e11",
    "panel_bg": "#181b1f",
    "panel_border": "#2a2e34",
    "text": "#d8d9da",
    "text_dim": "#8e8e8e",
    "grid": "#2a2e34",
    "green": "#73bf69",
    "yellow": "#f2cc0c",
    "orange": "#ff9830",
    "red": "#f2495c",
    "blue": "#5794f2",
    "purple": "#b877d9",
    "cyan": "#8ab8ff",
    "teal": "#4ec9b0",
}

METRIC_COLORS = [
    GRAFANA_COLORS["green"],
    GRAFANA_COLORS["blue"],
    GRAFANA_COLORS["orange"],
    GRAFANA_COLORS["yellow"],
    GRAFANA_COLORS["purple"],
    GRAFANA_COLORS["cyan"],
    GRAFANA_COLORS["teal"],
    GRAFANA_COLORS["red"],
]

SEVERITY_COLORS = {
    "critical": GRAFANA_COLORS["red"],
    "high": GRAFANA_COLORS["orange"],
    "medium": GRAFANA_COLORS["yellow"],
    "low": GRAFANA_COLORS["green"],
}


class DashboardGenerator:
    """
    Generates monitoring dashboard images in Grafana dark-theme style.
    Creates multi-panel time-series visualizations from metric DataFrames.
    """

    def __init__(self, figsize=(14, 8), dpi=100):
        """
        Args:
            figsize: Figure size in inches (width, height)
            dpi: Resolution (dots per inch) — 100 for speed, 150 for quality
        """
        self.figsize = figsize
        self.dpi = dpi
        self._setup_matplotlib_style()

    def _setup_matplotlib_style(self):
        """Configure matplotlib to match Grafana dark theme."""
        plt.rcParams.update({
            'figure.facecolor': GRAFANA_COLORS["bg"],
            'axes.facecolor': GRAFANA_COLORS["panel_bg"],
            'axes.edgecolor': GRAFANA_COLORS["panel_border"],
            'axes.labelcolor': GRAFANA_COLORS["text"],
            'xtick.color': GRAFANA_COLORS["text_dim"],
            'ytick.color': GRAFANA_COLORS["text_dim"],
            'text.color': GRAFANA_COLORS["text"],
            'grid.color': GRAFANA_COLORS["grid"],
            'grid.alpha': 0.3,
            'font.family': 'sans-serif',
            'font.size': 9,
        })

    def _format_metric_name(self, name):
        """Convert metric_name to 'Metric Name' for display."""
        return name.replace('_', ' ').replace('pct', '%').replace('ms', '(ms)').replace(
            'mbps', '(Mbps)').replace('rpm', '(RPM)').title()

    def _draw_panel_border(self, ax):
        """Draw a subtle panel border like Grafana."""
        for spine in ax.spines.values():
            spine.set_color(GRAFANA_COLORS["panel_border"])
            spine.set_linewidth(0.5)

    def _add_anomaly_region(self, ax, timestamps, anomaly_start_idx, severity="high"):
        """Add a shaded anomaly region to a panel."""
        if anomaly_start_idx is None or anomaly_start_idx >= len(timestamps):
            return

        color = SEVERITY_COLORS.get(severity, GRAFANA_COLORS["orange"])
        ax.axvspan(
            timestamps[anomaly_start_idx],
            timestamps[-1],
            alpha=0.08,
            color=color,
            zorder=0
        )
        ax.axvline(
            timestamps[anomaly_start_idx],
            color=color,
            linestyle='--',
            alpha=0.5,
            linewidth=1,
            zorder=5
        )

    def generate_dashboard(self, metrics_df, layer_name, scenario_name=None,
                           is_anomaly=False, anomaly_start_idx=None, severity=None,
                           output_path=None):
        """
        Generate a complete dashboard image from metric data.

        Args:
            metrics_df: pd.DataFrame with timestamp column + metric columns
            layer_name: Name of the infrastructure layer
            scenario_name: Name of the anomaly scenario (for title)
            is_anomaly: Whether this is an anomalous sample
            anomaly_start_idx: Index where anomaly starts
            severity: Severity level
            output_path: Where to save the image (None = return fig)

        Returns:
            Path to saved image, or matplotlib figure
        """
        metric_cols = [c for c in metrics_df.columns if c != 'timestamp']
        n_metrics = len(metric_cols)

        # Determine grid layout
        if n_metrics <= 2:
            nrows, ncols = 1, 2
        elif n_metrics <= 4:
            nrows, ncols = 2, 2
        elif n_metrics <= 6:
            nrows, ncols = 3, 2
        else:
            nrows, ncols = 4, 2

        fig, axes = plt.subplots(nrows, ncols, figsize=self.figsize)
        fig.patch.set_facecolor(GRAFANA_COLORS["bg"])

        if nrows == 1 and ncols == 1:
            axes = np.array([[axes]])
        elif nrows == 1 or ncols == 1:
            axes = axes.reshape(nrows, ncols)

        # Dashboard title
        status_indicator = "🔴" if is_anomaly else "🟢"
        title = f"  {layer_name}"
        if scenario_name:
            title += f"  —  {scenario_name}"
        if severity:
            title += f"  [{severity.upper()}]"

        fig.suptitle(title, fontsize=13, fontweight='bold',
                     color=GRAFANA_COLORS["text"], x=0.02, ha='left', y=0.98)

        # Status bar
        status_text = "ANOMALY DETECTED" if is_anomaly else "NORMAL"
        status_color = SEVERITY_COLORS.get(severity, GRAFANA_COLORS["green"]) if is_anomaly else GRAFANA_COLORS["green"]
        fig.text(0.98, 0.98, status_text, fontsize=10, fontweight='bold',
                 color=status_color, ha='right', va='top')

        timestamps = metrics_df['timestamp'].values

        for idx, metric in enumerate(metric_cols):
            row = idx // ncols
            col = idx % ncols
            if row >= nrows:
                break
            ax = axes[row][col]
            color = METRIC_COLORS[idx % len(METRIC_COLORS)]

            values = metrics_df[metric].values

            # Main line
            ax.plot(timestamps, values, color=color, linewidth=1.2, alpha=0.9)

            # Fill under curve
            ax.fill_between(timestamps, values, alpha=0.08, color=color)

            # Panel title
            formatted_name = self._format_metric_name(metric)
            current_val = values[-1]
            ax.set_title(f"{formatted_name}   {current_val:.1f}",
                        fontsize=9, fontweight='bold',
                        color=GRAFANA_COLORS["text"],
                        loc='left', pad=6)

            # Anomaly region
            if is_anomaly:
                self._add_anomaly_region(ax, timestamps, anomaly_start_idx, severity)

            # Grid and styling
            ax.grid(True, alpha=0.15, linestyle='-')
            ax.tick_params(axis='x', labelsize=7, rotation=30)
            ax.tick_params(axis='y', labelsize=7)
            self._draw_panel_border(ax)

            # X-axis formatting
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        # Hide unused panels
        for idx in range(n_metrics, nrows * ncols):
            row = idx // ncols
            col = idx % ncols
            if row < nrows:
                axes[row][col].set_visible(False)

        plt.tight_layout(rect=[0, 0.02, 1, 0.95])

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=self.dpi, facecolor=fig.get_facecolor(),
                       bbox_inches='tight', pad_inches=0.3)
            plt.close(fig)
            return output_path
        else:
            return fig

    def generate_compact_dashboard(self, metrics_df, layer_name, scenario_name=None,
                                    is_anomaly=False, anomaly_start_idx=None, severity=None,
                                    output_path=None):
        """
        Generate a compact, single-panel overlay dashboard (for smaller file sizes).
        Overlays all metrics on a single chart with a legend.

        Args: Same as generate_dashboard
        Returns: Path to saved image, or matplotlib figure
        """
        metric_cols = [c for c in metrics_df.columns if c != 'timestamp']

        fig, ax = plt.subplots(1, 1, figsize=(10, 4))
        fig.patch.set_facecolor(GRAFANA_COLORS["bg"])

        timestamps = metrics_df['timestamp'].values

        # Normalize all metrics to [0, 1] for overlay
        for idx, metric in enumerate(metric_cols):
            values = metrics_df[metric].values.astype(float)
            v_min, v_max = np.nanmin(values), np.nanmax(values)
            if v_max - v_min > 0:
                norm_values = (values - v_min) / (v_max - v_min)
            else:
                norm_values = np.zeros_like(values)

            color = METRIC_COLORS[idx % len(METRIC_COLORS)]
            formatted = self._format_metric_name(metric)
            ax.plot(timestamps, norm_values, color=color, linewidth=1.0, alpha=0.85, label=formatted)

        # Anomaly region
        if is_anomaly and anomaly_start_idx is not None:
            self._add_anomaly_region(ax, timestamps, anomaly_start_idx, severity)

        # Title
        title = f"{layer_name}"
        if scenario_name:
            title += f" — {scenario_name}"
        ax.set_title(title, fontsize=11, fontweight='bold', color=GRAFANA_COLORS["text"], loc='left')

        # Status
        status_color = SEVERITY_COLORS.get(severity, GRAFANA_COLORS["green"]) if is_anomaly else GRAFANA_COLORS["green"]
        status_text = f"ANOMALY [{severity.upper()}]" if is_anomaly and severity else "NORMAL"
        ax.text(0.99, 0.95, status_text, transform=ax.transAxes,
                fontsize=9, fontweight='bold', color=status_color, ha='right', va='top')

        ax.legend(loc='upper left', fontsize=7, framealpha=0.3,
                 facecolor=GRAFANA_COLORS["panel_bg"], edgecolor=GRAFANA_COLORS["panel_border"],
                 labelcolor=GRAFANA_COLORS["text"])

        ax.set_ylabel("Normalized Value", fontsize=8, color=GRAFANA_COLORS["text_dim"])
        ax.grid(True, alpha=0.15)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.tick_params(axis='x', labelsize=7, rotation=30)
        ax.tick_params(axis='y', labelsize=7)
        self._draw_panel_border(ax)

        plt.tight_layout()

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=self.dpi, facecolor=fig.get_facecolor(),
                       bbox_inches='tight', pad_inches=0.2)
            plt.close(fig)
            return output_path
        else:
            return fig
