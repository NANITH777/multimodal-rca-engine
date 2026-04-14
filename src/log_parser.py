"""
Log Parsing Module for the Multimodal RCA Engine.
Implements regex-based and Drain-based log parsing.
"""

import re
import pandas as pd
from collections import Counter, OrderedDict
from pathlib import Path


# ============================================
# Regex Patterns for Known Log Formats
# ============================================

LOG_PATTERNS = {
    "hdfs": {
        "pattern": r"^(?P<date>\d{6})\s+(?P<time>\d{6})\s+(?P<pid>\d+)\s+(?P<level>\w+)\s+(?P<component>[^:]+):\s+(?P<content>.*)$",
        "columns": ["date", "time", "pid", "level", "component", "content"],
    },
    "bgl": {
        "pattern": r"^(?P<label>\S+)\s+(?P<timestamp>\d+)\s+(?P<date>\S+)\s+(?P<node>\S+)\s+(?P<time>\S+)\s+(?P<node_repeat>\S+)\s+(?P<type>\S+)\s+(?P<component>\S+)\s+(?P<level>\S+)\s+(?P<content>.*)$",
        "columns": ["label", "timestamp", "date", "node", "time", "node_repeat", "type", "component", "level", "content"],
    },
    "openstack": {
        "pattern": r"^(?P<logrecord>\S+)\s+(?P<date>\S+)\s+(?P<time>\S+)\s+(?P<pid>\d+)\s+(?P<level>\w+)\s+(?P<component>\S+)\s+(?P<address>\[[\w\s\-\.]*\])\s+(?P<content>.*)$",
        "columns": ["logrecord", "date", "time", "pid", "level", "component", "address", "content"],
    },
}


class RegexLogParser:
    """
    Parse unstructured log lines into structured records using regex patterns.
    """

    def __init__(self, log_format="hdfs"):
        """
        Initialize parser with a predefined or custom format.
        
        Args:
            log_format: One of 'hdfs', 'bgl', 'openstack' or a custom regex string
        """
        if log_format in LOG_PATTERNS:
            self.pattern = re.compile(LOG_PATTERNS[log_format]["pattern"])
            self.columns = LOG_PATTERNS[log_format]["columns"]
            self.format_name = log_format
        else:
            self.pattern = re.compile(log_format)
            self.columns = list(self.pattern.groupindex.keys())
            self.format_name = "custom"

        self.parse_failures = 0
        self.total_lines = 0

    def parse_line(self, line):
        """
        Parse a single log line.
        
        Returns:
            dict with parsed fields, or None if parsing fails.
        """
        self.total_lines += 1
        match = self.pattern.match(line.strip())
        if match:
            return match.groupdict()
        else:
            self.parse_failures += 1
            return None

    def parse_file(self, filepath, max_lines=None, show_progress=True):
        """
        Parse an entire log file into a DataFrame.
        
        Args:
            filepath: Path to the log file
            max_lines: Maximum lines to parse (None = all)
            show_progress: Whether to show progress
            
        Returns:
            pd.DataFrame with parsed log entries
        """
        from tqdm import tqdm

        filepath = Path(filepath)
        records = []
        self.parse_failures = 0
        self.total_lines = 0

        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            if max_lines is None:
                lines = f.readlines()
            else:
                lines = []
                for i, line in enumerate(f):
                    if i >= max_lines:
                        break
                    lines.append(line)

        iterator = tqdm(lines, desc=f"Parsing {filepath.name}") if show_progress else lines

        for line in iterator:
            result = self.parse_line(line)
            if result:
                records.append(result)

        df = pd.DataFrame(records)

        success_rate = (1 - self.parse_failures / max(self.total_lines, 1)) * 100
        print(f"\n📊 Parsing Results for {filepath.name}:")
        print(f"   Total lines:     {self.total_lines:,}")
        print(f"   Parsed OK:       {len(records):,}")
        print(f"   Parse failures:  {self.parse_failures:,}")
        print(f"   Success rate:    {success_rate:.1f}%")

        return df

    def get_stats(self):
        """Return parsing statistics."""
        return {
            "total_lines": self.total_lines,
            "parsed_ok": self.total_lines - self.parse_failures,
            "failures": self.parse_failures,
            "success_rate": (1 - self.parse_failures / max(self.total_lines, 1)) * 100,
        }


class DrainLogParser:
    """
    Log template extraction using the Drain algorithm.
    Wraps the drain3 library for easy integration.
    """

    def __init__(self, depth=4, sim_th=0.4, max_children=100):
        """
        Initialize Drain parser.
        
        Args:
            depth: Depth of the parse tree
            sim_th: Similarity threshold for template matching
            max_children: Max children per node in parse tree
        """
        try:
            from drain3 import TemplateMiner
            from drain3.template_miner_config import TemplateMinerConfig
        except ImportError:
            raise ImportError(
                "drain3 is not installed. Install it with: pip install drain3"
            )

        config = TemplateMinerConfig()
        config.drain_depth = depth
        config.drain_sim_th = sim_th
        config.drain_max_children = max_children
        config.profiling_enabled = False

        self.template_miner = TemplateMiner(config=config)
        self.templates = {}
        self.cluster_counts = Counter()

    def add_log_message(self, log_message):
        """
        Process a single log message through Drain.
        
        Returns:
            dict with cluster_id and template
        """
        result = self.template_miner.add_log_message(log_message)
        cluster_id = result["cluster_id"]
        template = result["template_mined"]

        self.templates[cluster_id] = template
        self.cluster_counts[cluster_id] += 1

        return {
            "cluster_id": cluster_id,
            "template": template,
            "change_type": result.get("change_type", "none"),
        }

    def process_logs(self, log_messages, show_progress=True):
        """
        Process a list of log messages and extract templates.
        
        Args:
            log_messages: List of log content strings
            show_progress: Show progress bar
            
        Returns:
            List of dicts with cluster_id and template for each message
        """
        from tqdm import tqdm

        results = []
        iterator = tqdm(log_messages, desc="Drain Parsing") if show_progress else log_messages

        for msg in iterator:
            result = self.add_log_message(msg)
            results.append(result)

        return results

    def get_templates_df(self):
        """
        Get all discovered templates as a DataFrame.
        
        Returns:
            pd.DataFrame with columns: cluster_id, template, count
        """
        data = []
        for cluster_id, template in self.templates.items():
            data.append({
                "cluster_id": cluster_id,
                "template": template,
                "count": self.cluster_counts[cluster_id],
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values("count", ascending=False).reset_index(drop=True)

        return df

    def get_summary(self):
        """Print a summary of discovered templates."""
        n_templates = len(self.templates)
        total_logs = sum(self.cluster_counts.values())

        print(f"\n🔍 Drain Template Mining Summary:")
        print(f"   Total log messages processed: {total_logs:,}")
        print(f"   Unique templates discovered:  {n_templates}")
        print(f"   Avg logs per template:        {total_logs / max(n_templates, 1):.1f}")

        print(f"\n   Top 10 Templates:")
        for cluster_id, count in self.cluster_counts.most_common(10):
            template = self.templates[cluster_id]
            # Truncate long templates
            display_template = template[:80] + "..." if len(template) > 80 else template
            print(f"   [{cluster_id:4d}] ({count:6,}x) {display_template}")


def extract_block_ids(log_content_series):
    """
    Extract HDFS block IDs from log content.
    Used for session/trace grouping in HDFS logs.
    
    Args:
        log_content_series: pd.Series of log content strings
        
    Returns:
        pd.Series of block IDs (NaN where not found)
    """
    block_pattern = re.compile(r"(blk_-?\d+)")
    return log_content_series.str.extract(block_pattern, expand=False)
