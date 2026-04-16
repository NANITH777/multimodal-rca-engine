"""
RCA Analyzer Module.
Formats log sessions, builds text prompts, and evaluates results.
"""

import json
from pathlib import Path
from datetime import datetime
from sklearn.metrics import classification_report, confusion_matrix, precision_score, recall_score, f1_score
import pandas as pd

import sys
import os

class SessionFormatter:
    """Methods for formatting pandas DataFrame log sessions into text."""
    
    @staticmethod
    def format_session_from_df(session_df, block_id, max_events=50):
        """Format a Pandas DataFrame session into standard LLM format."""
        lines = []
        lines.append(f"[SESSION_START] Block: {block_id}")
        lines.append(f"Total events: {len(session_df)}, Showing: {min(len(session_df), max_events)}")
        lines.append("---")
        
        for i, (_, row) in enumerate(session_df.head(max_events).iterrows()):
            level = row.get('level', 'UNKNOWN')
            component = row.get('component', 'unknown')
            content = str(row.get('content', ''))
            
            if len(content) > 200:
                content = content[:200] + '...'
                
            lines.append(f"[E{i+1:03d}] [{level:<5s}] [{component}] {content}")
            
        lines.append("---")
        lines.append(f"[SESSION_END] Total: {len(session_df)} events")
        
        return "\n".join(lines)


class PromptBuilder:
    """Loads prompt templates and formats them with session data."""
    
    def __init__(self, templates_path=None):
        if templates_path is None:
            # Default to the processed directory if not provided
            project_root = Path(__file__).parent.parent
            templates_path = project_root / "data" / "processed" / "rca_prompt_templates.json"
            
        self.templates = {}
        if templates_path.exists():
            with open(templates_path, "r", encoding="utf-8") as f:
                self.templates = json.load(f)
        else:
            print(f"⚠️ Warning: Prompt template file {templates_path} not found!")

    def build_anomaly_classification_prompt(self, session_text):
        """Build a system and user prompt for anomaly classification."""
        if "anomaly_classification" not in self.templates:
            return None, None
            
        template = self.templates["anomaly_classification"]
        system_prompt = template["system"]
        user_prompt = template["user"].replace("{session_text}", session_text)
        
        return system_prompt, user_prompt

    def build_root_cause_prompt(self, session_text, template_info=""):
        """Build a system and user prompt for Root Cause Analysis."""
        if "root_cause_analysis" not in self.templates:
            return None, None
            
        template = self.templates["root_cause_analysis"]
        system_prompt = template["system"]
        
        user_prompt = template["user"].replace("{session_text}", session_text)
        user_prompt = user_prompt.replace("{template_info}", template_info)
        
        return system_prompt, user_prompt


class ResultEvaluator:
    """Evaluates classification performance logic against ground truth labels."""
    
    @staticmethod
    def evaluate_classifications(y_true, y_pred, y_pred_baseline=None):
        """
        Evaluate classification metrics.
        
        Args:
            y_true: True labels (0=Normal, 1=Anomaly)
            y_pred: LLM predicted labels (0=Normal, 1=Anomaly)
            y_pred_baseline: Baseline predictions for comparison
            
        Returns:
            dict of metrics
        """
        results = {}
        
        # Calculate Primary metrics (Focusing on Class 1 / Anomaly)
        cm = confusion_matrix(y_true, y_pred)
        results["confusion_matrix"] = cm.tolist()
        
        results["precision"] = precision_score(y_true, y_pred, zero_division=0)
        results["recall"] = recall_score(y_true, y_pred, zero_division=0)
        results["f1"] = f1_score(y_true, y_pred, zero_division=0)
        
        # Detailed report
        results["report"] = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
        
        if y_pred_baseline is not None:
            results["baseline_f1"] = f1_score(y_true, y_pred_baseline, zero_division=0)
            results["improvement"] = results["f1"] - results["baseline_f1"]
            
        return results

    @staticmethod
    def print_comparison_table(metrics_llm, metrics_baseline, llm_name="LLM"):
        """Prints a clean comparison table to the console."""
        print(f"\n📊 --- PERFORMANS KARŞILAŞTIRMASI ---")
        print(f"{'Metrik':<20} | {'Baseline (IF)':<15} | {llm_name:<15}")
        print("-" * 55)
        
        # Assuming baseline recall/precision from historical config or metrics argument
        bl_prec = metrics_baseline.get('precision', 0.48)
        bl_rec = metrics_baseline.get('recall', 0.41)
        bl_f1 = metrics_baseline.get('f1', 0.44)
        
        print(f"{'Anomali Precision':<20} | {bl_prec:<15.2f} | {metrics_llm['precision']:<15.2f}")
        print(f"{'Anomali Recall':<20} | {bl_rec:<15.2f} | {metrics_llm['recall']:<15.2f}")
        print(f"{'F1 Score (Ana)':<20} | {bl_f1:<15.2f} | {metrics_llm['f1']:<15.2f}")
        print("-" * 55)
        
        diff = metrics_llm['f1'] - bl_f1
        if diff > 0:
            print(f"✅ Sonuç: {llm_name}, Baseline modele göre +{diff*100:.1f} puan F1 artışı sağladı!")
        else:
            print(f"⚠️ Sonuç: {llm_name}, Baseline modele göre {diff*100:.1f} puan F1 düşüşü yaşadı.")
