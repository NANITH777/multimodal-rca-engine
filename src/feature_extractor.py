"""
Feature Extraction Module for the Multimodal RCA Engine.
Converts parsed logs into numerical feature vectors for anomaly detection.
"""

import numpy as np
import pandas as pd
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from pathlib import Path


class LogFeatureExtractor:
    """
    Extract features from parsed and template-mined log data.
    Supports event counting, TF-IDF, and sequential features.
    """

    def __init__(self):
        self.tfidf_vectorizer = None
        self.event_vocab = None

    # ============================================
    # Session / Trace Grouping
    # ============================================

    def group_by_block_id(self, df, block_id_col="block_id", event_col="event_id"):
        """
        Group logs by HDFS block ID to create sessions.
        
        Args:
            df: DataFrame with parsed logs (must have block_id and event_id columns)
            block_id_col: Column name for block IDs
            event_col: Column name for event/template IDs
            
        Returns:
            Dict mapping block_id -> list of event_ids
        """
        sessions = {}
        for block_id, group in df.groupby(block_id_col):
            if pd.notna(block_id):
                sessions[block_id] = group[event_col].tolist()
        
        print(f"📊 Created {len(sessions):,} sessions from block IDs")
        return sessions

    def group_by_time_window(self, df, timestamp_col="timestamp", event_col="event_id", window_minutes=5):
        """
        Group logs by fixed time windows.
        
        Args:
            df: DataFrame with parsed logs
            timestamp_col: Column name for timestamps
            event_col: Column name for event/template IDs
            window_minutes: Size of each time window in minutes
            
        Returns:
            Dict mapping window_id -> list of event_ids
        """
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
        df = df.dropna(subset=[timestamp_col])
        
        df["window_id"] = (
            df[timestamp_col]
            .dt.floor(f"{window_minutes}min")
            .astype(str)
        )
        
        sessions = {}
        for window_id, group in df.groupby("window_id"):
            sessions[window_id] = group[event_col].tolist()
        
        print(f"📊 Created {len(sessions):,} sessions from {window_minutes}-min windows")
        return sessions

    # ============================================
    # Event Count Vectors
    # ============================================

    def build_event_count_matrix(self, sessions, event_ids=None):
        """
        Build an event count matrix from sessions.
        Each row = one session, each column = count of a specific event template.
        
        Args:
            sessions: Dict mapping session_id -> list of event_ids
            event_ids: Optional list of event IDs to use as vocabulary.
                       If None, auto-discovers from data.
                       
        Returns:
            tuple: (pd.DataFrame with count matrix, list of session_ids)
        """
        if event_ids is None:
            all_events = set()
            for events in sessions.values():
                all_events.update(events)
            event_ids = sorted(all_events)

        self.event_vocab = event_ids

        rows = []
        session_ids = []

        for session_id, events in sessions.items():
            counter = Counter(events)
            row = [counter.get(eid, 0) for eid in event_ids]
            rows.append(row)
            session_ids.append(session_id)

        columns = [f"E{eid}" for eid in event_ids]
        df = pd.DataFrame(rows, columns=columns, index=session_ids)
        df.index.name = "session_id"

        print(f"📊 Event count matrix: {df.shape[0]} sessions × {df.shape[1]} event types")
        return df, session_ids

    # ============================================
    # TF-IDF Features
    # ============================================

    def build_tfidf_features(self, log_contents, max_features=500):
        """
        Build TF-IDF feature matrix from raw log content strings.
        
        Args:
            log_contents: List of log content strings (one per session/document)
            max_features: Maximum number of TF-IDF features
            
        Returns:
            tuple: (feature matrix as np.ndarray, feature names)
        """
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=max_features,
            stop_words="english",
            token_pattern=r"(?u)\b[A-Za-z_][A-Za-z0-9_]{2,}\b",  # Ignore pure numbers
            lowercase=True,
        )

        tfidf_matrix = self.tfidf_vectorizer.fit_transform(log_contents)
        feature_names = self.tfidf_vectorizer.get_feature_names_out()

        print(f"📊 TF-IDF matrix: {tfidf_matrix.shape[0]} documents × {tfidf_matrix.shape[1]} features")
        return tfidf_matrix, feature_names

    # ============================================
    # Sequential Features
    # ============================================

    def build_sequence_features(self, sessions, max_seq_len=50, pad_value=-1):
        """
        Build padded/truncated event sequences for each session.
        Useful for sequential models (LSTM, Transformer).
        
        Args:
            sessions: Dict mapping session_id -> list of event_ids
            max_seq_len: Maximum sequence length (truncate or pad)
            pad_value: Value to use for padding
            
        Returns:
            tuple: (np.ndarray of shape [n_sessions, max_seq_len], list of session_ids)
        """
        sequences = []
        session_ids = []

        for session_id, events in sessions.items():
            seq = list(events[:max_seq_len])

            # Pad if needed
            if len(seq) < max_seq_len:
                seq.extend([pad_value] * (max_seq_len - len(seq)))

            sequences.append(seq)
            session_ids.append(session_id)

        X = np.array(sequences)
        print(f"📊 Sequence matrix: {X.shape[0]} sessions × {X.shape[1]} max_length")
        return X, session_ids

    # ============================================
    # Anomaly Label Loading
    # ============================================

    @staticmethod
    def load_hdfs_labels(label_path):
        """
        Load HDFS anomaly labels from anomaly_label.csv.
        
        Args:
            label_path: Path to anomaly_label.csv
            
        Returns:
            dict mapping block_id -> label (0=normal, 1=anomaly)
        """
        df = pd.read_csv(label_path)

        # Handle different column name conventions
        if "BlockId" in df.columns:
            block_col = "BlockId"
        elif "block_id" in df.columns:
            block_col = "block_id"
        else:
            block_col = df.columns[0]

        if "Label" in df.columns:
            label_col = "Label"
        elif "label" in df.columns:
            label_col = "label"
        else:
            label_col = df.columns[1]

        labels = {}
        for _, row in df.iterrows():
            block_id = row[block_col]
            label_val = row[label_col]
            # Normalize to 0/1
            if isinstance(label_val, str):
                labels[block_id] = 1 if label_val.lower() in ["anomaly", "abnormal", "1"] else 0
            else:
                labels[block_id] = int(label_val)

        n_anomaly = sum(labels.values())
        n_normal = len(labels) - n_anomaly
        print(f"📊 Labels loaded: {n_normal:,} normal, {n_anomaly:,} anomaly ({n_anomaly/len(labels)*100:.1f}%)")

        return labels

    @staticmethod
    def align_features_and_labels(feature_df, labels_dict):
        """
        Align feature matrix rows with labels.
        
        Args:
            feature_df: DataFrame with session_id as index
            labels_dict: Dict mapping session_id -> label
            
        Returns:
            tuple: (aligned feature DataFrame, labels array)
        """
        common_ids = list(set(feature_df.index) & set(labels_dict.keys()))
        common_ids.sort()

        X = feature_df.loc[common_ids]
        y = np.array([labels_dict[sid] for sid in common_ids])

        print(f"📊 Aligned: {len(common_ids):,} sessions with both features and labels")
        print(f"   Normal:  {(y == 0).sum():,}")
        print(f"   Anomaly: {(y == 1).sum():,}")

        return X, y
