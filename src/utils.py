"""
Utility functions for the Multimodal RCA Engine.
Handles data downloading, extraction, and common file operations.
"""

import os
import sys
import zipfile
import tarfile
import requests
import yaml
from pathlib import Path
from tqdm import tqdm


# ============================================
# Project Paths
# ============================================
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PARSED_DIR = DATA_DIR / "parsed"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"
FIGURES_DIR = RESULTS_DIR / "figures"
CONFIGS_DIR = PROJECT_ROOT / "configs"


def ensure_directories():
    """Create all necessary project directories."""
    for d in [RAW_DIR, PARSED_DIR, PROCESSED_DIR, FIGURES_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    print("✅ All directories created successfully.")


def load_config(config_name="datasets.yaml"):
    """Load a YAML configuration file."""
    config_path = CONFIGS_DIR / config_name
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_file(url, dest_path, chunk_size=8192):
    """
    Download a file from URL with progress bar.
    Skips download if file already exists.
    """
    dest_path = Path(dest_path)
    if dest_path.exists():
        print(f"⏭️  File already exists: {dest_path.name}")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"⬇️  Downloading: {dest_path.name}")
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))

    with open(dest_path, "wb") as f:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc=dest_path.name) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                pbar.update(len(chunk))

    print(f"✅ Downloaded: {dest_path.name} ({total_size / 1e6:.1f} MB)")
    return dest_path


def extract_archive(archive_path, extract_dir=None):
    """
    Extract a .zip, .tar.gz, or .tar file.
    Returns the extraction directory.
    """
    archive_path = Path(archive_path)
    if extract_dir is None:
        extract_dir = archive_path.parent

    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    print(f"📦 Extracting: {archive_path.name}")

    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
    elif archive_path.name.endswith(".tar.gz") or archive_path.name.endswith(".tgz"):
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(extract_dir)
    elif archive_path.suffix == ".tar":
        with tarfile.open(archive_path, "r") as tf:
            tf.extractall(extract_dir)
    else:
        print(f"⚠️  Unknown archive format: {archive_path.suffix}")
        return extract_dir

    print(f"✅ Extracted to: {extract_dir}")
    return extract_dir


def download_and_extract_dataset(dataset_key, config=None):
    """
    Download and extract a dataset by its key from the config.
    
    Args:
        dataset_key: One of 'hdfs_v1', 'bgl', 'openstack'
        config: Optional pre-loaded config dict
    
    Returns:
        Path to extracted data directory
    """
    if config is None:
        config = load_config()

    dataset_info = config["loghub"]["datasets"].get(dataset_key)
    if dataset_info is None:
        raise ValueError(f"Unknown dataset: {dataset_key}. Available: {list(config['loghub']['datasets'].keys())}")

    url = dataset_info["url"]
    name = dataset_info["name"]

    # Determine file extension from URL
    if ".zip" in url:
        ext = ".zip"
    elif ".tar.gz" in url:
        ext = ".tar.gz"
    else:
        ext = ".zip"

    archive_path = RAW_DIR / f"{name}{ext}"
    extract_dir = RAW_DIR / name

    # Download
    download_file(url, archive_path)

    # Extract if not already done
    if not extract_dir.exists() or not any(extract_dir.iterdir()):
        extract_archive(archive_path, RAW_DIR)
    else:
        print(f"⏭️  Already extracted: {name}")

    return extract_dir


def read_log_file(filepath, max_lines=None, encoding="utf-8"):
    """
    Read a log file and return lines.
    
    Args:
        filepath: Path to the log file
        max_lines: Maximum number of lines to read (None = all)
        encoding: File encoding
    
    Returns:
        List of log lines
    """
    filepath = Path(filepath)
    lines = []

    try:
        with open(filepath, "r", encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                if max_lines and i >= max_lines:
                    break
                lines.append(line.rstrip("\n"))
    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")

    return lines


def get_file_size_mb(filepath):
    """Get file size in MB."""
    return os.path.getsize(filepath) / (1024 * 1024)


def count_lines(filepath):
    """Count the number of lines in a file efficiently."""
    count = 0
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for _ in f:
            count += 1
    return count


def print_section(title, char="═", width=60):
    """Print a formatted section header."""
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}\n")
