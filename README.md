# Multimodal RCA Engine — Bulut Sistemlerinde Multimodal LLM ve VLM Tabanlı Otonom Kök Neden Analizi

<div align="center">

**Multimodal Root Cause Analysis Engine for Cloud Systems**

*Designing and developing an autonomous root cause analysis mechanism based on multimodal LLM and VLM in cloud systems*

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange.svg)](https://jupyter.org/)
[![License](https://img.shields.io/badge/License-Research-green.svg)](#)

</div>

---

## 📋 Project Overview

This project implements a **Multimodal Root Cause Analysis (RCA) Engine** for cloud and microservice systems. It combines:

- **LLM-based semantic log analysis** — Understanding log patterns, anomalies, and error sequences
- **VLM-based visual metric analysis** — Analyzing dashboards, time-series graphs, and monitoring visualizations
- **Multimodal Data Fusion** — Correlating textual logs with visual metrics for comprehensive root cause identification

## 🏗️ Project Structure

```
multimodal-rca-engine/
├── notebooks/                          # Jupyter notebooks (step-by-step analysis)
│   ├── 01_data_acquisition_exploration.ipynb
│   ├── 02_log_parsing.ipynb
│   ├── 03_feature_engineering.ipynb
│   └── 04_llm_semantic_analysis.ipynb
├── src/                                # Reusable Python modules
│   ├── __init__.py
│   ├── log_parser.py
│   ├── feature_extractor.py
│   └── utils.py
├── configs/
│   └── datasets.yaml                   # Dataset configuration
├── data/                               # (gitignored) Raw & processed data
├── results/                            # (gitignored) Figures & reports
├── requirements.txt
└── README.md
```

## 🚀 Getting Started

### 1. Clone & Setup
```bash
git clone https://github.com/NANITH777/multimodal-rca-engine.git
cd multimodal-rca-engine
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Run Notebooks
```bash
jupyter notebook
```
Open notebooks in order: `01` → `02` → `03` → `04`

## 📊 Datasets

| Source | Dataset | Description |
|--------|---------|-------------|
| [LogHub](https://github.com/logpai/loghub) | HDFS_v1 | Hadoop logs with anomaly labels |
| [LogHub](https://github.com/logpai/loghub) | BGL | Blue Gene/L supercomputer logs |
| [LogHub](https://github.com/logpai/loghub) | OpenStack | Cloud platform logs |
| [Alibaba](https://github.com/alibaba/clusterdata) | Microservices v2022 | 28K+ microservices call graphs & metrics |

## 📄 Citations

```bibtex
@inproceedings{zhu2023loghub,
  title={Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics},
  author={Zhu, Jieming and He, Shilin and He, Pinjia and Liu, Jinyang and Lyu, Michael R.},
  booktitle={ISSRE},
  year={2023}
}
```

## 👥 Team
- **Supervisor**: Doç. Dr. Süleyman Eken — Kocaeli University
- **Researcher**: Canberk Duman — PhD Student

---
*Kocaeli Üniversitesi — TÜBİTAK Project*