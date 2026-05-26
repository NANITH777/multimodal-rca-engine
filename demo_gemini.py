"""
Script de demonstration de l'integration Gemini dans le projet RCA Engine.
Execute ce script apres avoir configure ta cle API dans le fichier .env

Usage:
    python demo_gemini.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Exemple de scenario et diagnostic (simule sans ML)
example_scenario = {
    "id":             "VM_002",
    "name":           "Memory Exhaustion",
    "name_tr":        "Bellek Tukenmesi",
    "description":    "Memory usage at 95% with increasing swap usage.",
    "layer":          "linux_vm",
    "layer_tr":       "Linux VM",
    "root_cause":     "Memory leak or high memory demand",
    "root_cause_tr":  "Bellek sizintisi veya yuksek bellek gereksinimi",
    "root_cause_category": "resource",
    "severity":       "critical",
    "remediation":    ["Run memory cleanup script", "Monitor and optimize swap usage"],
    "anomaly_type":   "gradual_rise",
    "detection_params": ["memory_usage_pct", "disk_io_mbps", "load_average"],
}

example_diagnosis = {
    "durum":    "[KRITIK]",
    "risk":     "%97.3",
    "sla":      "Mudahale SLA: 15 dakika",
    "aciklama": "Linux VM katmaninda Bellek Tukenmesi senaryosu tespit edildi.",
}

example_log_features = {
    "log_total_lines":    1847,
    "log_info_count":     320,
    "log_warn_count":     412,
    "log_error_count":    891,
    "log_critical_count": 224,
    "log_error_ratio":    0.482,
    "log_warn_ratio":     0.223,
    "log_critical_ratio": 0.121,
    "log_severity_score": 8.7,
}

print("=" * 65)
print("  Multimodal RCA Engine — Gemini AI Entegrasyon Demonstrasyonu")
print("=" * 65)

try:
    from models.gemini_explainer import GeminiExplainer

    explainer = GeminiExplainer(language="tr")

    print("\n--- 1. ANOMALI ACIKLAMASI ---")
    explanation = explainer.explain_anomaly(example_scenario, example_diagnosis)
    print(explanation)

    print("\n--- 2. GENISLETILMIS COZUM PLANI ---")
    expanded = explainer.expand_remediation(example_scenario, example_scenario["remediation"])
    print(expanded)

    print("\n--- 3. LOG ANALİZİ YORUMU ---")
    log_analysis = explainer.analyze_logs(example_log_features, "VM_002")
    print(log_analysis)

    print("\n--- 4. SERBEST METIN SORU ---")
    answer = explainer.chat(
        "Bu anomali kac dakika icinde ciddi bir sistem kesintisine yol acabilir?",
        context={"scenario": "VM_002", "severity": "critical", "risk": "%97.3"}
    )
    print(answer)

    print("\n" + "=" * 65)
    print("  Entegrasyon demonstrasyonu basariyla tamamlandi.")
    print("=" * 65)

except ImportError as e:
    print(f"\nHATA: {e}")
    print("Cozum: pip install google-generativeai python-dotenv")

except ValueError as e:
    print(f"\nHATA: {e}")
    print("Cozum: d:/multimodal-rca-engine/.env dosyasina API anahtarini ekle:")
    print("  GEMINI_API_KEY=your_api_key_here")
