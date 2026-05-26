"""
================================================================================
Multimodal RCA Engine - Anomali Teşhis ve Çözüm Önerileri (Remediation) Test Betiği
================================================================================
Bu betik, multimodal veri kümesinden örnek normal ve anomali içeren kayıtları çeker,
eğitilen modellerle tahminler yapar, kök nedenlerini ve etkilenen altyapı katmanlarını bulur,
ve configs/anomaly_scenarios.yaml dosyasından ilgili sistem için önerilen çözüm 
adımlarını (Remediation Actions) ekrana yazdırır.

Tüm loglar ve çıktılar Türkçe olarak tasarlanmıştır.
"""

import os
import sys
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Proje kök dizinini Python yoluna ekle
sys.path.insert(0, str(Path(__file__).parent))

from models.data_loader import MultimodalDataLoader
from models.rca_models import AnomalyDetector, RCAClassifier, RemediationEngine
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier

def print_box(title, lines, color_code="32"):
    """Konsolda renkli ve çerçeveli bir kutu çizer. (32=Yeşil, 31=Kırmızı, 36=Turkuaz)"""
    width = max(len(line) for line in lines + [title]) + 6
    print(f"\033[1;{color_code}m" + "=" * width)
    print(f"‖  {title:<{width-6}}  ‖")
    print("=" * width)
    for line in lines:
        print(f"‖  {line:<{width-6}}  ‖")
    print("=" * width + "\033[0m")

def main():
    print("=" * 80)
    print("🔍 Multimodal RCA Teşhis ve Çözüm Arama Modülü Başlatılıyor...")
    print("=" * 80)

    # 1. Veri Yükleme
    dataset_dir = Path("d:/multimodal-rca-engine/data/multimodal_dataset")
    loader = MultimodalDataLoader(dataset_dir=dataset_dir, noise_level=0.15, feature_dropout=0.05, seed=42)
    data = loader.build_feature_matrix(max_samples=10000, include_tfidf=False)
    splits = loader.split_data(data, test_size=0.2, val_size=0.1, random_state=42)
    
    test_split = splits["test"]
    X_test = np.hstack([test_split["X_metrics"].values, test_split["X_logs"].values])
    y_test_anomaly = test_split["y_anomaly"]

    # 2. Modellerin Hızlıca Eğitilmesi (Çok hızlı sürer)
    print("\n🤖 Teşhis modelleri hızlıca eğitiliyor...")
    
    # Anomali Dedektörü (XGBoost)
    detector = xgb.XGBClassifier(n_estimators=100, max_depth=6, eval_metric="logloss", random_state=42, n_jobs=-1)
    X_train = np.hstack([splits["train"]["X_metrics"].values, splits["train"]["X_logs"].values])
    y_train = splits["train"]["y_anomaly"]
    detector.fit(X_train, y_train)
    
    # Kök Neden Analizi Modelleri (Random Forest)
    train_anomaly_mask = splits["train"]["y_anomaly"] == 1
    X_train_anom = X_train[train_anomaly_mask]
    
    rca_models = {}
    for target in ["root_cause", "layer", "scenario"]:
        rf = RandomForestClassifier(n_estimators=100, max_depth=12, class_weight="balanced", random_state=42, n_jobs=-1)
        rf.fit(X_train_anom, splits["train"][f"y_{target}"][train_anomaly_mask])
        rca_models[target] = rf
        
    print("✅ Modeller başarıyla eğitildi ve hazırlandı.")

    # 3. Çözüm Arama Motorunun Yüklenmesi
    remediation_engine = RemediationEngine()
    
    # Label Decoder'ları tanımlama
    scenario_le = loader.label_encoders["y_scenario"]
    layer_le = loader.label_encoders["y_layer"]
    rc_le = loader.label_encoders["y_root_cause"]

    # 4. Örnek Test Vakaları Seçimi
    # Test setinden 1 adet normal ve 2 adet farklı anomali örneği çekelim
    normal_indices = np.where(y_test_anomaly == 0)[0]
    anomaly_indices = np.where(y_test_anomaly == 1)[0]

    test_cases = []
    if len(normal_indices) > 0:
        test_cases.append(("Normal Sistem Örneği", normal_indices[0]))
    if len(anomaly_indices) > 1:
        test_cases.append(("Birinci Anomali Vakası", anomaly_indices[0]))
        test_cases.append(("İkinci Anomali Vakası", anomaly_indices[1]))

    # Dizin oluşturma
    figures_dir = Path("d:/multimodal-rca-engine/results/figures")
    figures_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 80)
    print("🧪 CANLI TEST VE TEŞHİS BAŞLIYOR")
    print("=" * 80)

    for case_name, idx in test_cases:
        print(f"\n👉 [İnceleme Altındaki Örnek]: {case_name} (Test İndeksi: {idx})")
        sample_x = X_test[idx].reshape(1, -1)
        
        # A. Anomali Kararı
        is_anom_pred = detector.predict(sample_x)[0]
        prob_anom = detector.predict_proba(sample_x)[0, 1]
        
        if is_anom_pred == 0:
            lines = [
                f"Tahmin: NORMAL SİSTEM DURUMU (Güven Skoru: %{(1 - prob_anom)*100:.2f})",
                "Metrik Durumu: Tüm parametreler ve loglar kabul edilebilir seviyede.",
                "Çözüm: Herhangi bir müdahaleye gerek yoktur. Sistem stabil çalışıyor."
            ]
            print_box("🟢 DURUM: SİSTEM NORMAL", lines, color_code="32")
        else:
            # B. RCA Tahminleri
            pred_rc_code = rca_models["root_cause"].predict(sample_x)[0]
            pred_layer_code = rca_models["layer"].predict(sample_x)[0]
            pred_sc_code = rca_models["scenario"].predict(sample_x)[0]
            
            pred_rc = rc_le.inverse_transform([pred_rc_code])[0]
            pred_layer = layer_le.inverse_transform([pred_layer_code])[0]
            pred_sc = scenario_le.inverse_transform([pred_sc_code])[0]
            
            # YAML'dan detayları ve çözüm adımlarını al
            rem_info = remediation_engine.get_remediation(pred_sc)
            sc_info = rem_info.get("info", {})
            actions = rem_info.get("actions", [])
            
            # Orijinal Türkçe eşleşmelerini ve detaylarını yazdır
            lines = [
                f"Tahmin: SİSTEM ANOMALİSİ (Güven Skoru: %{prob_anom*100:.2f})",
                f"Etkilenen Katman (Layer): {pred_layer.upper()}",
                f"Kök Neden Sınıfı (Category): {pred_rc.upper()}",
                f"Belirlenen Senaryo Kodu: {pred_sc}",
                f"Açıklama: {sc_info.get('name', 'Bilinmeyen Senaryo')}"
            ]
            print_box("🚨 ALARM: ANOMALİ VE KÖK NEDEN TESPİT EDİLDİ!", lines, color_code="31")
            
            # C. Çözüm Adımları (Solutions)
            action_lines = []
            for i, action in enumerate(actions):
                action_lines.append(f"{i+1}. [EYLEM] -> {action}")
                
            if not action_lines:
                action_lines.append("Detaylı çözüm adımı tanımlanmamış, sistem adminine bildirin.")
                
            print_box(f"🛠️ ÖNERİLEN ÇÖZÜM ADIMLARI (REMEDIATION SOLUTIONS)", action_lines, color_code="36")
            
            # D. Hızlı bir grafik çizerek kaydedelim (Metriklerin görselleştirilmesi)
            # Log seviyesi dağılımlarını görselleştirme
            plt.figure(figsize=(8, 4))
            log_metrics = test_split["X_logs"].iloc[idx]
            log_metrics.plot(kind="bar", color="crimson")
            plt.title(f"Örnek {idx} - Log Hata Göstergeleri Dağılımı ({pred_sc})", fontsize=12)
            plt.ylabel("Değer")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            
            plot_path = figures_dir / f"test_diagnose_log_sample_{idx}.png"
            plt.savefig(plot_path, dpi=120)
            plt.close()
            print(f"📊 Örnek {idx} için log özellikleri grafiği kaydedildi: {plot_path}")

    print("\n" + "=" * 80)
    print("🎉 VAKA İNCELEMELERİ VE TEŞHİS TESTLERİ TAMAMLANDI!")
    print("=" * 80)

if __name__ == "__main__":
    main()
