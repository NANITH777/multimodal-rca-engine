"""
================================================================================
Gelişmiş Makine Öğrenimi (XGBoost & LightGBM) ve SHAP ile Açıklanabilir RCA Hattı
================================================================================
Bu betik, metrik ve log özniteliklerini içeren multimodal veri kümesini yükler,
ikili anomali tespiti için XGBoost ve LightGBM modellerini eğitir ve karşılaştırır,
çok sınıflı Kök Neden Analizi (RCA) için Random Forest modellerini eğitir ve 
SHAP (SHapley Additive exPlanations) kullanarak model kararlarını açıklar.

Tüm kod yorum satırları ve ekrana yazdırılacak loglar Türkçe olarak tasarlanmıştır.
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Proje kök dizinini Python yoluna ekle
sys.path.insert(0, str(Path(__file__).parent))

from models.data_loader import MultimodalDataLoader
from models.rca_models import AnomalyDetector, RCAClassifier
import xgboost as xgb
import lightgbm as lgb
import shap
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.ensemble import RandomForestClassifier

def main():
    print("=" * 85)
    print("🚀 Gelişmiş XGBoost & LightGBM & SHAP Multimodal RCA Eğitim Hattı Başlatılıyor")
    print("=" * 85)

    # 1. Veri Yükleme ve Hazırlık
    # --------------------------------------------------------------------------
    dataset_dir = Path("d:/multimodal-rca-engine/data/multimodal_dataset")
    print(f"\n📂 [1/5] Veri kümesi dizini okunuyor: {dataset_dir}")
    
    # MultimodalDataLoader nesnesi oluşturma (gürültü ve öznitelik kaybı simülasyonu aktif)
    loader = MultimodalDataLoader(dataset_dir=dataset_dir, noise_level=0.15, feature_dropout=0.05, seed=42)
    
    # 10.000 örneklik veri kümesini yükle (TF-IDF öznitelikleri olmadan hızlı işlem için)
    print("📊 Öznitelik matrisi inşa ediliyor...")
    data = loader.build_feature_matrix(max_samples=10000, include_tfidf=False)
    
    # Veriyi bölme: Eğitim (%70), Doğrulama (%10), Test (%20)
    print("\n✂️ Veri seti Eğitim, Doğrulama ve Test kümelerine bölünüyor...")
    splits = loader.split_data(data, test_size=0.2, val_size=0.1, random_state=42)
    
    train_split = splits["train"]
    val_split = splits["val"]
    test_split = splits["test"]
    
    # Öznitelik isimlerini dinamik olarak tanımlama
    metric_names = []
    for pos in range(8):
        prefix = f"m{pos}"
        for stat in ["mean", "std", "min", "max", "range", "median", "skew", "kurtosis", "slope", "spike", "change", "volatility", "autocorr"]:
            metric_names.append(f"{prefix}_{stat}")
            
    log_names = [
        "log_total_lines", "log_info_count", "log_warn_count", 
        "log_error_count", "log_critical_count", "log_error_ratio", 
        "log_warn_ratio", "log_critical_ratio", "log_severity_score"
    ]
    
    feature_names = metric_names + log_names
    print(f"✅ Toplam öznitelik sayısı: {len(feature_names)}")

    # 2. Anomali Tespiti Modellerinin Eğitimi ve Karşılaştırılması (Binary Classification)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("🤖 [2/5] Anomali Tespiti Modellerinin Eğitimi ve Karşılaştırması")
    print("=" * 65)

    # A. Öznitelikleri birleştirme fonksiyonu
    def combine_features(split):
        return np.hstack([split["X_metrics"].values, split["X_logs"].values])

    X_train = combine_features(train_split)
    y_train = train_split["y_anomaly"]
    
    X_val = combine_features(val_split)
    y_val = val_split["y_anomaly"]
    
    X_test = combine_features(test_split)
    y_test = test_split["y_anomaly"]

    # B. XGBoost Modeli Eğitimi
    print("\n📈 XGBoost Modeli eğitiliyor...")
    xgb_model = xgb.XGBClassifier(
        n_estimators=200, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        eval_metric="logloss", random_state=42, n_jobs=-1
    )
    xgb_model.fit(X_train, y_train)
    print("   XGBoost Eğitimi Tamamlandı.")

    # C. LightGBM Modeli Eğitimi
    print("\n⚡ LightGBM Modeli eğitiliyor...")
    lgb_model = lgb.LGBMClassifier(
        n_estimators=200, max_depth=8, learning_rate=0.1,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, n_jobs=-1, verbose=-1
    )
    lgb_model.fit(X_train, y_train)
    print("   LightGBM Eğitimi Tamamlandı.")

    # D. İki Modelin Performansının Karşılaştırılması
    models = {"XGBoost": xgb_model, "LightGBM": lgb_model}
    model_metrics = {}

    for name, model in models.items():
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        auc = roc_auc_score(y_test, y_prob)
        
        model_metrics[name] = {
            "Accuracy": acc,
            "Precision": prec,
            "Recall": rec,
            "F1-Score": f1,
            "AUC-ROC": auc
        }

    # Karşılaştırma tablosunu ekrana yazdırma
    print("\n📊 TEST SETİ MODELLERİN KARŞILAŞTIRMA TABLOSU:")
    print("=" * 75)
    print(f"{'Model':<15} | {'Accuracy':<10} | {'Precision':<10} | {'Recall':<10} | {'F1-Score':<10} | {'AUC-ROC':<10}")
    print("-" * 75)
    for name, metrics in model_metrics.items():
        print(f"{name:<15} | {metrics['Accuracy']:<10.4f} | {metrics['Precision']:<10.4f} | {metrics['Recall']:<10.4f} | {metrics['F1-Score']:<10.4f} | {metrics['AUC-ROC']:<10.4f}")
    print("=" * 75)

    # Detaylı Sınıflandırma Raporu (XGBoost için)
    print("\n🔍 XGBoost Modeli Detaylı Sınıflandırma Raporu:")
    xgb_pred = xgb_model.predict(X_test)
    print(classification_report(y_test, xgb_pred, target_names=["Normal Durum", "Sistem Anomalisi"]))

    # 3. RCA Çok Sınıflı Modellerinin Eğitimi (Random Forest)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("🎯 [3/5] Kök Neden Analizi (RCA) Çok Sınıflı Sınıflandırıcılar")
    print("=" * 65)
    print("Sınıf süreksizliklerini (Discontinuous Classes) engellemek amacıyla Random Forest kullanılmaktadır.")

    # RCA modellerini eğitme
    rca_targets = {
        "root_cause": "Kök Neden Kategorisi",
        "layer": "Altyapı Katmanı",
        "scenario": "Anomali Senaryosu"
    }

    # Sadece anomalili örnekleri süz
    train_anomaly_mask = train_split["y_anomaly"] == 1
    test_anomaly_mask = test_split["y_anomaly"] == 1

    X_train_anom = X_train[train_anomaly_mask]
    X_test_anom = X_test[test_anomaly_mask]

    rca_models = {}
    for target_key, target_name in rca_targets.items():
        y_train_target = train_split[f"y_{target_key}"][train_anomaly_mask]
        y_test_target = test_split[f"y_{target_key}"][test_anomaly_mask]
        
        n_classes = len(np.unique(y_train_target))
        print(f"\n📌 {target_name} eğitiliyor ({n_classes} Sınıf)...")
        
        rf = RandomForestClassifier(n_estimators=150, max_depth=18, class_weight="balanced", random_state=42, n_jobs=-1)
        rf.fit(X_train_anom, y_train_target)
        rca_models[target_key] = rf
        
        # Test performansı değerlendirme
        y_pred_target = rf.predict(X_test_anom)
        acc = accuracy_score(y_test_target, y_pred_target)
        f1_macro = f1_score(y_test_target, y_pred_target, average="macro", zero_division=0)
        
        print(f"   📊 Test Skoru: Doğruluk={acc:.4f} | F1 (Macro)={f1_macro:.4f}")

    # 4. SHAP ile Karar Açıklanabilirliği Analizi (XGBoost Üzerinden)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("🔍 [4/5] SHAP ile Anomali Karar Mekanizması Açıklanabilirliği")
    print("=" * 65)
    
    print("TreeExplainer başlatılıyor ve SHAP değerleri hesaplanıyor...")
    explainer = shap.TreeExplainer(xgb_model)
    
    # Hızlı hesaplama için test setinden rastgele 500 örnek alınıyor
    shap_sample_size = min(500, X_test.shape[0])
    X_test_sample = X_test[:shap_sample_size]
    
    shap_values = explainer(X_test_sample)
    
    figures_dir = Path("d:/multimodal-rca-engine/results/figures")
    figures_dir.mkdir(parents=True, exist_ok=True)

    # A. Global SHAP Özet Grafiği (Summary Plot)
    summary_plot_path = figures_dir / "shap_summary_plot.png"
    print(f"\n📈 Global SHAP Özet Grafiği oluşturuluyor -> {summary_plot_path}")
    
    plt.figure(figsize=(12, 8))
    shap.summary_plot(shap_values, X_test_sample, feature_names=feature_names, show=False)
    plt.title("RCA Engine - SHAP Global Anomali Öznitelik Analizi", fontsize=14, pad=15)
    plt.tight_layout()
    plt.savefig(summary_plot_path, dpi=150)
    plt.close()

    # B. Bireysel Örnek Açıklaması (Waterfall Plot)
    # Test setindeki ilk sistem anomalisini (Anomaly=1) süzüp inceleyelim
    y_test_sample_labels = y_test[:shap_sample_size]
    anomaly_indices = np.where(y_test_sample_labels == 1)[0]
    
    if len(anomaly_indices) > 0:
        sample_idx = anomaly_indices[0]  # İlk anomali örneğinin indeksi
        sample_scenario_code = test_split["y_scenario"][sample_idx]
        
        # Orijinal senaryo adını label encoder'dan çözme
        scenario_le = loader.label_encoders["y_scenario"]
        scenario_name = scenario_le.inverse_transform([sample_scenario_code])[0]
        
        waterfall_plot_path = figures_dir / f"shap_waterfall_sample_{sample_idx}.png"
        print(f"🎯 Bireysel Anomali İncelemesi: Örnek {sample_idx} ({scenario_name}) için Waterfall Grafiği çiziliyor...")
        
        plt.figure(figsize=(12, 6))
        # shap_values nesnesi waterfall plot'a uygun formata dönüştürülüyor
        shap.plots.waterfall(shap_values[sample_idx], show=False)
        plt.title(f"Örnek {sample_idx} Anomali Karar Analizi ({scenario_name})", fontsize=12, pad=15)
        plt.tight_layout()
        plt.savefig(waterfall_plot_path, dpi=150)
        plt.close()
    else:
        print("⚠️ Test setinde açıklanabilecek anomali örneği bulunamadı.")

    # 5. Pipeline Tamamlanma Raporu
    # --------------------------------------------------------------------------
    print("\n" + "=" * 85)
    print("🎉 Gelişmiş Makine Öğrenimi ve Açıklanabilirlik Hattı Başarıyla Tamamlandı!")
    print(f"📁 Grafikleriniz şu dizine başarıyla kaydedildi: {figures_dir}")
    print("   1. shap_summary_plot.png (Genel Öznitelik Önem Dereceleri)")
    print("   2. shap_waterfall_sample_*.png (Bireysel Anomali Hata Teşhis Analizi)")
    print("=" * 85)

if __name__ == "__main__":
    main()
