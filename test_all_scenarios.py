"""
================================================================================
 Multimodal RCA Engine — Kapsamlı Senaryo Teşhis ve Çözüm Raporu
================================================================================
Bu betik, 10.000 örneklik multimodal veri kümesinden tüm altyapı katmanlarını
ve 22 anomali senaryosunu kapsayan gerçekçi testler yapar. Her anomali için:
  - Güven skoru ve risk seviyesi belirlenir
  - Etkilenen katman, kök neden ve senaryo tespit edilir
  - Yapıcı ve ayrıntılı Türkçe açıklama mesajları üretilir
  - Öncelik sırasına göre sıralanmış çözüm adımları listelenir
  - Özet HTML rapor üretilir
"""

import sys, yaml, numpy as np, pandas as pd, matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
from models.data_loader import MultimodalDataLoader
from models.rca_models import RemediationEngine
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, f1_score, precision_score,
                              recall_score, roc_auc_score, classification_report)
import xgboost as xgb

# ──────────────────────────────────────────────────────────────
# SEVERITY KATEGORİLERİ VE RENKLERİ
# ──────────────────────────────────────────────────────────────
SEVERITY_CONFIG = {
    "critical": {"label": "🔴 KRİTİK",  "color": "#dc2626", "priority": 1, "sla": "15 dakika"},
    "high":     {"label": "🟠 YÜKSEK",  "color": "#ea580c", "priority": 2, "sla": "1 saat"},
    "medium":   {"label": "🟡 ORTA",    "color": "#ca8a04", "priority": 3, "sla": "4 saat"},
    "low":      {"label": "🟢 DÜŞÜK",   "color": "#16a34a", "priority": 4, "sla": "24 saat"},
}

# ──────────────────────────────────────────────────────────────
# YAML'DAN SENARYO BİLGİLERİNİ YÜKLELİM
# ──────────────────────────────────────────────────────────────
def load_scenario_catalog():
    yaml_path = Path(__file__).parent / "configs" / "anomaly_scenarios.yaml"
    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    catalog = {}
    for layer_key, layer_data in cfg.get("layers", {}).items():
        for sc in layer_data.get("scenarios", []):
            catalog[sc["id"]] = {
                "id":          sc["id"],
                "name":        sc["name"],
                "name_tr":     sc.get("name_tr", sc["name"]),
                "description": sc.get("description", ""),
                "layer":       layer_key,
                "layer_tr":    layer_data.get("name_tr", layer_key),
                "root_cause":  sc.get("root_cause", ""),
                "root_cause_tr": sc.get("root_cause_tr", ""),
                "root_cause_category": sc.get("root_cause_category", ""),
                "severity":    sc.get("severity", "medium"),
                "remediation": sc.get("remediation", []),
                "anomaly_type": sc.get("anomaly_type", ""),
                "detection_params": sc.get("detection_params", []),
            }
    return catalog

# ──────────────────────────────────────────────────────────────
# YAPILANDIRMACI TEŞHİS MESAJI ÜRETİCİ
# ──────────────────────────────────────────────────────────────
def build_diagnosis_message(scenario_info, prob, is_anomaly):
    """Tespit edilen anomali için yapıcı ve ayrıntılı Türkçe açıklama üretir."""
    if not is_anomaly:
        return {
            "durum": "✅ SİSTEM NORMAL",
            "ozet": "Tüm metrikler beklenen eşikler dahilinde seyrediyor.",
            "aciklama": (
                "Sistem şu anda stabil bir durumdadır. Metrik değerleri, log hata oranları "
                "ve altyapı sağlık göstergeleri normal aralıkta. "
                "Mevcut durumda herhangi bir müdahale gerekmemektedir."
            ),
            "risk": "Düşük",
            "sla": "—",
            "oncelik": 4,
        }

    sc   = scenario_info
    sev  = sc["severity"]
    cfg  = SEVERITY_CONFIG.get(sev, SEVERITY_CONFIG["medium"])
    risk_score = round(prob * 100, 1)

    aciklama_parts = [
        f"'{sc['layer_tr']}' katmanında **{sc['name_tr']}** senaryosu tespit edildi.",
        f"Kök neden analizi, bu durumun '{sc['root_cause_tr']}' kaynaklı olduğuna işaret ediyor.",
        f"Sistem anomali olasılığı %{risk_score:.1f} güvenle hesaplandı.",
    ]
    if sc["anomaly_type"] == "spike":
        aciklama_parts.append(
            "Ölçüm verilerinde ani ve keskin bir sıçrama gözlemlendi; "
            "bu durum genellikle beklenmedik yük artışı veya saldırı girişimine işaret eder."
        )
    elif sc["anomaly_type"] == "gradual_rise":
        aciklama_parts.append(
            "Değerlerde zaman içinde kademeli bir yükseliş gözlemlendi; "
            "bu durum kaynak tükenmesi veya konfigürasyon kaymalarına işaret edebilir."
        )
    elif sc["anomaly_type"] == "sustained_high":
        aciklama_parts.append(
            "Değerler uzun süredir yüksek seyrediyor; "
            "bu tür kalıcı anomaliler, altta yatan bir yapısal sorunun işareti olabilir."
        )
    elif sc["anomaly_type"] == "oscillation":
        aciklama_parts.append(
            "Değerlerde düzensiz salınım gözlemleniyor; "
            "bu durum yük dengeleyici hataları veya konfigürasyon tutarsızlıklarına işaret edebilir."
        )
    elif sc["anomaly_type"] == "step_increase":
        aciklama_parts.append(
            "Değerlerde basamaklı bir artış tespit edildi; "
            "bu durum donanım bozulması veya kritik bir sistem bileşenindeki arızaya işaret edebilir."
        )

    if sc["detection_params"]:
        aciklama_parts.append(
            f"Tetikleyici metrikler: {', '.join(sc['detection_params'])}."
        )

    return {
        "durum":    cfg["label"],
        "ozet":     sc["description"],
        "aciklama": " ".join(aciklama_parts),
        "risk":     f"%{risk_score}",
        "sla":      f"Müdahale SLA: {cfg['sla']}",
        "oncelik":  cfg["priority"],
        "renk":     cfg["color"],
    }

def build_remediation_steps(remediation_list, severity, scenario_id):
    """Çözüm adımlarını öncelikli ve ayrıntılı biçimde düzenler."""
    steps = []
    base_actions = remediation_list if remediation_list else ["Sistem adminine bildirin."]
    
    # Genel ön adım — her zaman önce durum değerlendirmesi
    steps.append({
        "adim": 0,
        "eylem": f"[HAZIRLIK] Anomali kaydını ve zaman damgasını kayıt altına al.",
        "aciklama": f"Senaryo {scenario_id} için olay kaydı oluşturun ve ilgili ekipleri bilgilendirin.",
        "oncelik": "ÖNCELİKLİ",
    })

    for i, action in enumerate(base_actions, 1):
        # Her eylemi genişlet
        if "restart" in action.lower() or "yeniden" in action.lower():
            detail = "Servisi durdurmadan önce aktif bağlantıları kontrol edin. Yeniden başlatma sonrası log dosyalarını inceleyin."
        elif "script" in action.lower() or "betiği" in action.lower():
            detail = "Betiği root yetkisiyle çalıştırın. Çıktıyı kayıt dosyasına yönlendirin ve başarılı tamamlandığını doğrulayın."
        elif "block" in action.lower() or "engelle" in action.lower() or "limit" in action.lower():
            detail = "Engelleme kuralını uygularken meşru trafiği etkilememek için beyaz liste kontrolü yapın."
        elif "backup" in action.lower() or "yedek" in action.lower():
            detail = "Yedeklemenin bütünlüğünü doğrulayın. Yedek tamamlanmadan donanım değişikliğine geçmeyin."
        elif "monitor" in action.lower() or "izle" in action.lower() or "analiz" in action.lower():
            detail = "İzleme sırasında 5 dakikalık aralıklarla ölçüm alın ve eğilimi kayıt altına alın."
        elif "clean" in action.lower() or "temizle" in action.lower():
            detail = "Temizlik öncesi hangi dosya ve kaynakların silineceğini listeleyin ve onay alın."
        elif "scale" in action.lower() or "ölçekle" in action.lower():
            detail = "Ölçekleme işlemi sırasında mevcut servis kesintisini minimize etmek için rolling update kullanın."
        else:
            detail = "Bu adımın uygulanması sırasında sistem durumunu sürekli izleyin."

        steps.append({
            "adim": i,
            "eylem": f"[EYLEM {i}] {action}",
            "aciklama": detail,
            "oncelik": "KRİTİK" if severity == "critical" and i == 1 else "STANDART",
        })

    # Son adım — izleme ve doğrulama
    steps.append({
        "adim": len(steps),
        "eylem": "[DOĞRULAMA] Uygulanan çözümlerin etkinliğini kontrol et.",
        "aciklama": (
            "Çözüm adımlarını uyguladıktan sonra 10-15 dakika boyunca metrikleri izleyin. "
            "Anomali değerleri normal eşiğin altına inmişse olayı kapalı olarak işaretleyin. "
            "Devam ediyorsa bir üst seviye (L2/L3) destek ekibine iletim yapın."
        ),
        "oncelik": "KAPANIŞ",
    })
    return steps

# ──────────────────────────────────────────────────────────────
# ANA FONKSİYON
# ──────────────────────────────────────────────────────────────
def main():
    print("=" * 80)
    print("  Multimodal RCA Engine — Kapsamlı Senaryo Teşhis Sistemi")
    print("=" * 80)
    print(f"  Başlangıç: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Senaryo kataloğunu yükle
    catalog = load_scenario_catalog()
    print(f"✅ Senaryo kataloğu yüklendi: {len(catalog)} senaryo")

    # Veri yükle ve modelleri eğit
    dataset_dir = Path("d:/multimodal-rca-engine/data/multimodal_dataset")
    loader = MultimodalDataLoader(dataset_dir=dataset_dir, noise_level=0.15, feature_dropout=0.05, seed=42)
    data = loader.build_feature_matrix(max_samples=10000, include_tfidf=False)
    splits = loader.split_data(data, test_size=0.2, val_size=0.1, random_state=42)

    def combine(s): return np.hstack([s['X_metrics'].values, s['X_logs'].values])
    X_train, y_train = combine(splits['train']), splits['train']['y_anomaly']
    X_test,  y_test  = combine(splits['test']),  splits['test']['y_anomaly']

    print("\n📈 Modeller eğitiliyor...")
    detector = xgb.XGBClassifier(n_estimators=200, max_depth=8, eval_metric='logloss', random_state=42, n_jobs=-1)
    detector.fit(X_train, y_train)

    anom_mask_tr = y_train == 1
    X_tr_anom    = X_train[anom_mask_tr]
    rca_models   = {}
    le           = loader.label_encoders
    for key in ['root_cause', 'layer', 'scenario', 'severity']:
        y_tr = splits['train'][f'y_{key}'][anom_mask_tr]
        rf = RandomForestClassifier(n_estimators=150, max_depth=18, class_weight='balanced', random_state=42, n_jobs=-1)
        rf.fit(X_tr_anom, y_tr)
        rca_models[key] = rf
    print("✅ Tüm modeller hazır.\n")

    # ── GENEL PERFORMANS METRİKLERİ ──────────────────────────────
    yp = detector.predict(X_test)
    yb = detector.predict_proba(X_test)[:, 1]
    metrics = {
        'Accuracy':  accuracy_score(y_test, yp),
        'Precision': precision_score(y_test, yp, zero_division=0),
        'Recall':    recall_score(y_test, yp, zero_division=0),
        'F1-Score':  f1_score(y_test, yp, zero_division=0),
        'AUC-ROC':   roc_auc_score(y_test, yb),
    }
    print("━" * 60)
    print("  ANOMALİ DEDEKTÖRÜ PERFORMANSI (Test Seti)")
    print("━" * 60)
    for k, v in metrics.items():
        bar = "█" * int(v * 20)
        print(f"  {k:<12}: {v:.4f}  {bar}")
    print()

    # ── TEST SENARYOLARINı SEÇ ───────────────────────────────────
    # Her altyapı katmanından en az 1 anomali + 2 normal örnek
    anom_idxs   = np.where(y_test == 1)[0]
    normal_idxs = np.where(y_test == 0)[0]

    # Her benzersiz senaryodan 1 örnek seç (maks 22)
    sc_le     = le['y_scenario']
    test_sc   = sc_le.inverse_transform(splits['test']['y_scenario'][anom_idxs])
    seen_scs  = set()
    sel_anom  = []
    for raw_idx, sc_name in zip(anom_idxs, test_sc):
        if sc_name != 'none' and sc_name not in seen_scs:
            seen_scs.add(sc_name)
            sel_anom.append(raw_idx)
        if len(sel_anom) >= 22:
            break

    # 3 normal örnek
    sel_normal = list(normal_idxs[:3])

    all_cases = (
        [("NORMAL", idx) for idx in sel_normal] +
        [("ANOMALI", idx) for idx in sel_anom]
    )

    # ── TÜM VAKALARI TEŞHİS ET ───────────────────────────────────
    print("━" * 60)
    print(f"  CANLI TEŞHİS TESTLERİ — {len(all_cases)} VAKA")
    print("━" * 60)

    results      = []
    severity_cnt = defaultdict(int)
    scenario_hits= defaultdict(int)

    for case_type, idx in all_cases:
        x   = X_test[idx].reshape(1, -1)
        is_anom  = detector.predict(x)[0]
        prob_anom= detector.predict_proba(x)[0, 1]

        if is_anom == 1:
            sc_code  = rca_models['scenario'].predict(x)[0]
            sc_id    = sc_le.inverse_transform([sc_code])[0]
            layer_id = le['y_layer'].inverse_transform([rca_models['layer'].predict(x)[0]])[0]
            rc_id    = le['y_root_cause'].inverse_transform([rca_models['root_cause'].predict(x)[0]])[0]
            sev_code = rca_models['severity'].predict(x)[0]
            sev_id   = le['y_severity'].inverse_transform([sev_code])[0]
            sc_info  = catalog.get(sc_id, {
                "id": sc_id, "name_tr": sc_id, "description": "",
                "layer_tr": layer_id, "root_cause_tr": rc_id,
                "root_cause_category": rc_id, "severity": sev_id,
                "remediation": [], "anomaly_type": "", "detection_params": [],
            })
        else:
            sc_info  = {}
            sc_id    = "none"
            layer_id = "—"
            rc_id    = "—"
            sev_id   = "normal"

        diag  = build_diagnosis_message(sc_info, prob_anom, is_anom)
        steps = build_remediation_steps(
            sc_info.get("remediation", []),
            sev_id,
            sc_id
        ) if is_anom else []

        result = {
            "case_type":   case_type,
            "idx":         idx,
            "is_anomaly":  is_anom,
            "prob_anomaly":prob_anom,
            "scenario_id": sc_id,
            "layer":       layer_id,
            "root_cause":  rc_id,
            "severity":    sev_id,
            "diagnosis":   diag,
            "steps":       steps,
            "sc_info":     sc_info,
        }
        results.append(result)

        if is_anom:
            severity_cnt[sev_id] += 1
            scenario_hits[sc_id] += 1

        # Konsola özet yazdır
        sep = "─" * 60
        print(f"\n{sep}")
        print(f"  Örnek #{idx:4d}  |  {diag['durum']}")
        print(sep)
        if is_anom:
            print(f"  Senaryo   : {sc_id} — {sc_info.get('name_tr','')}")
            print(f"  Katman    : {sc_info.get('layer_tr', layer_id)}")
            print(f"  Kök Neden : {sc_info.get('root_cause_tr', rc_id)}")
            print(f"  Risk      : {diag['risk']}")
            print(f"  Müdahale  : {diag['sla']}")
            print(f"\n  📝 AÇIKLAMA:")
            # Satır sarması
            words = diag['aciklama'].split()
            line, out = [], []
            for w in words:
                line.append(w)
                if len(' '.join(line)) > 62:
                    out.append('  ' + ' '.join(line[:-1]))
                    line = [w]
            if line:
                out.append('  ' + ' '.join(line))
            print('\n'.join(out))
            print(f"\n  🛠️  ÇÖZÜM ADIMLARI:")
            for s in steps:
                print(f"     [{s['oncelik']}] {s['eylem']}")
                print(f"            → {s['aciklama'][:85]}{'...' if len(s['aciklama'])>85 else ''}")
        else:
            print(f"  Risk : Düşük | Güven: %{(1-prob_anom)*100:.1f}")
            print(f"  {diag['aciklama']}")

    # ── GRAFİKLER ────────────────────────────────────────────────
    figures_dir = Path("d:/multimodal-rca-engine/results/figures")
    figures_dir.mkdir(parents=True, exist_ok=True)

    # Grafik 1 — Senaryo dağılımı ve güven skoru
    anom_results = [r for r in results if r['is_anomaly']]

    fig = plt.figure(figsize=(18, 13))
    gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    # A. Güven skoru dağılımı (tüm vakalar)
    ax_a = fig.add_subplot(gs[0, :2])
    idxs = [r['idx'] for r in results]
    probs= [r['prob_anomaly'] for r in results]
    colors_a = ['#ef4444' if r['is_anomaly'] else '#22c55e' for r in results]
    bars = ax_a.bar(range(len(results)), probs, color=colors_a, alpha=0.85, width=0.8)
    ax_a.axhline(0.5, color='black', linestyle='--', lw=1.5, label='Karar Eşiği (0.5)')
    ax_a.set_xticks(range(len(results)))
    ax_a.set_xticklabels([r['scenario_id'][:7] for r in results], rotation=55, ha='right', fontsize=7.5)
    ax_a.set_ylabel('Anomali Olasılığı')
    ax_a.set_title(f'Tüm Test Vakalarının Anomali Güven Skoru ({len(results)} Vaka)', fontsize=12, pad=8)
    ax_a.set_ylim(0, 1.08)
    ax_a.legend(fontsize=9)
    normal_patch = plt.matplotlib.patches.Patch(color='#22c55e', label='Normal')
    anom_patch   = plt.matplotlib.patches.Patch(color='#ef4444', label='Anomali')
    ax_a.legend(handles=[normal_patch, anom_patch], loc='upper right', fontsize=9)

    # B. Şiddet dağılımı (pasta)
    ax_b = fig.add_subplot(gs[0, 2])
    sev_labels  = {'critical': 'Kritik', 'high': 'Yüksek', 'medium': 'Orta', 'low': 'Düşük'}
    sev_colors  = {'critical': '#dc2626', 'high': '#ea580c', 'medium': '#ca8a04', 'low': '#16a34a'}
    sev_data    = {k: v for k, v in severity_cnt.items() if v > 0}
    if sev_data:
        ax_b.pie([v for v in sev_data.values()],
                 labels=[sev_labels.get(k, k) for k in sev_data.keys()],
                 colors=[sev_colors.get(k, '#888') for k in sev_data.keys()],
                 autopct='%1.0f%%', startangle=90,
                 wedgeprops=dict(edgecolor='white', linewidth=2))
    ax_b.set_title('Şiddet Seviyesi\nDağılımı', fontsize=11, pad=8)

    # C. Senaryo hit bar chart
    ax_c = fig.add_subplot(gs[1, :])
    if scenario_hits:
        sc_names = list(scenario_hits.keys())
        sc_vals  = [scenario_hits[s] for s in sc_names]
        sc_colors_list = []
        for sc_id in sc_names:
            sev = catalog.get(sc_id, {}).get('severity', 'medium')
            sc_colors_list.append(sev_colors.get(sev, '#8b5cf6'))
        ax_c.bar(range(len(sc_names)), sc_vals, color=sc_colors_list, alpha=0.85)
        ax_c.set_xticks(range(len(sc_names)))
        ax_c.set_xticklabels(
            [f"{s}\n{catalog.get(s,{}).get('name_tr','')[:20]}" for s in sc_names],
            rotation=35, ha='right', fontsize=8
        )
        ax_c.set_ylabel('Tespit Sayısı')
        ax_c.set_title('Senaryo Başına Anomali Tespit Sayısı (Renk = Şiddet)', fontsize=12, pad=8)
        for i, (bar, sc_id) in enumerate(zip(ax_c.patches, sc_names)):
            sev = catalog.get(sc_id, {}).get('severity', '—')
            ax_c.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.02,
                      sev[:3].upper(), ha='center', va='bottom', fontsize=7, fontweight='bold')

    # D. Katman dağılımı (yatay bar)
    ax_d = fig.add_subplot(gs[2, :2])
    layer_counts = defaultdict(int)
    for r in anom_results:
        layer_counts[r['layer']] += 1
    if layer_counts:
        layers_sorted = sorted(layer_counts.items(), key=lambda x: x[1], reverse=True)
        lnames = [l[0].replace('_', ' ').title() for l in layers_sorted]
        lvals  = [l[1] for l in layers_sorted]
        ax_d.barh(range(len(lnames)), lvals, color='#6366f1', alpha=0.85)
        ax_d.set_yticks(range(len(lnames)))
        ax_d.set_yticklabels(lnames, fontsize=9)
        ax_d.set_xlabel('Anomali Sayısı')
        ax_d.set_title('Katman Bazında Anomali Dağılımı', fontsize=12, pad=8)

    # E. Kök neden dağılımı
    ax_e = fig.add_subplot(gs[2, 2])
    rc_counts = defaultdict(int)
    for r in anom_results:
        rc_counts[r['root_cause']] += 1
    if rc_counts:
        rc_names = list(rc_counts.keys())
        rc_vals  = [rc_counts[k] for k in rc_names]
        rc_palette = ['#2563eb','#16a34a','#dc2626','#f59e0b','#8b5cf6','#06b6d4']
        ax_e.pie(rc_vals, labels=rc_names,
                 colors=rc_palette[:len(rc_names)],
                 autopct='%1.0f%%', startangle=90,
                 wedgeprops=dict(edgecolor='white', linewidth=1.5),
                 textprops={'fontsize': 8})
        ax_e.set_title('Kök Neden\nKategorisi', fontsize=11, pad=8)

    fig.suptitle(
        f'Multimodal RCA Engine — Kapsamlı Teşhis Paneli  '
        f'({datetime.now().strftime("%Y-%m-%d %H:%M")})',
        fontsize=14, fontweight='bold', y=1.01
    )
    panel_path = figures_dir / 'scenario_diagnosis_panel.png'
    plt.savefig(panel_path, dpi=130, bbox_inches='tight')
    plt.close()
    print(f"\n✅ Kapsamlı teşhis paneli kaydedildi: {panel_path}")

    # ── HTML RAPOR ────────────────────────────────────────────────
    html_path = Path("d:/multimodal-rca-engine/results/diagnosis_report.html")
    severity_colors_html = {'critical': '#fee2e2', 'high': '#ffedd5',
                            'medium': '#fef9c3', 'normal': '#f0fdf4', 'none': '#f0fdf4'}

    rows = ""
    for r in results:
        bg = severity_colors_html.get(r['severity'], '#fff')
        sc_name = r['sc_info'].get('name_tr', '—') if r['is_anomaly'] else '—'
        layer_tr = r['sc_info'].get('layer_tr', r['layer']) if r['is_anomaly'] else 'Normal'
        rem_html = ""
        for s in r['steps']:
            rem_html += f"<li><b>[{s['oncelik']}]</b> {s['eylem']}<br><small>{s['aciklama']}</small></li>"
        rows += f"""
<tr style="background:{bg}">
  <td><code>{r['scenario_id']}</code></td>
  <td>{layer_tr}</td>
  <td>{r['sc_info'].get('root_cause_tr', r['root_cause']) if r['is_anomaly'] else '—'}</td>
  <td><b>{r['diagnosis']['durum']}</b></td>
  <td>{r['diagnosis']['risk']}</td>
  <td>{r['diagnosis']['sla']}</td>
  <td><small>{r['diagnosis']['aciklama'][:200]}...</small></td>
  <td><ol style="margin:0;padding-left:16px">{rem_html}</ol></td>
</tr>"""

    html_content = f"""<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<title>RCA Engine Teşhis Raporu</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; margin: 24px; background: #f8fafc; color: #1e293b; }}
  h1   {{ color: #1e293b; border-bottom: 3px solid #2563eb; padding-bottom: 8px; }}
  h2   {{ color: #2563eb; margin-top: 28px; }}
  .metrics {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
  .metric-card {{ background: white; border-radius: 8px; padding: 14px 20px;
                  box-shadow: 0 1px 4px rgba(0,0,0,0.1); min-width: 120px; text-align: center; }}
  .metric-card .val {{ font-size: 2em; font-weight: bold; color: #2563eb; }}
  .metric-card .label {{ font-size: 0.85em; color: #64748b; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; background: white;
           box-shadow: 0 1px 4px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }}
  th {{ background: #1e293b; color: white; padding: 10px 8px; font-size: 0.85em; text-align: left; }}
  td {{ padding: 8px; font-size: 0.82em; vertical-align: top; border-bottom: 1px solid #e2e8f0; }}
  tr:hover {{ filter: brightness(0.97); }}
  img {{ max-width: 100%; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); margin: 16px 0; }}
  footer {{ margin-top: 40px; color: #94a3b8; font-size: 0.8em; }}
</style>
</head>
<body>
<h1>🔍 Multimodal RCA Engine — Kapsamlı Senaryo Teşhis Raporu</h1>
<p>Oluşturulma: <b>{datetime.now().strftime('%d %B %Y, %H:%M:%S')}</b> &nbsp;|&nbsp;
   Test edilen vaka sayısı: <b>{len(all_cases)}</b> &nbsp;|&nbsp;
   Tespit edilen anomali: <b>{sum(1 for r in results if r['is_anomaly'])}</b></p>

<h2>📊 Model Performans Özeti</h2>
<div class="metrics">
{"".join(f'<div class="metric-card"><div class="val">{v:.4f}</div><div class="label">{k}</div></div>' for k,v in metrics.items())}
</div>

<h2>🖼️ Teşhis Paneli</h2>
<img src="figures/scenario_diagnosis_panel.png" alt="Teşhis Paneli">

<h2>📋 Senaryo Teşhis Tablosu</h2>
<table>
<thead>
  <tr>
    <th>Senaryo</th><th>Katman</th><th>Kök Neden</th><th>Durum</th>
    <th>Risk</th><th>SLA</th><th>Açıklama</th><th>Çözüm Adımları</th>
  </tr>
</thead>
<tbody>{rows}</tbody>
</table>
<footer>Multimodal RCA Engine © {datetime.now().year} — XGBoost + Random Forest + SHAP tabanlı anomali tespit motoru</footer>
</body>
</html>"""

    html_path.parent.mkdir(parents=True, exist_ok=True)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✅ HTML Teşhis Raporu kaydedildi: {html_path}")

    # ── ÖZET ─────────────────────────────────────────────────────
    n_anom = sum(1 for r in results if r['is_anomaly'])
    n_crit = severity_cnt.get('critical', 0)
    n_high = severity_cnt.get('high', 0)
    print(f"\n{'='*60}")
    print(f"  KAPSAMLI TEŞHİS TAMAMLANDI")
    print(f"{'='*60}")
    print(f"  Test edilen toplam vaka   : {len(results)}")
    print(f"  Tespit edilen anomali     : {n_anom}")
    print(f"  Kritik seviyeli anomali   : {n_crit}")
    print(f"  Yüksek seviyeli anomali   : {n_high}")
    print(f"  Tespit edilen senaryo çeşidi: {len(scenario_hits)}/22")
    print(f"  Anomali Dedektörü F1      : {metrics['F1-Score']:.4f}")
    print(f"  AUC-ROC                   : {metrics['AUC-ROC']:.4f}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
