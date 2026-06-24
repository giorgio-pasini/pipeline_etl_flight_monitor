"""
Alerting simple basé sur les métriques du job (Étape 9).

Philosophie (cohérente avec le reste du projet) : pas d'infra lourde.
- Évalue des règles sur les métriques finalisées d'un batch
- Journalise chaque alerte (WARNING / ERROR)
- Persiste les alertes en JSON dans `_logs/alerts/`
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Niveaux de sévérité
INFO = "INFO"
WARNING = "WARNING"
CRITICAL = "CRITICAL"


def evaluate_alerts(metrics: Dict, config) -> List[Dict]:
    """
    Évaluer les règles d'alerte sur les métriques finalisées d'un batch.

    Règles :
    - Erreurs présentes -> CRITICAL
    - % valide < seuil (ALERT_THRESHOLD_PCT_VALID) -> WARNING
    - Durée > SLA (COLLECTION_TIMEOUT_MINUTES * 60) -> WARNING
    - Extraction vide -> WARNING

    Args:
        metrics: dict produit par JobMetrics.finalize()
        config: DatalakeConfig (seuils)

    Returns:
        Liste d'alertes [{severity, type, message}]
    """
    alerts: List[Dict] = []

    num_errors = metrics.get("num_errors", 0)
    if num_errors > 0:
        alerts.append({
            "severity": CRITICAL,
            "type": "pipeline_errors",
            "message": f"{num_errors} erreur(s) durant le batch",
        })

    pct_valid = metrics.get("validation", {}).get("pct_valid", 100)
    threshold = getattr(config, "ALERT_THRESHOLD_PCT_VALID", 70)
    extraction_rows = metrics.get("extraction", {}).get("rows", 0)
    # % valide pertinent seulement si des vols ont été extraits
    if extraction_rows > 0 and pct_valid < threshold:
        alerts.append({
            "severity": WARNING,
            "type": "low_data_quality",
            "message": f"Qualité {pct_valid:.1f}% < seuil {threshold}%",
        })

    duration = metrics.get("total_duration_seconds", 0)
    sla_seconds = getattr(config, "COLLECTION_TIMEOUT_MINUTES", 30) * 60
    if duration > sla_seconds:
        alerts.append({
            "severity": WARNING,
            "type": "sla_breach",
            "message": f"Durée {duration:.0f}s > SLA {sla_seconds}s",
        })

    if extraction_rows == 0:
        alerts.append({
            "severity": WARNING,
            "type": "empty_extraction",
            "message": "Aucun vol extrait de l'API",
        })

    return alerts


def dispatch_alerts(
    alerts: List[Dict],
    batch_id: str,
    config,
    logger_obj: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    Journaliser et persister les alertes.

    Args:
        alerts: liste d'alertes (sortie d'evaluate_alerts)
        batch_id: identifiant du batch
        config: DatalakeConfig (pour LOG_PATH)
        logger_obj: logger optionnel

    Returns:
        Chemin du fichier d'alertes écrit, ou None si aucune alerte
    """
    log = logger_obj or logger

    if not alerts:
        log.info("Aucune alerte (toutes les règles sont OK)")
        return None

    # Journalisation
    for a in alerts:
        line = f"[ALERTE {a['severity']}] {a['type']} : {a['message']}"
        if a["severity"] == CRITICAL:
            log.error(line)
        else:
            log.warning(line)

    # Persistance JSON
    payload = {
        "batch_id": batch_id,
        "timestamp": datetime.now().isoformat(),
        "alert_count": len(alerts),
        "alerts": alerts,
    }

    alerts_dir = Path(config.LOG_PATH) / "alerts"
    alerts_dir.mkdir(parents=True, exist_ok=True)
    out_path = alerts_dir / f"{batch_id}_alerts.json"
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info(f"✓ {len(alerts)} alerte(s) écrite(s) : {out_path}")

    return str(out_path)


def check_and_alert(metrics: Dict, config, logger_obj: Optional[logging.Logger] = None) -> List[Dict]:
    """
    Raccourci : évaluer puis dispatcher les alertes.

    Returns:
        La liste des alertes déclenchées.
    """
    alerts = evaluate_alerts(metrics, config)
    batch_id = metrics.get("batch_id", "unknown")
    dispatch_alerts(alerts, batch_id, config, logger_obj=logger_obj)
    return alerts
