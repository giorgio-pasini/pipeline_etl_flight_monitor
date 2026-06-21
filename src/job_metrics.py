"""
Métriques simples pour le job ETL.

Collecte des métriques de base :
- Durée d'exécution du batch
- Nombre de vols extractés/validés/chargés
- Pourcentage de validité
- Nombres d'erreurs et avertissements
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


class JobMetrics:
    """Collecte des métriques simples du job."""

    def __init__(self, batch_id: str):
        """
        Initialiser les métriques.

        Args:
            batch_id: Identifiant du batch (ex: "20260621_141530")
        """
        self.batch_id = batch_id
        self.start_time = datetime.now()
        self.metrics = {
            "batch_id": batch_id,
            "start_time": self.start_time.isoformat(),
            "extraction": {"rows": 0, "duration_seconds": 0},
            "validation": {"valid_rows": 0, "invalid_rows": 0, "pct_valid": 0},
            "analysis": {
                "on_ground_count": 0,
                "in_flight_count": 0,
                "pct_in_flight": 0,
            },
            "dimensions": {
                "dim_airlines": {"unique_count": 0},
                "dim_airports": {"unique_count": 0},
                "dim_aircraft_models": {"unique_count": 0},
                "dim_countries_continents": {"unique_count": 0},
            },
            "gold": {
                "kpis_computed": 0,
                "duration_seconds": 0,
                "kpi_airline_volumes": {"rows": 0},
                "kpi_continental_regional": {"rows": 0},
                "kpi_longest_flight": {"rows": 0},
                "kpi_continental_avg_distance": {"rows": 0},
                "kpi_aircraft_manufacturers": {"rows": 0},
                "kpi_airline_aircraft_top3": {"rows": 0},
                "kpi_airport_imbalance": {"rows": 0},
            },
            "errors": [],
            "warnings": [],
        }

    def set_extraction(self, num_rows: int, duration_seconds: float):
        """Enregistrer les métriques d'extraction."""
        self.metrics["extraction"]["rows"] = num_rows
        self.metrics["extraction"]["duration_seconds"] = round(duration_seconds, 2)

    def set_validation(self, valid_rows: int, invalid_rows: int):
        """Enregistrer les métriques de validation."""
        total = valid_rows + invalid_rows
        pct_valid = (valid_rows / total * 100) if total > 0 else 0

        self.metrics["validation"]["valid_rows"] = valid_rows
        self.metrics["validation"]["invalid_rows"] = invalid_rows
        self.metrics["validation"]["pct_valid"] = round(pct_valid, 1)

    def set_analysis(self, on_ground_count: int, in_flight_count: int):
        """Enregistrer l'analyse des données (vol status)."""
        total = on_ground_count + in_flight_count
        pct_in_flight = (in_flight_count / total * 100) if total > 0 else 0

        self.metrics["analysis"]["on_ground_count"] = on_ground_count
        self.metrics["analysis"]["in_flight_count"] = in_flight_count
        self.metrics["analysis"]["pct_in_flight"] = round(pct_in_flight, 1)

    def set_dimension(self, dim_name: str, unique_count: int):
        """
        Enregistrer les métriques d'une dimension.

        Args:
            dim_name: Nom de la dimension (ex: "dim_airlines", "dim_airports")
            unique_count: Nombre d'éléments uniques
        """
        if dim_name in self.metrics["dimensions"]:
            self.metrics["dimensions"][dim_name]["unique_count"] = unique_count

    def set_kpi_result(self, kpi_name: str, num_rows: int):
        """
        Enregistrer le résultat d'un KPI.

        Args:
            kpi_name: Nom du KPI (ex: "kpi_airline_volumes")
            num_rows: Nombre de rows dans la table KPI
        """
        kpi_key = f"kpi_{kpi_name}" if not kpi_name.startswith("kpi_") else kpi_name
        if kpi_key in self.metrics["gold"]:
            self.metrics["gold"][kpi_key]["rows"] = num_rows

    def set_gold(self, num_kpis: int, duration_seconds: float):
        """Enregistrer les métriques Gold (KPIs)."""
        self.metrics["gold"]["kpis_computed"] = num_kpis
        self.metrics["gold"]["duration_seconds"] = round(duration_seconds, 2)

    def add_error(self, error_type: str, message: str, phase: str = None):
        """Ajouter une erreur."""
        self.metrics["errors"].append({
            "type": error_type,
            "message": message,
            "phase": phase,
            "timestamp": datetime.now().isoformat(),
        })

    def add_warning(self, warning_type: str, message: str):
        """Ajouter un avertissement."""
        self.metrics["warnings"].append({
            "type": warning_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })

    def finalize(self) -> Dict:
        """
        Finaliser et retourner les métriques.

        Returns:
            Dict avec toutes les métriques
        """
        end_time = datetime.now()
        self.metrics["end_time"] = end_time.isoformat()
        self.metrics["total_duration_seconds"] = round(
            (end_time - self.start_time).total_seconds(), 2
        )
        self.metrics["status"] = "success" if not self.metrics["errors"] else "warning"
        self.metrics["num_errors"] = len(self.metrics["errors"])
        self.metrics["num_warnings"] = len(self.metrics["warnings"])

        return self.metrics

    def save_to_json(self, output_path: str = None) -> str:
        """
        Sauvegarder les métriques en JSON.

        Args:
            output_path: Chemin de sortie

        Returns:
            Chemin du fichier
        """
        if output_path is None:
            output_path = f"datalake/_logs/{self.batch_id}_metrics.json"

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(self.metrics, f, indent=2, default=str)

        return output_path

    @staticmethod
    def load_from_json(json_path: str) -> Dict:
        """Charger les métriques depuis un fichier JSON."""
        with open(json_path, "r") as f:
            return json.load(f)

    def get_summary(self) -> str:
        """
        Retourner un résumé texte des métriques.

        Utile pour logging/debugging.
        """
        summary = f"""
╔═══════════════════════════════════════════════════════╗
║ BATCH METRICS SUMMARY: {self.batch_id}
╚═══════════════════════════════════════════════════════╝

📥 EXTRACTION:
  • Rows: {self.metrics['extraction']['rows']}
  • Duration: {self.metrics['extraction']['duration_seconds']}s

✅ VALIDATION:
  • Valid: {self.metrics['validation']['valid_rows']}
  • Invalid: {self.metrics['validation']['invalid_rows']}
  • Valid %: {self.metrics['validation']['pct_valid']}%

📊 DATA ANALYSIS:
  • In Flight: {self.metrics['analysis']['in_flight_count']} ({self.metrics['analysis']['pct_in_flight']}%)
  • On Ground: {self.metrics['analysis']['on_ground_count']}

📋 DIMENSIONS:
  • Airlines: {self.metrics['dimensions']['dim_airlines']['unique_count']} unique
  • Airports: {self.metrics['dimensions']['dim_airports']['unique_count']} unique
  • Aircraft Models: {self.metrics['dimensions']['dim_aircraft_models']['unique_count']} unique
  • Countries: {self.metrics['dimensions']['dim_countries_continents']['unique_count']} unique

🎯 GOLD KPIs:
  • kpi_airline_volumes: {self.metrics['gold']['kpi_airline_volumes']['rows']} rows
  • kpi_continental_regional: {self.metrics['gold']['kpi_continental_regional']['rows']} rows
  • kpi_longest_flight: {self.metrics['gold']['kpi_longest_flight']['rows']} rows
  • kpi_continental_avg_distance: {self.metrics['gold']['kpi_continental_avg_distance']['rows']} rows
  • kpi_aircraft_manufacturers: {self.metrics['gold']['kpi_aircraft_manufacturers']['rows']} rows
  • kpi_airline_aircraft_top3: {self.metrics['gold']['kpi_airline_aircraft_top3']['rows']} rows
  • kpi_airport_imbalance: {self.metrics['gold']['kpi_airport_imbalance']['rows']} rows

❌ ERRORS: {self.metrics['num_errors']}
⚠️  WARNINGS: {self.metrics['num_warnings']}

⏱️  TOTAL DURATION: {self.metrics.get('total_duration_seconds', 'N/A')}s
📊 STATUS: {self.metrics.get('status', 'unknown')}
"""
        return summary

    @staticmethod
    def load_all_metrics(logs_dir: str = "datalake/_logs") -> list:
        """
        Charger tous les fichiers de métriques.

        Returns:
            Liste de dictionnaires de métriques (triés par date décroissante)
        """
        metrics_files = sorted(
            Path(logs_dir).glob("*_metrics.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )

        all_metrics = []
        for f in metrics_files:
            try:
                with open(f, "r") as file:
                    all_metrics.append(json.load(file))
            except Exception:
                pass

        return all_metrics
