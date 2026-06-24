"""
Dashboard Streamlit simple pour visualiser les métriques du job ETL.

Usage:
    streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import json

from src.job_metrics import JobMetrics

try:
    from config.pipeline_config import PipelineConfig
    GOLD_DIR = PipelineConfig.GOLD_PATH
    LOG_DIR = PipelineConfig.LOG_PATH
except Exception:
    GOLD_DIR = "datalake/gold"
    LOG_DIR = "datalake/_logs"


# Phases du job (libellé, marqueur dans le log) — cf. src/batch_job.run_batch
_PHASES = [
    ("Extraction API", "Phase 1 :"),
    ("Validation & qualité", "Phase 2 :"),
    ("Profil qualité", "Phase 3 :"),
    ("Partitionnement", "Phase 4 :"),
    ("Écriture Bronze", "Phase 5 :"),
    ("Silver + Gold (KPIs)", "Phase 6 :"),
]


def _latest_batch_log():
    """Chemin du log de batch le plus récent (ou None)."""
    logs = sorted(Path(LOG_DIR).glob("batch_job_*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1] if logs else None


def _analyze_run(path: Path) -> dict:
    """Analyser un log de batch : état (running/done/stalled), phase, durée, issue."""
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [l for l in text.splitlines() if l.strip()]

    reached = -1
    for i, (_label, marker) in enumerate(_PHASES):
        if marker in text:
            reached = i

    finished = ("✓ Métriques sauvegardées" in text
                or "✅ Batch complété avec succès" in text
                or "❌ Erreur lors du batch" in text)
    success = "✅ Batch complété avec succès" in text
    empty = "Aucun vol collecté" in text

    # Horodatage de début (1re ligne) et de dernière activité (mtime)
    started = None
    try:
        started = datetime.strptime(lines[0][:23], "%Y-%m-%d %H:%M:%S,%f")
    except Exception:
        pass
    mtime = path.stat().st_mtime
    age = time.time() - mtime

    if finished:
        state = "done"
    elif age < 180:           # mise à jour < 3 min -> considéré actif
        state = "running"
    else:
        state = "stalled"

    return {
        "state": state, "reached": reached, "finished": finished,
        "success": success, "empty": empty, "started": started,
        "age": age, "mtime": mtime, "lines": lines, "name": path.name,
    }


def _read_kpi(name: str):
    """Lire une table KPI Gold (Parquet partitionné) via pandas. None si absente/vide."""
    path = Path(GOLD_DIR) / name
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    # Ne garder que le dernier snapshot calculé (la table contient un snapshot par (jour, heure) ;
    # `computed_at` identifie le calcul le plus récent).
    if "computed_at" in df.columns:
        df = df[df["computed_at"] == df["computed_at"].max()]
    # Colonnes techniques retirées de l'affichage
    return df.drop(columns=[c for c in ("computed_at", "tech_year", "tech_month", "tech_day") if c in df.columns])


st.set_page_config(
    page_title="Pipeline ETL Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

_title_col, _refresh_col = st.columns([5, 1])
_title_col.title("🚀 Pipeline ETL Dashboard")
if _refresh_col.button("🔄 Rafraîchir", use_container_width=True, key="refresh_top"):
    st.rerun()
st.markdown("---")

# ============================================================================
# Sidebar : Navigation + Filtres
# ============================================================================

with st.sidebar:
    st.header("📊 Navigation")
    page = st.radio(
        "Choisir une vue",
        ["Statut d'exécution", "KPIs (Gold)", "Dernière exécution", "Historique des exécutions"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.write("📁 Logs: `datalake/_logs/`")
    st.write(f"🏆 Gold: `{GOLD_DIR}`")


# ============================================================================
# PAGE : Statut d'exécution — run en cours / progression / résultat
# ============================================================================

if page == "Statut d'exécution":
    st.header("⏱️ Statut d'exécution")

    log_path = _latest_batch_log()
    if log_path is None:
        st.warning(
            "⚠️ Aucun log de batch trouvé. Lance d'abord le pipeline :\n\n"
            "`python scripts/run_job.py --with-silver-gold`"
        )
    else:
        run = _analyze_run(log_path)
        total_steps = len(_PHASES)
        done_steps = run["reached"] + 1  # phases atteintes

        # --- Bandeau d'état ---
        if run["state"] == "running":
            st.info(f"🟢 **Exécution en cours** — log `{run['name']}`")
            phase_label = _PHASES[run["reached"]][0] if run["reached"] >= 0 else "Initialisation"
            st.progress(min(done_steps / (total_steps + 1), 0.99),
                        text=f"Phase {max(done_steps,1)}/{total_steps} — {phase_label}")
        elif run["state"] == "done":
            if run["success"]:
                st.success(f"✅ **Dernier run réussi** — log `{run['name']}`")
            elif run["empty"]:
                st.warning(f"⚠️ **Dernier run : extraction vide** (aucun vol collecté) — `{run['name']}`")
            else:
                st.error(f"❌ **Dernier run en échec** — log `{run['name']}`")
            st.progress(1.0, text="Terminé")
        else:  # stalled
            st.warning(
                f"🟠 **Run possiblement interrompu** — aucune mise à jour depuis "
                f"{run['age']/60:.0f} min (log `{run['name']}`). Process arrêté/crashé ?"
            )
            phase_label = _PHASES[run["reached"]][0] if run["reached"] >= 0 else "?"
            st.progress(min(done_steps / (total_steps + 1), 0.99),
                        text=f"Bloqué après : {phase_label}")

        # --- Métriques de timing ---
        c1, c2, c3 = st.columns(3)
        if run["started"]:
            c1.metric("Démarré à", run["started"].strftime("%H:%M:%S"))
            ref = time.time() if run["state"] == "running" else run["mtime"]
            c2.metric("Durée", f"{(ref - run['started'].timestamp()):.0f} s")
        c3.metric("Phases atteintes", f"{max(done_steps,0)}/{total_steps}")

        # --- Détail des phases ---
        st.subheader("Progression par phase")
        for i, (label, _m) in enumerate(_PHASES):
            if i < run["reached"] or (run["state"] == "done" and run["success"]):
                icon = "✅"
            elif i == run["reached"]:
                icon = "🟢" if run["state"] == "running" else ("✅" if run["success"] else "⏹️")
            else:
                icon = "⚪"
            st.write(f"{icon} Phase {i+1} — {label}")

        # --- Résultat (si terminé) : métriques du dernier run ---
        if run["state"] == "done":
            st.subheader("Résultat")
            all_metrics = JobMetrics.load_all_metrics()
            if all_metrics:
                m = all_metrics[0]
                d1, d2, d3 = st.columns(3)
                d1.metric("Statut", m.get("status", "?"))
                d2.metric("Erreurs", m.get("num_errors", 0))
                d3.metric("Durée totale", f"{m.get('total_duration_seconds', 0):.0f} s")
                errs = m.get("errors") or []
                if errs:
                    with st.expander(f"❌ Détail des {len(errs)} erreur(s)"):
                        for e in errs:
                            st.code(f"[{e.get('phase','?')}] {e.get('message','')[:1500]}")

        # --- Tail du log ---
        st.subheader("Journal (50 dernières lignes)")
        st.code("\n".join(run["lines"][-50:]) or "(vide)", language="log")

        # --- Rafraîchissement ---
        st.markdown("---")
        cols = st.columns([1, 3])
        if cols[0].button("🔄 Rafraîchir", key="refresh_status"):
            st.rerun()
        if run["state"] == "running":
            auto = cols[1].checkbox("Auto-refresh (5 s)", value=True)
            if auto:
                time.sleep(5)
                st.rerun()


# ============================================================================
# PAGE 0 : KPIs (Gold) — valeurs métier calculées
# ============================================================================

if page == "KPIs (Gold)":
    st.header("🏆 KPIs calculés (couche Gold)")

    if not Path(GOLD_DIR).exists():
        st.warning(
            "⚠️ Aucune donnée Gold trouvée. Lance d'abord le pipeline :\n\n"
            "`python scripts/run_job.py --with-silver-gold`"
        )
    else:
        # --- KPI 1 : Compagnie la plus active ---
        st.subheader("1 · Compagnie avec le plus de vols en cours")
        df = _read_kpi("kpi_airline_volumes")
        if df is not None:
            top = df.iloc[0]
            c1, c2 = st.columns([1, 2])
            c1.metric(
                top.get("airline_name") or top.get("airline_icao", "?"),
                int(top.get("active_flights_count", 0)),
                "vols en cours",
            )
            c2.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI 2 : Top compagnie régionale par continent ---
        st.subheader("2 · Top compagnie régionale par continent")
        df = _read_kpi("kpi_continental_regional")
        if df is not None:
            c1, c2 = st.columns([2, 1])
            c1.dataframe(df, use_container_width=True, hide_index=True)
            if {"origin_continent", "regional_flights_count"}.issubset(df.columns):
                c2.bar_chart(df.set_index("origin_continent")["regional_flights_count"])
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI 3 : Vol en cours le plus long ---
        st.subheader("3 · Vol en cours au trajet le plus long")
        df = _read_kpi("kpi_longest_flight")
        if df is not None:
            r = df.iloc[0]
            route = f"{r.get('origin_iata','?')} → {r.get('destination_iata','?')}"
            c1, c2 = st.columns([1, 2])
            c1.metric(route, f"{float(r.get('distance_km', 0)):,.0f} km",
                      r.get("airline_name") or r.get("callsign", ""))
            c2.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI 4 : Distance moyenne par continent ---
        st.subheader("4 · Distance de vol moyenne par continent")
        df = _read_kpi("kpi_continental_avg_distance")
        if df is not None:
            c1, c2 = st.columns([2, 1])
            c1.dataframe(df, use_container_width=True, hide_index=True)
            if {"origin_continent", "avg_distance_km"}.issubset(df.columns):
                c2.bar_chart(df.set_index("origin_continent")["avg_distance_km"])
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI 5 : Constructeur le plus actif ---
        st.subheader("5 · Constructeur d'avions le plus actif")
        df = _read_kpi("kpi_aircraft_manufacturers")
        if df is not None:
            top = df.iloc[0]
            c1, c2 = st.columns([1, 2])
            c1.metric(top.get("manufacturer", "?"), int(top.get("active_flights_count", 0)), "vols actifs")
            c2.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI 6 : Top 3 modèles par pays de compagnie ---
        st.subheader("6 · Top 3 des modèles d'avion par pays")
        df = _read_kpi("kpi_airline_aircraft_top3")
        if df is not None:
            country_col = "origin_airport_country_code"
            if country_col in df.columns:
                countries = sorted(df[country_col].dropna().unique().tolist())
                default_ix = countries.index("US") if "US" in countries else 0
                sel = st.selectbox("Pays", countries, index=default_ix)
                st.dataframe(
                    df[df[country_col] == sel].sort_values("rank"),
                    use_container_width=True, hide_index=True,
                )
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("KPI non disponible — lancer le pipeline.")

        # --- KPI bonus : Aéroport au plus grand écart départs/arrivées ---
        st.subheader("Bonus · Aéroport au plus grand écart départs/arrivées")
        df = _read_kpi("kpi_airport_imbalance")
        if df is not None:
            r = df.iloc[0]
            c1, c2 = st.columns([1, 2])
            c1.metric(r.get("airport_iata", "?"), int(r.get("imbalance", 0)),
                      f"{int(r.get('departures', 0))} dép / {int(r.get('arrivals', 0))} arr")
            c2.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("KPI non disponible — lancer le pipeline.")


# ============================================================================
# PAGE 1 : Dernière exécution
# ============================================================================

if page == "Dernière exécution":
    st.header("📍 Dernière exécution")

    # Charger les métriques les plus récentes
    all_metrics = JobMetrics.load_all_metrics()

    if not all_metrics:
        st.warning("⚠️ Aucune métrique d'exécution trouvée. Lancez d'abord le pipeline.")
    else:
        latest = all_metrics[0]

        # Indicateurs principaux
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Durée (s)",
                latest.get("total_duration_seconds", "N/A"),
                delta="Cible : < 600 s"
            )

        with col2:
            pct_valid = latest.get("validation", {}).get("pct_valid", 0)
            st.metric(
                "Qualité des données",
                f"{pct_valid}%",
                delta="Cible : >= 70%"
            )

        with col3:
            num_errors = latest.get("num_errors", 0)
            st.metric(
                "Erreurs",
                num_errors,
                delta="✅ Sain" if num_errors == 0 else "⚠️ Voir les logs"
            )

        with col4:
            num_warnings = latest.get("num_warnings", 0)
            st.metric(
                "Avertissements",
                num_warnings
            )

        st.markdown("---")

        # Extraction
        st.subheader("📥 Extraction")
        col1, col2 = st.columns(2)

        with col1:
            extraction = latest.get("extraction", {})
            st.metric(
                "Vols extraits",
                extraction.get("rows", 0)
            )

        with col2:
            st.metric(
                "Durée (s)",
                extraction.get("duration_seconds", 0)
            )

        st.markdown("---")

        # Validation
        st.subheader("✅ Validation")
        col1, col2, col3 = st.columns(3)

        validation = latest.get("validation", {})

        with col1:
            st.metric("Lignes valides", validation.get("valid_rows", 0))

        with col2:
            st.metric("Lignes invalides", validation.get("invalid_rows", 0))

        with col3:
            st.metric("% valides", f"{validation.get('pct_valid', 0)}%")

        st.markdown("---")

        # Analyse des données
        st.subheader("📊 Analyse des données")
        col1, col2, col3 = st.columns(3)

        analysis = latest.get("analysis", {})

        with col1:
            st.metric("En vol", analysis.get("in_flight_count", 0))

        with col2:
            st.metric("Au sol", analysis.get("on_ground_count", 0))

        with col3:
            st.metric("% en vol", f"{analysis.get('pct_in_flight', 0)}%")

        st.markdown("---")

        # Dimensions
        st.subheader("📋 Dimensions (valeurs uniques)")
        col1, col2, col3, col4 = st.columns(4)

        dims = latest.get("dimensions", {})

        with col1:
            st.metric("Compagnies", dims.get("dim_airlines", {}).get("unique_count", 0))

        with col2:
            st.metric("Aéroports", dims.get("dim_airports", {}).get("unique_count", 0))

        with col3:
            st.metric("Modèles d'avion", dims.get("dim_aircraft_models", {}).get("unique_count", 0))

        with col4:
            st.metric("Pays", dims.get("dim_countries_continents", {}).get("unique_count", 0))

        st.markdown("---")

        # Gold / KPIs
        st.subheader("🎯 Gold (KPIs)")
        col1, col2 = st.columns(2)

        gold = latest.get("gold", {})

        with col1:
            st.metric("KPIs calculés", gold.get("kpis_computed", 0))

        with col2:
            st.metric("Durée (s)", gold.get("duration_seconds", 0))

        # Résultats des KPIs
        st.write("**Résultats des KPIs (lignes) :**")
        kpi_cols = st.columns(3)
        kpi_names = [
            "kpi_airline_volumes",
            "kpi_continental_regional",
            "kpi_longest_flight",
            "kpi_continental_avg_distance",
            "kpi_aircraft_manufacturers",
            "kpi_airline_aircraft_top3",
            "kpi_airport_imbalance",
        ]

        for idx, kpi_name in enumerate(kpi_names):
            col = kpi_cols[idx % 3]
            kpi_rows = gold.get(kpi_name, {}).get("rows", 0)
            col.metric(kpi_name.replace("kpi_", "").replace("_", " ").title(), kpi_rows)

        st.markdown("---")

        # Erreurs & avertissements
        errors = latest.get("errors", [])
        warnings = latest.get("warnings", [])

        if errors:
            st.subheader("❌ Erreurs")
            for error in errors:
                with st.expander(f"{error['type']} ({error.get('phase', 'N/A')})"):
                    st.write(f"**Message :** {error['message']}")
                    st.write(f"**Heure :** {error['timestamp']}")

        if warnings:
            st.subheader("⚠️ Avertissements")
            for warning in warnings:
                with st.expander(f"{warning['type']}"):
                    st.write(f"**Message :** {warning['message']}")
                    st.write(f"**Heure :** {warning['timestamp']}")

        st.markdown("---")

        # Métadonnées
        st.subheader("ℹ️ Métadonnées")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"**Batch ID :** {latest.get('batch_id')}")

        with col2:
            st.write(f"**Statut :** {latest.get('status')}")

        with col3:
            st.write(f"**Terminé :** {latest.get('end_time')}")


# ============================================================================
# PAGE 2 : Historique des exécutions
# ============================================================================

elif page == "Historique des exécutions":
    st.header("📜 Historique des exécutions")

    all_metrics = JobMetrics.load_all_metrics()

    if not all_metrics:
        st.info("Aucun historique d'exécution disponible.")
    else:
        # Construire le dataframe
        data = []
        for m in all_metrics:
            dims = m.get("dimensions", {})
            analysis = m.get("analysis", {})
            data.append({
                "Batch ID": m.get("batch_id"),
                "Statut": m.get("status"),
                "Durée (s)": m.get("total_duration_seconds"),
                "Vols": m.get("extraction", {}).get("rows", 0),
                "% valides": m.get("validation", {}).get("pct_valid", 0),
                "% en vol": analysis.get("pct_in_flight", 0),
                "Compagnies": dims.get("dim_airlines", {}).get("unique_count", 0),
                "Aéroports": dims.get("dim_airports", {}).get("unique_count", 0),
                "Erreurs": m.get("num_errors", 0),
                "Avertissements": m.get("num_warnings", 0),
                "Heure": m.get("end_time"),
            })

        df = pd.DataFrame(data)

        # Afficher le tableau
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "Statut": st.column_config.TextColumn(width="small"),
                "Durée (s)": st.column_config.NumberColumn(width="small"),
                "% valides": st.column_config.NumberColumn(width="small"),
                "Erreurs": st.column_config.NumberColumn(width="small"),
                "Avertissements": st.column_config.NumberColumn(width="small"),
            }
        )

        # Graphiques
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Tendance de la durée")
            st.line_chart(df.set_index("Batch ID")["Durée (s)"])

        with col2:
            st.subheader("Tendance de la qualité des données")
            st.line_chart(df.set_index("Batch ID")["% valides"])

        # Téléchargement
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Télécharger en CSV",
            data=csv,
            file_name=f"historique_executions_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )


st.markdown("---")
