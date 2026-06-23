"""
Dashboard Streamlit simple pour visualiser les métriques du job ETL.

Usage:
    streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

from src.job_metrics import JobMetrics

try:
    from config.datalake_config import DatalakeConfig
    GOLD_DIR = DatalakeConfig.GOLD_PATH
except Exception:
    GOLD_DIR = "datalake/gold"


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
    # Colonnes techniques retirées de l'affichage
    return df.drop(columns=[c for c in ("computed_at", "tech_year", "tech_month", "tech_day") if c in df.columns])


st.set_page_config(
    page_title="Pipeline ETL Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚀 Pipeline ETL Dashboard")
st.markdown("---")

# ============================================================================
# Sidebar : Navigation + Filtres
# ============================================================================

with st.sidebar:
    st.header("📊 Navigation")
    page = st.radio(
        "Select view",
        ["KPIs (Gold)", "Last Execution", "Execution History"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.write("📁 Logs: `datalake/_logs/`")
    st.write(f"🏆 Gold: `{GOLD_DIR}`")


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
# PAGE 1 : Last Execution
# ============================================================================

if page == "Last Execution":
    st.header("📍 Last Execution")

    # Load latest metrics
    all_metrics = JobMetrics.load_all_metrics()

    if not all_metrics:
        st.warning("⚠️ No execution metrics found. Run the pipeline first.")
    else:
        latest = all_metrics[0]

        # Main KPIs
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Duration (sec)",
                latest.get("total_duration_seconds", "N/A"),
                delta="Target: < 600s"
            )

        with col2:
            pct_valid = latest.get("validation", {}).get("pct_valid", 0)
            st.metric(
                "Data Quality",
                f"{pct_valid}%",
                delta="Target: >= 70%"
            )

        with col3:
            num_errors = latest.get("num_errors", 0)
            st.metric(
                "Errors",
                num_errors,
                delta="✅ Healthy" if num_errors == 0 else "⚠️ Check logs"
            )

        with col4:
            num_warnings = latest.get("num_warnings", 0)
            st.metric(
                "Warnings",
                num_warnings
            )

        st.markdown("---")

        # Extraction
        st.subheader("📥 Extraction")
        col1, col2 = st.columns(2)

        with col1:
            extraction = latest.get("extraction", {})
            st.metric(
                "Flights Extracted",
                extraction.get("rows", 0)
            )

        with col2:
            st.metric(
                "Duration (sec)",
                extraction.get("duration_seconds", 0)
            )

        st.markdown("---")

        # Validation
        st.subheader("✅ Validation")
        col1, col2, col3 = st.columns(3)

        validation = latest.get("validation", {})

        with col1:
            st.metric("Valid Rows", validation.get("valid_rows", 0))

        with col2:
            st.metric("Invalid Rows", validation.get("invalid_rows", 0))

        with col3:
            st.metric("Valid %", f"{validation.get('pct_valid', 0)}%")

        st.markdown("---")

        # Data Analysis
        st.subheader("📊 Data Analysis")
        col1, col2, col3 = st.columns(3)

        analysis = latest.get("analysis", {})

        with col1:
            st.metric("In Flight", analysis.get("in_flight_count", 0))

        with col2:
            st.metric("On Ground", analysis.get("on_ground_count", 0))

        with col3:
            st.metric("In Flight %", f"{analysis.get('pct_in_flight', 0)}%")

        st.markdown("---")

        # Dimensions
        st.subheader("📋 Dimensions (Unique Values)")
        col1, col2, col3, col4 = st.columns(4)

        dims = latest.get("dimensions", {})

        with col1:
            st.metric("Airlines", dims.get("dim_airlines", {}).get("unique_count", 0))

        with col2:
            st.metric("Airports", dims.get("dim_airports", {}).get("unique_count", 0))

        with col3:
            st.metric("Aircraft Models", dims.get("dim_aircraft_models", {}).get("unique_count", 0))

        with col4:
            st.metric("Countries", dims.get("dim_countries_continents", {}).get("unique_count", 0))

        st.markdown("---")

        # Gold/KPIs
        st.subheader("🎯 Gold (KPIs)")
        col1, col2 = st.columns(2)

        gold = latest.get("gold", {})

        with col1:
            st.metric("KPIs Computed", gold.get("kpis_computed", 0))

        with col2:
            st.metric("Duration (sec)", gold.get("duration_seconds", 0))

        # KPI results
        st.write("**KPI Results (rows):**")
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

        # Errors & Warnings
        errors = latest.get("errors", [])
        warnings = latest.get("warnings", [])

        if errors:
            st.subheader("❌ Errors")
            for error in errors:
                with st.expander(f"{error['type']} ({error.get('phase', 'N/A')})"):
                    st.write(f"**Message:** {error['message']}")
                    st.write(f"**Time:** {error['timestamp']}")

        if warnings:
            st.subheader("⚠️ Warnings")
            for warning in warnings:
                with st.expander(f"{warning['type']}"):
                    st.write(f"**Message:** {warning['message']}")
                    st.write(f"**Time:** {warning['timestamp']}")

        st.markdown("---")

        # Metadata
        st.subheader("ℹ️ Metadata")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"**Batch ID:** {latest.get('batch_id')}")

        with col2:
            st.write(f"**Status:** {latest.get('status')}")

        with col3:
            st.write(f"**Finished:** {latest.get('end_time')}")


# ============================================================================
# PAGE 2 : Execution History
# ============================================================================

elif page == "Execution History":
    st.header("📜 Execution History")

    all_metrics = JobMetrics.load_all_metrics()

    if not all_metrics:
        st.info("No execution history available.")
    else:
        # Create dataframe
        data = []
        for m in all_metrics:
            dims = m.get("dimensions", {})
            analysis = m.get("analysis", {})
            data.append({
                "Batch ID": m.get("batch_id"),
                "Status": m.get("status"),
                "Duration (sec)": m.get("total_duration_seconds"),
                "Flights": m.get("extraction", {}).get("rows", 0),
                "Valid %": m.get("validation", {}).get("pct_valid", 0),
                "In Flight %": analysis.get("pct_in_flight", 0),
                "Airlines": dims.get("dim_airlines", {}).get("unique_count", 0),
                "Airports": dims.get("dim_airports", {}).get("unique_count", 0),
                "Errors": m.get("num_errors", 0),
                "Warnings": m.get("num_warnings", 0),
                "Time": m.get("end_time"),
            })

        df = pd.DataFrame(data)

        # Display table
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "Status": st.column_config.TextColumn(width="small"),
                "Duration (sec)": st.column_config.NumberColumn(width="small"),
                "Valid %": st.column_config.NumberColumn(width="small"),
                "Errors": st.column_config.NumberColumn(width="small"),
                "Warnings": st.column_config.NumberColumn(width="small"),
            }
        )

        # Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Duration Trend")
            st.line_chart(df.set_index("Batch ID")["Duration (sec)"])

        with col2:
            st.subheader("Data Quality Trend")
            st.line_chart(df.set_index("Batch ID")["Valid %"])

        # Download
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"execution_history_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )


st.markdown("---")
