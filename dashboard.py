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
        ["Last Execution", "Execution History", "KPI Summary"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.write("📁 Logs directory: `datalake/_logs/`")


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


# ============================================================================
# PAGE 3 : KPI Summary
# ============================================================================

elif page == "KPI Summary":
    st.header("🎯 KPI Summary")

    all_metrics = JobMetrics.load_all_metrics()

    if not all_metrics:
        st.info("No KPI data available.")
    else:
        # Summary stats
        col1, col2, col3, col4, col5 = st.columns(5)

        total_batches = len(all_metrics)
        avg_duration = sum(m.get("total_duration_seconds", 0) for m in all_metrics) / total_batches
        avg_quality = sum(m.get("validation", {}).get("pct_valid", 0) for m in all_metrics) / total_batches
        avg_in_flight = sum(m.get("analysis", {}).get("pct_in_flight", 0) for m in all_metrics) / total_batches
        total_errors = sum(m.get("num_errors", 0) for m in all_metrics)

        with col1:
            st.metric("Total Batches", total_batches)

        with col2:
            st.metric("Avg Duration (sec)", f"{avg_duration:.1f}")

        with col3:
            st.metric("Avg Quality %", f"{avg_quality:.1f}%")

        with col4:
            st.metric("Avg In Flight %", f"{avg_in_flight:.1f}%")

        with col5:
            st.metric("Total Errors", total_errors)

        st.markdown("---")

        # Breakdown by status
        st.subheader("Execution Status Distribution")

        statuses = {}
        for m in all_metrics:
            status = m.get("status", "unknown")
            statuses[status] = statuses.get(status, 0) + 1

        col1, col2 = st.columns([1, 1])

        with col1:
            st.bar_chart(pd.DataFrame(
                list(statuses.items()),
                columns=["Status", "Count"]
            ).set_index("Status"))

        with col2:
            st.write("**Batch Status Summary:**")
            for status, count in statuses.items():
                st.write(f"- {status}: {count} batches")

        st.markdown("---")

        # Quality distribution
        st.subheader("Quality Distribution")

        qualities = [m.get("validation", {}).get("pct_valid", 0) for m in all_metrics]

        col1, col2 = st.columns([1, 1])

        with col1:
            st.metric("Min Quality", f"{min(qualities)}%")

        with col2:
            st.metric("Max Quality", f"{max(qualities)}%")

        st.line_chart(pd.DataFrame(
            {
                "Batch": [m.get("batch_id") for m in all_metrics],
                "Quality %": qualities
            }
        ).set_index("Batch"))

        st.markdown("---")

        # Dimensions & KPIs Summary
        st.subheader("📋 Dimensions & KPIs Average")

        col1, col2, col3, col4 = st.columns(4)

        avg_airlines = sum(m.get("dimensions", {}).get("dim_airlines", {}).get("unique_count", 0) for m in all_metrics) / total_batches
        avg_airports = sum(m.get("dimensions", {}).get("dim_airports", {}).get("unique_count", 0) for m in all_metrics) / total_batches
        avg_aircraft = sum(m.get("dimensions", {}).get("dim_aircraft_models", {}).get("unique_count", 0) for m in all_metrics) / total_batches
        avg_countries = sum(m.get("dimensions", {}).get("dim_countries_continents", {}).get("unique_count", 0) for m in all_metrics) / total_batches

        with col1:
            st.metric("Avg Airlines", f"{avg_airlines:.0f}")

        with col2:
            st.metric("Avg Airports", f"{avg_airports:.0f}")

        with col3:
            st.metric("Avg Aircraft Models", f"{avg_aircraft:.0f}")

        with col4:
            st.metric("Avg Countries", f"{avg_countries:.0f}")

        st.markdown("---")

        st.subheader("🎯 KPI Results (Average Rows)")

        kpi_names = [
            "kpi_airline_volumes",
            "kpi_continental_regional",
            "kpi_longest_flight",
            "kpi_continental_avg_distance",
            "kpi_aircraft_manufacturers",
            "kpi_airline_aircraft_top3",
            "kpi_airport_imbalance",
        ]

        kpi_avgs = {}
        for kpi_name in kpi_names:
            total_rows = sum(m.get("gold", {}).get(kpi_name, {}).get("rows", 0) for m in all_metrics)
            kpi_avgs[kpi_name.replace("kpi_", "")] = total_rows / total_batches

        kpi_df = pd.DataFrame(
            list(kpi_avgs.items()),
            columns=["KPI", "Avg Rows"]
        )
        st.dataframe(kpi_df, use_container_width=True)

st.markdown("---")
st.caption("🤖 Generated with Claude Code")
