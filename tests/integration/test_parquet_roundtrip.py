"""Test d'intégration : round-trip Parquet partitionné (Bronze).

Verrouille la régression de partitionnement corrigée à l'étape 8 : les colonnes
tech_year/tech_month/tech_day/tech_hour doivent être écrites puis relues avec les
bonnes valeurs, et le partition-pruning doit fonctionner.

Nécessite l'écriture Parquet (HADOOP_HOME/winutils sous Windows) -> skip sinon.
"""

import pytest
from datetime import datetime
from pyspark.sql.functions import lit, col

from src.datalake_utils import get_partition_values
from config.pipeline_config import PARTITION_COLUMNS_BRONZE


def test_bronze_partition_roundtrip(spark_session, temp_datalake, parquet_write_supported):
    if not parquet_write_supported:
        pytest.skip("Écriture Parquet indisponible (HADOOP_HOME/winutils requis sous Windows)")

    df = spark_session.createDataFrame(
        [("F1", "DAL"), ("F2", "AFR")],
        "flight_id STRING, airline_icao STRING",
    )

    parts = get_partition_values(datetime(2026, 6, 21, 14, 30, 0))
    for k, v in parts.items():
        df = df.withColumn(k, lit(v))

    path = str(temp_datalake / "bronze" / "flights_raw")
    df.write.mode("append").partitionBy(*PARTITION_COLUMNS_BRONZE).parquet(path)

    back = spark_session.read.parquet(path)

    # Colonnes de partition restaurées à la lecture
    for k in PARTITION_COLUMNS_BRONZE:
        assert k in back.columns

    # Valeurs correctes (et non un timestamp epoch comme dans le bug d'origine)
    rows = back.collect()
    assert all(r["tech_year"] == "2026" for r in rows)
    assert all(r["tech_month"] == "2026-06" for r in rows)
    assert all(r["tech_day"] == "2026-06-21" for r in rows)
    assert all(r["tech_hour"] == "14" for r in rows)

    # Partition pruning : filtrer par partition existante / inexistante
    assert back.filter(col("tech_day") == "2026-06-21").count() == 2
    assert back.filter(col("tech_day") == "2020-01-01").count() == 0
