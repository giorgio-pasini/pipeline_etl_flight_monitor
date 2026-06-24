"""
DAG d'orchestration du pipeline ETL trafic aérien.

Pattern production : Airflow **orchestre**, il n'exécute pas Spark en interne.
Le batch tourne dans un conteneur isolé `flight-etl` via DockerOperator (équivalent
local du KubernetesPodOperator de prod). Le conteneur écrit ses sorties + métriques
dans le volume partagé `flight_datalake`, que le dashboard Streamlit visualise.

Planifié toutes les 2 h (exigence projet, BATCH_INTERVAL_HOURS = 2).
"""

import os
import glob
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Volume partagé (nom explicite défini dans docker-compose.yml).
DATALAKE_VOLUME = "flight_datalake"
# Chemin du volume monté DANS le conteneur Airflow (pour la tâche de contrôle).
DATALAKE_IN_AIRFLOW = "/opt/airflow/datalake"
# Accès au démon Docker via le proxy filtrant (docker-socket-proxy) : Airflow ne monte plus
# le socket brut. Surchargeable par env (ex. unix://var/run/docker.sock en debug local hors compose).
DOCKER_HOST_URL = os.getenv("DOCKER_HOST", "tcp://docker-socket-proxy:2375")

# Variables transmises au conteneur du job (secrets/tuning hérités de l'environnement Airflow).
JOB_ENV = {
    "DIM_AIRPORTS_STATIC_PATH": "/app/datalake/_reference/airports.dat",
    "SPARK_DRIVER_MEMORY": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
    "FR24_EMAIL": os.getenv("FR24_EMAIL", ""),
    "FR24_PASSWORD": os.getenv("FR24_PASSWORD", ""),
}

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_outcome(**_):
    """Échoue si le dernier batch n'a pas réussi (lecture des métriques JSON)."""
    pattern = os.path.join(DATALAKE_IN_AIRFLOW, "_logs", "*_metrics.json")
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    if not files:
        raise AirflowException(f"Aucun fichier de métriques trouvé ({pattern})")

    with open(files[-1], encoding="utf-8") as f:
        m = json.load(f)

    status, errors = m.get("status"), m.get("num_errors", 0)
    print(f"Dernier run : status={status}, num_errors={errors} ({os.path.basename(files[-1])})")
    if status != "success" or errors:
        raise AirflowException(f"Batch en échec : status={status}, num_errors={errors}")


with DAG(
    dag_id="flight_etl_pipeline",
    description="ETL trafic aérien — Bronze→Silver→Gold, toutes les 2 h",
    default_args=default_args,
    schedule="0 */2 * * *",            # toutes les 2 heures
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                  # pas de runs ETL concurrents
    tags=["etl", "flightradar", "spark"],
) as dag:

    run_etl = DockerOperator(
        task_id="run_etl",
        image="flight-etl:latest",
        command="python scripts/run_job.py --with-silver-gold",
        mounts=[Mount(source=DATALAKE_VOLUME, target="/app/datalake", type="volume")],
        environment=JOB_ENV,
        docker_url=DOCKER_HOST_URL,
        auto_remove="success",
        mount_tmp_dir=False,            # évite l'échec de montage tmp (Docker Desktop/Windows)
        network_mode="bridge",
    )

    verify = PythonOperator(
        task_id="check_outcome",
        python_callable=check_outcome,
    )

    run_etl >> verify
