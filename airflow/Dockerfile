# Image Airflow LÉGÈRE : elle orchestre seulement.
# Le pipeline Spark s'exécute dans un conteneur `flight-etl` séparé (DockerOperator),
# donc PAS besoin de Java / pyspark / code métier ici — uniquement le provider Docker.
FROM apache/airflow:2.10.4-python3.11

# Provider Docker (DockerOperator) + Postgres (backend métadonnées fiable, sans verrou SQLite).
RUN pip install --no-cache-dir \
    "apache-airflow-providers-docker==3.14.0" \
    "apache-airflow-providers-postgres==5.13.1"
