# Pipeline ETL trafic aérien — image unique (job ETL + dashboard).
# Conteneur Linux : Spark n'a PAS besoin de winutils/hadoop.dll (prérequis Windows uniquement).
FROM python:3.11-slim-bookworm

# Java 17 = runtime Spark ; procps fournit `ps` (requis par Spark en mode local).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dépendances d'abord (couche cache indépendante du code).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code applicatif (data/ et datalake/ exclus via .dockerignore).
COPY . .

# Utilisateur non-root + point de montage du volume datalake accessible en écriture.
# Docker copie la propriété de /app/datalake sur le volume nommé créé à vide.
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/datalake \
    && chown -R app:app /app
USER app

EXPOSE 8501

# Défaut : exécuter un batch complet. docker-compose surcharge par service.
CMD ["python", "scripts/run_job.py", "--with-silver-gold"]
