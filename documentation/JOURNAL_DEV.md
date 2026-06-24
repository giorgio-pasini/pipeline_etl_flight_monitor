# Journal de développement

Synthèse chronologique des étapes qui ont mené le projet à son état actuel,
reconstruite depuis l'historique git (**35 commits, du 18 au 24 juin 2026**).
L'objectif n'est pas de redocumenter le système — l'état courant est décrit dans
[DOCUMENTATION.md](DOCUMENTATION.md) et le [README](../README.md) — mais d'expliquer
**le pourquoi de chaque grande décision** et l'ordre dans lequel les briques ont été
posées.

Chaque phase suit la même trame : *contexte → décisions → résultat*. La table de
référence commit par commit se trouve en [annexe](#annexe--table-chronologique-des-commits).

---

## Phase 0 — Exploration de l'API (18 juin)

**Contexte.** Avant toute architecture, il fallait comprendre ce que l'API
FlightRadar24 renvoie réellement : format des zones, champs disponibles, coût des
appels de détail, structure des vols en cours.

**Décisions.** Travail exploratoire dans un notebook (`notebook_exploration.ipynb`),
consigné dans une documentation de découverte, puis premier jet du plan
d'implémentation.

**Résultat.** Une vision claire de la donnée source et un plan initial — base des choix
d'architecture de la phase suivante.

> Commits : `7a7d0f8`, `9fd7dcf`.

---

## Phase 1 — Cadrage & choix d'architecture (21 juin)

**Contexte.** Le sujet demande un traitement régulier (toutes les 2 h) du trafic
mondial, scalable et tolérant aux pannes. La première intuition — du *Structured
Streaming* — a été remise en question.

**Décision pivot.** Abandon du Structured Streaming au profit de **Spark Core en
batch** (`78191ab`, *« Refactor: Change from Structured Streaming to Spark Core
Batch »*). Le besoin réel est un **batch ponctuel toutes les 2 h**, pas un flux
continu : le batch est plus simple, plus testable, et reste scalable + tolérant aux
pannes. Cette décision conditionne toute la suite.

**Décisions.** Formalisation du plan et d'un diagramme technique (`8b4a368`), puis pose
du **squelette complet** du projet : `config/`, `src/` (batch_job, flight_extraction,
data_quality, datalake_utils, schemas), `scripts/` (init_datalake, purge), tests,
`requirements.txt`.

**Résultat.** Une fondation orientée **architecture Medallion** (Bronze → Silver →
Gold) en Spark batch, prête à être remplie.

> Commits : `8b4a368`, `78191ab`.

---

## Phase 2 — Pipeline batch Medallion (21 juin)

**Contexte.** Construire le pipeline couche par couche, en posant la couche de tests
tôt pour sécuriser chaque ajout.

**Décisions & résultat.**
- **Bronze** : extraction API → écriture brute partitionnée (`8d967e6`).
- **Couche de tests** d'emblée : unit / integration / e2e + `conftest.py` (`4b958fb`).
- **Silver & Gold** : `transformations.py` (nettoyage, enrichissement, KPIs),
  `silver_gold_loader.py`, optimisation du partitionnement temporel (`386d803`).
- **Observabilité** : monitoring/logging, `job_metrics.py` (métriques JSON) et première
  version du **dashboard Streamlit** (`e6b6e59`).
- **Lancement** : `run_job.py` et primitives d'ordonnancement (`e577f2b`).

> Commits : `8d967e6`, `4b958fb`, `386d803`, `e6b6e59`, `e577f2b`.

---

## Phase 3 — Consolidation & fiabilité (21-22 juin)

**Contexte.** Le pipeline fonctionne de bout en bout ; place au durcissement et au
nettoyage des dettes accumulées.

**Décisions.** Corrections de fond et **suppression définitive de `streaming_job.py`**
(héritage de l'approche abandonnée), ajout de `reference_data.py` (pays→continent,
avion→constructeur) (`3b95a1c`). Complétion de la couverture de tests (`6107cd5`). Ajout
de l'**alerting** et de tests de **tolérance aux pannes** (`c5be79e`).

**Résultat.** Première implémentation considérée comme complète et testée.

> Commits : `3b95a1c`, `6107cd5`, `c5be79e`.

---

## Phase 4 — Couverture mondiale & anti-quota (22 juin)

**Contexte.** Passer d'une zone à une **couverture mondiale** (top zones) a fait
exploser le nombre d'appels API → **blocage par les quotas / rate-limit** de
FlightRadar24 (`2e57cd5`). C'est le principal obstacle technique du projet.

**Décisions.** Mise en place de **stratégies anti-quota** (`e0aed5a`) :
- **Enrichissement par dimensions *bulk*** au lieu d'un `get_flight_details` par vol
  (1 appel groupé au lieu de N) → introduction de `dimension_loader.py`.
- **Jeu d'aéroports statique OpenFlights** auto-téléchargé/rafraîchi
  (`data/airports.dat`, `3880e45`) → **zéro quota API** pour le référentiel aéroports et
  résultats reproductibles.

**Résultat.** Couverture mondiale tenable sans se faire bloquer par l'API ; base de
données de référence fiable et déterministe.

> Commits : `e060e82`, `2e57cd5`, `e0aed5a`, `3880e45`.

---

## Phase 5 — Dashboard & corrections datalake (23 juin)

**Contexte.** Rendre le pipeline observable et corriger les incohérences de structure
du datalake apparues à l'usage.

**Décisions.** Dashboard complété (suivi des runs + KPIs) (`4bb64b1`) ; correction de la
structure du datalake et des chemins de partitions (`9dbf1bf`).

> Commits : `4bb64b1`, `9dbf1bf`.

---

## Phase 6 — Grande simplification (23 juin)

**Contexte.** Le projet avait accumulé une documentation éparpillée (une quinzaine de
fichiers `.md` à la racine) et des modules trop ambitieux par rapport au besoin réel.

**Décisions.**
- **Documentation simplifiée** (`efa9730`) : suppression de ~15 fichiers `.md` dispersés
  (`LOGGING.md`, `PARTITIONING.md`, `SCHEDULING.md`, `TESTS.md`, etc.) au profit d'un
  **`DOCUMENTATION.md` unique**.
- **Suppression des parties trop lourdes** (`afb0bc8`) : `partitioning_optimizer.py`,
  `spark_tuning.py`, `profile_partitions.py` — sur-ingénierie au regard du besoin.
- Corrections de bugs et amélioration du suivi depuis le dashboard, fiabilisation de
  l'exécution manuelle (`e403e6d`, `f65fc9b`).

**Résultat.** Un projet nettement plus lisible et resserré sur l'essentiel — fil rouge
qui se poursuivra jusqu'à la fin.

> Commits : `efa9730`, `afb0bc8`, `e403e6d`, `f65fc9b`.

---

## Phase 7 — Conteneurisation & orchestration (23 juin)

**Contexte.** Rendre le projet exécutable sans prérequis (ni Java, ni winutils) et
l'orchestrer toutes les 2 h.

**Décisions.**
- **Dockerisation** : `Dockerfile` (image `flight-etl`), `docker-compose.yml`,
  `.env.example` (`7146a8a`, `abe594a`).
- **Orchestration Airflow** (`daea603`) : `airflow/Dockerfile` et
  `dags/flight_etl_dag.py` planifié `0 */2 * * *`. Choix d'un **Airflow qui orchestre
  sans exécuter** — le job tourne isolé dans un conteneur via `DockerOperator`
  (équivalent local fidèle du `KubernetesPodOperator` de prod).

**Résultat.** Première phase complète : un `docker compose up` lance le pipeline + le
dashboard ; le profil `airflow` ajoute l'ordonnancement.

> Commits : `7146a8a`, `abe594a`, `daea603`.

---

## Phase 8 — Finitions & durcissement (24 juin)

**Contexte.** Dernière passe : robustesse, sécurité, rationalisation des tests et de la
documentation pour coller au plus près du besoin.

**Décisions.**
- Correction des **credentials Airflow** (`08ea91c`).
- Poursuite du nettoyage : `__init__.py` allégés (`a3cadab`), suppression des scripts
  `schedule_job.*` et allègement de `schemas.py` / `init_datalake.py` (`8dd3f3b`).
- **Optimisations Spark** (`88596a8`).
- **Durcissement de l'idempotence et de la solidité** (`fd2dcc4`) : overwrite par
  partition + déduplication, validé par tests.
- **Rationalisation des tests** (`017e1de`, `f796ae7`) : réduction du nombre de tests
  vers l'essentiel et correction des tests skippés ; **renommage**
  `config/datalake_config.py` → `config/pipeline_config.py`.
- Ajustements aux besoins du sujet (`3d282b8`).
- **Sécurité du socket Docker** (`aec365e`) : introduction de `docker-socket-proxy`
  (réseau interne, non-root, API minimale) pour neutraliser la faille classique du
  `docker.sock` exposé à Airflow.
- **README** raccourci et déduplicé + nettoyage des derniers fichiers d'exploration
  (`9794f56`, `efa3923`, `8d43c36`).

**Résultat.** L'état actuel du projet : pipeline ETL batch complet, dockerisé, orchestré,
sécurisé, testé et documenté.

> Commits : `08ea91c`, `a3cadab`, `8dd3f3b`, `88596a8`, `fd2dcc4`, `017e1de`, `f796ae7`,
> `3d282b8`, `aec365e`, `9794f56`, `efa3923`, `8d43c36`.

---

## Fil rouge des décisions

Quatre constantes traversent tout l'historique :

1. **Coller au besoin réel, pas au buzzword** — le pivot streaming → batch (Phase 1) et
   les suppressions de la Phase 6 montrent une volonté répétée de retirer la
   sur-ingénierie dès qu'elle n'est pas justifiée par le sujet (batch toutes les 2 h).
2. **Anti-quota comme contrainte de conception** — l'enrichissement *bulk* et le
   référentiel aéroports statique (Phase 4) découlent directement d'un blocage
   rencontré, pas d'une optimisation théorique.
3. **Rejouabilité & idempotence** — l'architecture Medallion (Bronze rejouable) et
   l'overwrite par partition (Phase 8) garantissent qu'un re-run remplace au lieu
   d'empiler.
4. **Simplification continue** — la documentation et le code n'ont cessé d'être
   resserrés (Phases 6 et 8), jusqu'au README final déduplicé.

---

## Annexe — Table chronologique des commits

| Date | Commit | Résumé | Portée (fichiers-clés) |
|---|---|---|---|
| 18/06 | `7a7d0f8` | Première itération d'exploration | `notebook_exploration.ipynb`, `documentation.md` |
| 18/06 | `9fd7dcf` | Exploration et plan faits | `documentation_decouverte_api.md`, `plan_de_implementation.md` |
| 21/06 | `8b4a368` | Préparation production, création du plan | `plan_de_implementation.md`, `diagramme_technique_exalt.drawio` |
| 21/06 | `78191ab` | **Pivot : Structured Streaming → Spark Core Batch** | scaffolding `src/`, `config/`, `scripts/`, `README.md` |
| 21/06 | `8d967e6` | 1re phase dev, batch jusqu'à Bronze | déplacement des `.md` vers `documentation/` |
| 21/06 | `4b958fb` | Implémentation de la couche de test | `tests/` (unit/integration/e2e), `conftest.py`, `pytest.ini` |
| 21/06 | `386d803` | Avancement jusqu'à l'optimisation | `transformations.py`, `silver_gold_loader.py`, partitionnement |
| 21/06 | `e6b6e59` | Ajout monitoring et logging | `dashboard.py`, `job_metrics.py` |
| 21/06 | `e577f2b` | Étape 7 (scheduling) | `scripts/run_job.py`, `schedule_job.*` |
| 21/06 | `3b95a1c` | Correction des erreurs passées | suppr. `streaming_job.py`, ajout `reference_data.py` |
| 22/06 | `6107cd5` | Tests restants pour la fonctionnalité | `test_parquet_roundtrip.py`, `test_silver_gold_loader.py` |
| 22/06 | `c5be79e` | Première implémentation terminée | `alerting.py`, `test_fault_tolerance.py` |
| 22/06 | `e060e82` | Extension vers le top 8 zones | `batch_job.py`, `flight_extraction.py` |
| 22/06 | `2e57cd5` | Partie finalisée, **bloqué par les quotas** | `flight_extraction.py` |
| 22/06 | `e0aed5a` | **Stratégies anti-quota** | `dimension_loader.py`, enrichissement bulk |
| 22/06 | `3880e45` | Dév. de base terminé | `data/airports.dat` (OpenFlights) |
| 23/06 | `4bb64b1` | Premier passage job Spark + dashboard | `dashboard.py` |
| 23/06 | `9dbf1bf` | Correction structure datalake | `silver_gold_loader.py`, `config/` |
| 23/06 | `efa9730` | **Documentation simplifiée** | −15 `.md` → `DOCUMENTATION.md` |
| 23/06 | `afb0bc8` | Suppression des parties trop lourdes | suppr. `partitioning_optimizer`, `spark_tuning`, `profile_partitions` |
| 23/06 | `e403e6d` | Nettoyage, bug fix, suivi dashboard | `dashboard.py`, `batch_job.py` |
| 23/06 | `f65fc9b` | Résolution exécution manuelle | `batch_job.py` |
| 23/06 | `7146a8a` | Implémentation Docker | `Dockerfile`, `docker-compose.yml`, `.env.example` |
| 23/06 | `abe594a` | Fin de la dockerisation | `transformations.py`, tests |
| 23/06 | `daea603` | **Première phase complète (Airflow)** | `airflow/Dockerfile`, `dags/flight_etl_dag.py` |
| 24/06 | `08ea91c` | Correction credentials Airflow | `.env.example`, `docker-compose.yml` |
| 24/06 | `a3cadab` | Nettoyage `__init__.py` | `src/__init__.py` |
| 24/06 | `8dd3f3b` | Simplification (suppr. `schedule_job.*`) | `init_datalake.py`, `schemas.py` |
| 24/06 | `88596a8` | Optimisations Spark | `config/`, `batch_job.py` |
| 24/06 | `fd2dcc4` | **Failles idempotence & solidité corrigées** | `silver_gold_loader.py`, `batch_job.py`, tests |
| 24/06 | `017e1de` | Optimisation du nombre/typologie des tests | `tests/` (−273 lignes) |
| 24/06 | `f796ae7` | Correction des tests skippés + renommage config | `datalake_config.py` → `pipeline_config.py` |
| 24/06 | `3d282b8` | Mise à jour pour mieux coller aux besoins | `README.md`, `DOCUMENTATION.md`, `silver_gold_loader.py` |
| 24/06 | `aec365e` | **Sécurisation `docker.sock`** | `docker-compose.yml`, `flight_etl_dag.py` |
| 24/06 | `9794f56` | README ajusté aux besoins | `README.md` |
| 24/06 | `efa3923` | README raccourci, moins de répétitions | `README.md` |
| 24/06 | `8d43c36` | Nettoyage README et vieux fichiers | suppr. `plan_de_implementation.md`, `documentation_decouverte_api.md`, `.drawio` |
