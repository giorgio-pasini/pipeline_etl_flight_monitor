# Justifications du modèle de données et des choix d'architecture

**Document :** Architecture du pipeline ETL de trafic aérien  
**Date :** 2026-06-21  
**Auteur :** Data Engineering Team

---

## 1. Vue d'ensemble

Le pipeline suit une **architecture Medallion** (Bronze → Silver → Gold) pour :
- **Traçabilité** : chaque étape laisse une trace auditable
- **Résilience** : une erreur en Silver/Gold n'invalide pas Bronze
- **Scalabilité** : plusieurs pipelines Gold peuvent consommer Silver sans duplication
- **Performance** : les requêtes analytiques hitent la couche adaptée (Gold = pré-agrégé)

---

## 2. Choix technologiques

### 2.1 Spark Core (Batch API)

**Pourquoi :** Le kata spécifie une exécution toutes les 2 heures. C'est un **batch job**, pas du streaming continu.
- **Spark Core batch API** : parfaitement adapté pour un job déclenché périodiquement
- **Orchestration externe** : scheduler (cron, Airflow, etc.) déclenche le job toutes les 2h
- **Exactly-once semantics** : chaque vol est compté une et une seule fois (write mode "append" idempotent)
- **Fault tolerance** : Spark gère les failures, job rejouable (même données → même résultat)
- **Native Spark** : aucune dépendance externe (vs Kafka, Kinesis, Structured Streaming)

Alternative rejetée : Structured Streaming. Conçu pour du streaming continu (micro-batches en boucle infinie),
pas pour des jobs batch périodiques. Sur-ingénierie pour ce cas d'usage.

### 2.2 Format Parquet

**Pourquoi :**
- **Compression native** (~5× vs CSV) → économie de stockage
- **Columnar** → requêtes analytiques 10-100× plus rapides
- **Schéma fort** → détection de type au write-time
- **Partition pruning** → Spark élimine automatiquement les partitions inutiles
- **Écosystème** : lisible par Spark, Pandas, duckdb, Trino, etc.

Alternatives :
- CSV : humain-lisible, mais zéro compression, pas de schéma → rejeté
- AVRO : bon, mais moins répandu dans l'écosystème Spark/analytics
- Delta Lake : excellent (transactions, versioning), mais plus lourd pour ce MVP

### 2.3 Partitionnement temporel

**Schéma:** `tech_year=YYYY/tech_month=YYYY-MM/tech_day=YYYY-MM-DD/tech_hour=HH`

**Pourquoi :**
- **Spec du kata** : demande explicite `tech_year=2023/tech_month=2023-07/...`
- **Partition pruning** : une requête "donnez-moi les vols du 16 juillet" élimine 1000 jours
- **Retention facile** : supprimer `tech_day=2026-05-21` supprime un jour entier
- **Chronologie** : aligné avec la collecte (batch toutes les 2h) → pas d'out-of-order complexe

### 2.4 Couches de nettoyage progressif

**Bronze** : données brutes, aucune transformation  
**Silver** : nettoyage, normalisation, flagging de qualité  
**Gold** : agrégations, ranking, KPIs

**Pourquoi :**
- **Auditabilité** : si un KPI Gold est faux, on peut revenir à Silver/Bronze pour déboguer
- **Découplage** : ajouter un nouveau KPI Gold n'impacte pas Silver
- **Réutilisabilité** : d'autres projets peuvent consommer Silver

---

## 3. Modèle de données détaillé

### 3.1 Fact vs. Dimensions

Le modèle suit une **star schema** (variante simplified dimensional modeling) :

```
                    dim_airlines
                         |
                         v
flights_raw -----> fact_flights <----- dim_airports
                         ^
                         |
                    dim_aircraft_models
```

**fact_flights** : 
- Volume important (~1500 vols/batch)
- Mesures : altitude, vitesse, position, distance
- Dimensions : airline_icao (FK), airport IATA (FK), aircraft_code (FK)
- Calcul : distance haversine (origin lat/lon → destination lat/lon)

**dim_airlines, dim_airports, dim_aircraft_models** :
- Petites tables (~2000 compagnies, ~50000 aéroports, ~3000 modèles)
- Mises à jour lentes (growth only, rarement suppression/modification)
- Enrichissent fact_flights (names, countries, continents)

### 3.2 Colonnes de qualité

**data_quality_flags (string)**  
Concaténation de flags séparés par virgule. Exemples :
```
"MISSING_DESTINATION,INVALID_ALTITUDE"
"MISSING_ORIGIN"
null (si aucun problème détecté)
```

Flags définis :
- `MISSING_ORIGIN`, `MISSING_DESTINATION`, `MISSING_AIRLINE`, `MISSING_AIRCRAFT_CODE` : codes manquants
- `MISSING_POSITION`, `INVALID_ALTITUDE`, `INVALID_GROUND_SPEED`, `INCONSISTENT_POSITION` : données incohérentes
- `MISSING_ORIGIN_COUNTRY`, `MISSING_DESTINATION_COUNTRY` : enrichissement manquant (Silver)

Avantage : chaque flag est tracé → audit trail complet.

**is_valid (boolean)**  
Drapeau synthétique : "ce vol est utilisable pour les KPIs".

Critères :
```
is_valid = (
  on_ground = 0 AND
  origin_iata NOT NULL AND
  destination_iata NOT NULL AND
  airline_icao NOT NULL AND
  data_quality_flags IS NULL
)
```

Les KPIs ne consomment que les vols avec `is_valid=True`.

### 3.3 Continent vs. Pays

**Observation :** L'API retourne le **pays**, pas le continent.  
**Solution :** Table `dim_countries_continents` (mapping statique ISO 3166-1 alpha-2 → continent).

| Pays | Code | Continent |
|------|------|-----------|
| France | FR | EU |
| Japon | JP | AS |
| Brésil | BR | SA |
| USA | US | NA |

Utilisée dans les joins Silver → Gold pour les KPIs continentaux.

---

## 4. Mappage des KPIs

### KPI 1: Compagnie avec le + de vols en cours

```sql
SELECT airline_icao, airline_name, COUNT(*) as active_flights_count
FROM fact_flights
WHERE is_valid = True AND on_ground = 0
GROUP BY airline_icao, airline_name
ORDER BY active_flights_count DESC
LIMIT 1
```

**Données sources :** `fact_flights` (on_ground, airline_icao)  
**Données dérivées :** None (aggregate natif)  
**Table Gold :** `kpi_airline_volumes` (avec rank)

---

### KPI 2: Par continent, compagnie la + active en régional

Définition : vols où continent_origine = continent_destination.

```sql
SELECT continent_code, airline_icao, airline_name, COUNT(*) as regional_flights_count
FROM fact_flights f
JOIN dim_countries_continents c_orig ON f.origin_airport_country_code = c_orig.country_code
JOIN dim_countries_continents c_dest ON f.destination_airport_country_code = c_dest.country_code
WHERE is_valid = True 
  AND c_orig.continent_code = c_dest.continent_code
GROUP BY continent_code, airline_icao, airline_name
```

**Données sources :** `fact_flights`, `dim_airports` (country codes), `dim_countries_continents`  
**Données dérivées :** continent_code (via join)  
**Table Gold :** `kpi_continental_regional` (avec rank/continent)

---

### KPI 3: Vol en cours au trajet le + long

```sql
SELECT flight_id, callsign, airline_name, origin_airport_name, destination_airport_name,
       distance_nm, latitude, longitude, altitude_feet
FROM fact_flights
WHERE is_valid = True
ORDER BY distance_nm DESC
LIMIT 1
```

**Données sources :** `fact_flights` (distance_nm = haversine calculé)  
**Données dérivées :** distance_nm (Silver)  
**Table Gold :** `kpi_longest_flights` (snapshot ponctuel)

---

### KPI 4: Par continent, longueur de vol moyenne

```sql
SELECT c.continent_code, c.continent_name,
       AVG(f.distance_nm) as avg_distance,
       MIN(f.distance_nm) as min_distance,
       MAX(f.distance_nm) as max_distance,
       COUNT(*) as flight_count
FROM fact_flights f
JOIN dim_countries_continents c ON f.origin_airport_country_code = c.country_code
WHERE is_valid = True
GROUP BY c.continent_code, c.continent_name
```

**Données sources :** `fact_flights`, `dim_countries_continents`  
**Table Gold :** `kpi_continental_avg_distance`

---

### KPI 5: Constructeur d'avions avec le + de vols actifs

```sql
SELECT m.manufacturer, COUNT(*) as active_flights_count
FROM fact_flights f
JOIN dim_aircraft_models m ON f.aircraft_code = m.aircraft_code
WHERE is_valid = True AND on_ground = 0
GROUP BY m.manufacturer
ORDER BY active_flights_count DESC
```

**Données sources :** `fact_flights`, `dim_aircraft_models` (manufacturer derivable du code ou lookup)  
**Table Gold :** `kpi_aircraft_manufacturers` (avec rank)

---

### KPI 6: Par pays de compagnie, top 3 des modèles d'avion

```sql
SELECT a.country_code, a.airline_icao, a.airline_name,
       m.aircraft_code, m.aircraft_model,
       COUNT(*) as usage_count,
       ROW_NUMBER() OVER (PARTITION BY a.country_code, a.airline_icao ORDER BY COUNT(*) DESC) as rank
FROM fact_flights f
JOIN dim_airlines a ON f.airline_icao = a.airline_icao
JOIN dim_aircraft_models m ON f.aircraft_code = m.aircraft_code
WHERE is_valid = True
GROUP BY a.country_code, a.airline_icao, a.airline_name, m.aircraft_code, m.aircraft_model
QUALIFY rank <= 3
```

**Données sources :** `fact_flights`, `dim_airlines` (country_code), `dim_aircraft_models`  
**Table Gold :** `kpi_airline_aircraft_models` (avec rank 1-3)

---

### KPI Bonus: Aéroport au + grand écart départs/arrivées

```sql
SELECT a.airport_iata, a.airport_name, a.country_name,
       COUNT(CASE WHEN f.origin_iata = a.airport_iata THEN 1 END) as outgoing_flights,
       COUNT(CASE WHEN f.destination_iata = a.airport_iata THEN 1 END) as incoming_flights,
       COUNT(CASE WHEN f.origin_iata = a.airport_iata THEN 1 END) -
       COUNT(CASE WHEN f.destination_iata = a.airport_iata THEN 1 END) as imbalance
FROM fact_flights f
JOIN dim_airports a ON (f.origin_iata = a.airport_iata OR f.destination_iata = a.airport_iata)
WHERE is_valid = True
GROUP BY a.airport_iata, a.airport_name, a.country_name
ORDER BY ABS(imbalance) DESC
```

**Données sources :** `fact_flights`, `dim_airports`  
**Table Gold :** `kpi_airport_imbalance`

---

## 5. Stratégie de rétention et nettoyage

### Bronze

- **Retention :** 30 jours
- **Nettoyage :** purger `tech_day` > 30 jours après collecte
- **Justification :** garantir la capacité de rejouer/déboguer ; données brutes occupent peu (comprimé Parquet)

### Silver

- **Retention :** 60 jours (données nettoyées, plus de valeur analytique)
- **Nettoyage :** purger `tech_day` > 60 jours

### Gold

- **Retention :** 1 an (KPIs, utilisés pour trends/benchmarking)
- **Nettoyage :** purger `kpi_date` > 365 jours

### Procédure de purge

```bash
spark-submit purge_old_partitions.py \
  --datalake-path /path/to/datalake \
  --layer bronze \
  --retention-days 30
```

---

## 6. Scalabilité et coûts

### Volumes estimés

**Par batch (2 heures) :**
- Vols capturés globalement : ~1500 (sans zone)
- Vols enrichis (après get_flight_details) : ~1500
- Taille comprimée Parquet (Bronze) : ~5-10 MB
- Taille (Silver, après nettoyage/join) : ~8-15 MB
- Taille (Gold, agrégations) : ~0.5-1 MB

**Par jour (12 batches) :**
- Bronze : ~120 MB
- Silver : ~180 MB
- Gold : ~12 MB
- **Total : ~300 MB/jour**

**Par mois (30 jours) :**
- ~9 GB
- **Par an : ~110 GB** (très petit, bien en-dessous des coûts significatifs)

### Ressources Spark

**Mode** : Spark Core batch (orchestré toutes les 2h par scheduler)  
**Driver** : 2 vCPU, 4 GB RAM  
**Executors** : 4 × (4 vCPU, 8 GB RAM chacun)  
**Raison** : ~1500 vols = workload raisonnable ; parallélisation zone-by-zone en Étape 3 peut augmenter

---

## 7. Résilience et fault-tolerance

### Philosophie

**"Loud but not loud-breaking"** — les erreurs ne doivent JAMAIS arrêter le pipeline,
mais elles doivent être **très visibles** dans les logs et les data.

### Implémentation

1. **Flagging détaillé** : chaque problème de qualité ajoute un flag à `data_quality_flags`
2. **Séparation valid/invalid** : colonnes `is_valid=True` utilisées pour KPIs ; `is_valid=False` loggées séparément
3. **Logging structuré** : chaque étape (extract, load, transform) logs les counts et le profil de qualité
4. **Métriques Prometheus** : counts par flag, taux d'erreur, latence (Étape 6)
5. **Alertes** : déclenchées si `pct_invalid > 10%` ou `is_valid == 0%` (Étape 6)

### Exemple : gestion d'une compagnie manquante

```python
# Scenario : get_flight_details() retourne None pour airline_name
df = df.withColumn("airline_name",
                   coalesce(col("airline_name"), lit("UNKNOWN")))
df = df.withColumn("data_quality_flags",
                   concat_ws(",", col("data_quality_flags"), lit("MISSING_AIRLINE_NAME")))
# Le vol continue le pipeline → flaggé pour audit, mais n'échoue pas
```

---

## 8. Évolution future

### MVP (actuellement)
- Batch toutes les 2h
- Collecte globale (~1500 vols) + zones optionnelles
- KPIs horaires
- Dashboard Streamlit statique

### Phase 2 (si scaling)
- Collecte zone-by-zone en paralèle → ~50k vols/batch
- SLA horaire (lag < 1h)
- Streaming réel (Kafka ou Kinesis) + exactement-une sémantique
- Alertes Slack/SMS sur anomalies

### Phase 3 (long-term)
- Airflow orchestration
- Déploiement AWS (S3 datalake, EC2/ECS workers)
- Quicksight/Tableau dashboard
- ML : prédiction retards aéroports, anomaly detection

---

## 9. Conclusion

Le modèle proposé est :
✅ **Simple** : star schema, pas d'over-engineering  
✅ **Auditable** : traçabilité complète Bronze → Silver → Gold  
✅ **Résilient** : fault-tolerant, flagging détaillé  
✅ **Performant** : Parquet, partition pruning, agrégations pré-calculées  
✅ **Évolutif** : facile d'ajouter zones, KPIs ou sources (aéroports, compagnies, etc.)
