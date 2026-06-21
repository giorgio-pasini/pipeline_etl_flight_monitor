# 🚀 Guide de démarrage — Pipeline ETL trafic aérien

**Bienvenue !** Ce guide vous montre comment démarrer le projet en 5 minutes.

---

## 📖 Lire d'abord (par ordre d'importance)

1. **[STATUS.md](STATUS.md)** (2 min)  
   → État actuel du projet (33% complété, POC opérationnel)

2. **[README.md](README.md)** (5 min)  
   → Vue d'ensemble architecture + démarrage rapide

3. **[README_quickstart.md](README_quickstart.md)** (5 min)  
   → Instructions étape-par-étape pour tester le POC

---

## ⚡ Démarrer le POC (5 minutes)

```bash
# Étape 1 : Installer dépendances
pip install -r requirements.txt

# Étape 2 : Initialiser datalake
python scripts/init_datalake.py --verbose

# Étape 3 : Lancer le POC (collecte 1 batch)
python src/batch_job.py --single-batch --verbose

# Étape 4 : Vérifier les résultats
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('datalake/bronze/flights_raw')
print(f'✅ {df.count()} vols collectés et partitionnés')
print(f'Valid: {df.filter(\"is_valid=True\").count()}')
"
```

**Durée estimée :** 5-7 minutes  
**Résultat :** 1500 vols dans `datalake/bronze/flights_raw`

---

## 📚 Documentation détaillée

| Si vous voulez... | Lire... |
|---|---|
| Comprendre l'architecture | [README_modele.md](README_modele.md) |
| Déboguer un problème | [README_quickstart.md](README_quickstart.md) → Troubleshooting |
| Continuer le développement | [documentation/documentation_dev.md](documentation/documentation_dev.md) |
| Comprendre l'API | [notebook_exploration.ipynb](notebook_exploration.ipynb) |
| Vérifier le statut complet | [STATUS.md](STATUS.md) |
| Voir ce qui a été livré | [LIVRAISON.md](LIVRAISON.md) |

---

## 🎯 Statut actuel

✅ **Étapes 1-3 complétées :**
- Modèle de données (star schema)
- Infrastructure datalake (Medallion 3 couches)
- POC Spark opérationnel

⏳ **Étapes 4-9 à faire :**
- Transformation Silver + Gold
- Dashboard Streamlit
- Monitoring & orchestration

---

## 💡 Ce que vous pouvez faire maintenant

### Essayer le POC
```bash
python src/batch_job.py --single-batch
```
→ Collecte les vols, valide, écrit en Bronze (5 min)

### Explorer les données
```bash
# Avec Spark SQL
spark-sql
> SELECT COUNT(*) FROM parquet.`datalake/bronze/flights_raw`

# Ou Python Pandas
df = spark.read.parquet("datalake/bronze/flights_raw").toPandas()
df.head()
```

### Nettoyer les vieilles données
```bash
python scripts/purge_old_partitions.py --all-layers --dry-run
```

### Continuer le développement
→ Lire [documentation/documentation_dev.md](documentation/documentation_dev.md) Étape 4

---

## 🔧 Configuration

**Chemin datalake par défaut :** `./datalake/`

**Surcharger :**
```bash
export DATALAKE_ROOT=/data/my_datalake
python src/batch_job.py --single-batch
```

Voir [config/datalake_config.py](config/datalake_config.py) pour tous les paramètres.

---

## ❓ Questions ?

1. **Comment ça fonctionne ?**  
   → [README_modele.md](README_modele.md)

2. **Ça ne marche pas, quoi faire ?**  
   → [README_quickstart.md](README_quickstart.md) → Troubleshooting

3. **Qu'est-ce qui reste à faire ?**  
   → [STATUS.md](STATUS.md)

4. **Je veux voir les détails techniques**  
   → [documentation/documentation_dev.md](documentation/documentation_dev.md)

---

## 📞 Support

- Logs : `datalake/_logs/*.log`
- Relancer avec `--verbose` pour plus de détails
- Consulter la doc pertinente (voir ci-dessus)

---

**Bonne chance ! 🚀**
