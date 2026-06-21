⚠️ Tests critiques encore manquants (comme demandé)
La couverture reste insuffisante pour garantir la solidité en production. Par ordre de priorité :

Validation E2E des KPIs sur dataset connu — un test qui charge un petit jeu Bronze→Silver→Gold et vérifie les valeurs des 7 KPIs (pas seulement le nombre de lignes). C'est le cœur métier.
Round-trip Parquet partitionné — écrire puis relire Bronze et vérifier que tech_year/month/day/hour sont corrects et que le partition-pruning fonctionne (la régression la plus dangereuse qu'on vient de corriger n'a aucun test la verrouillant ; bloqué localement par winutils, mais essentiel en CI).
data_quality exhaustif — un test par flag (les 8 : INVALID_GROUND_SPEED, INCONSISTENT_POSITION, MISSING_POSITION, etc.). Aujourd'hui seuls 2-3 flags sont couverts.
silver_gold_loader.run_full_etl — test d'orchestration Bronze→Silver→Gold de bout en bout (actuellement aucun ; seules les fonctions unitaires de transformation sont testées).
Idempotence / déduplication — vérifier que rejouer un batch ne double pas les vols (le dedup par flight_id n'a qu'un test unitaire minimal).
cleanup_old_partitions en mode réel (dry_run=False) — vérifier qu'il supprime bien au-delà de la rétention et garde le reste (seul le dry-run est testé).