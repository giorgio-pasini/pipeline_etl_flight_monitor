"""
Pipeline ETL de trafic aérien — Bibliothèques de support.

Modules :
- schemas : Définitions des StructType Spark pour toutes les couches
- data_quality : Validation et flagging de la qualité des données
- transformations : Fonctions de nettoyage et enrichissement (à venir)
- batch_job : Job Spark Core Batch (orchestré toutes les 2h)
"""

from . import schemas, data_quality

__version__ = "0.1.0"
