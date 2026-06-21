# Plan de implementation

Ce pipeline doit permettre de fournir les indicateurs suivants:

La compagnie avec le + de vols en cours
Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
Le vol en cours avec le trajet le plus long
Pour chaque continent, la longueur de vol moyenne
L'entreprise constructeur d'avions avec le plus de vols actifs
Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage


Le job doit donc être

fault-tolerant: Un corner-case pas couvert ou une donnée corrompue ne doivent pas causer l'arret du job.
observable: En loggant les informations pertinantes
systématique: conserver les données & résultats dans un mécanisme de stockage, en adoptant une nomencalture adaptée permettant aux data analyst en aval de retrouver les valeurs recherchées pour un couple (Date, Heure) donné.

Un grand pouvoir implique de grandes responsabilités. Vos choix DOIVENT être justifiés dans un Readme.


L'extraction des données PEUT être faite dans le format de votre choix. CSV, Parquet, AVRO, ... celui qu'il vous semble le plus adapté


Votre pipeline DOIT inclure une phase de data cleaning


Le rendu PEUT comporter un Jupyter notebook avec les résultats


votre pipeline DEVRAIT utiliser Apache Spark et l'API DataFrame


votre pipeline DEVRAIT stocker les données dans un dossier avec une nomenclature horodatée. Ex: Flights/rawzone/tech_year=2023/tech_month=2023-07/tech_day=2023-07-16/flights2023071619203001.csv



Questions Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?


- #DONE : créer un premier notebook d'exploration pour voir ce que l'API offre
- #TODO : base the developpement with a test-based development, so to make sure everything is correctly safe. Don't overdo with the number of tests, the idea is to secure the solution without an unmanageable over-abundance of tests
- #TODO : faire une modélisation des données pour comprendre comme mieux arranger les tables
- #TODO : faire une structure datalake pour le layer landing en déposant le résultat directe des l'extraction de l'API, en suivant la structure de projet proposée par le Kata
- #TODO : faire un POC de Structured Streaming en utilisant Spark pour commencer à déposer les fichiers dans le datalake
- #TODO : faire un POC de transformation sur les données extraites pour s'assurer que les réponses aux KPI sont contenues dans le données qu'on a extrait, autre que voir rapidement les transformations à faire
- #TODO : individuer les meilleures stratégies de partition à appliquer pour le job Spark
- #TODO : implementer système de logging et de monitoring du job
- #TODO : implementer le job spark finale en utilisant le structured streaming
- #TODO : implementer la dashboard streamlit pour répondre aux questions du kata
- #TODO : tout implementer avec une structure fault-tolerant mais loud, de facon qui si il y a des problèmes ils ne cassent pas le job mais ils sont très bien flaggés dans les données et dans les logs
- #TODO : implementer tout ca en utilisant la structure plus simple mais efficace possible, sans allant trop rapidement aux over-engineering
- #ONHOLD : implementer Apache AirFlow pour faire tourner automatiquement le job spark
- #ONHOLD : tout deployer sur AWS