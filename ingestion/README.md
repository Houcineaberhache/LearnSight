# Module Data Ingestion — LearnSight

## 1. Présentation

Le module Data Ingestion assure le chargement, le nettoyage et la préparation des données du dataset OULAD, ainsi que la publication des événements d'apprentissage dans Apache Kafka.

Il constitue la source de données du pipeline Big Data LearnSight.

## 2. Sources de données

Le module utilise principalement les fichiers OULAD suivants :

| Fichier source | Utilisation |
|---|---|
| `studentVle.csv` | Événements de clics et d'activité sur les ressources pédagogiques |
| `studentAssessment.csv` | Résultats des évaluations |
| `assessments.csv` | Informations complémentaires sur les évaluations |
| `studentInfo.csv` | Données de référence statiques sur les étudiants |

Les données `studentInfo.csv` sont des données de référence statiques. Elles ne sont pas publiées dans Kafka par ce module.

### Prévention du data leakage

Lors du nettoyage, la variable cible `final_result` est isolée par défaut de `studentInfo.csv`. Les labels `final_result` et `is_dropout` sont enregistrés séparément dans `student_target_labels.csv`.

Cette séparation permet d'éviter d'utiliser les variables cibles comme caractéristiques d'entrée du modèle de prédiction.

L'option `--no-isolate-target` permet de désactiver ce comportement.

## 3. Architecture Kafka

Deux producteurs alimentent les topics Kafka :

| Producteur | Source | Topic Kafka |
|---|---|---|
| `vle_producer.py` | `studentVle.csv` | `clics_vle` |
| `evaluations_producer.py` | `studentAssessment.csv` et `assessments.csv` | `evaluations` |

Les deux producteurs peuvent être exécutés en parallèle avec `run_all_producers.py`.

L'option `--limit` définit le nombre maximal d'événements par producteur.

## 4. Structure du module

```text
ingestion/
├── clean_oulad.py
├── explore_oulad.py
├── requirements.txt
├── README.md
├── producers/
│   ├── vle_producer.py
│   ├── evaluations_producer.py
│   ├── run_all_producers.py
│   ├── kafka_admin.py
│   └── kafka_consumer.py
├── schemas/
│   ├── clics_vle_schema.json
│   ├── evaluations_schema.json
│   └── student_profiles_schema.json
├── tests/
│   ├── test_clean_oulad.py
│   ├── test_consumer.py
│   ├── test_kafka_admin.py
│   ├── test_kafka_integration.py
│   ├── test_producers.py
│   ├── test_schemas.py
│   └── test_validator.py
└── utils/
    ├── kafka_utils.py
    ├── schema_validator.py
    └── __init__.py
```

## 5. Préparation de l'environnement

Depuis la racine du projet, activer l'environnement virtuel :

```cmd
venv\Scripts\activate
```

Installer les dépendances :

```cmd
pip install -r ingestion\requirements.txt
```

Le dataset OULAD brut doit être placé dans `data/raw/`.

## 6. Nettoyage des données

Lancer le script de préparation :

```cmd
python ingestion/clean_oulad.py
```

Par défaut :
- Les fichiers sources sont lus depuis `data/raw/`.
- Les données nettoyées sont enregistrées dans `data/processed/`.
- La variable cible est isolée dans `student_target_labels.csv`.

Options supplémentaires :

```cmd
python ingestion/clean_oulad.py --help
```

## 7. Lancement de l'ingestion

Sous Windows PowerShell, depuis la racine du projet :

```powershell
.\scripts\run_ingestion.ps1 -Limit 1000 -Delay 0.01 -Validate
```

Cette commande lance les deux producteurs en parallèle, avec une limite maximale de 1 000 événements par producteur.

Pour effectuer un test sans publier de messages dans Kafka :

```powershell
.\scripts\run_ingestion.ps1 -DryRun -Limit 5 -Validate -SkipTopics
```

Pour créer ou vérifier les topics Kafka :

```cmd
python -m ingestion.producers.kafka_admin --create --list
```

## 8. Lecture et validation des messages

Exemple pour lire les événements VLE :

```cmd
python -m ingestion.producers.kafka_consumer --topic clics_vle --from-beginning --max-messages 5 --validate
```

Pour les évaluations :

```cmd
python -m ingestion.producers.kafka_consumer --topic evaluations --from-beginning --max-messages 5 --validate
```

## 9. Tests

Exécuter les tests du module :

```cmd
python -m pytest ingestion\tests -v
```

Les contrats JSON permettent de vérifier la structure et la validité des événements avant leur publication ou pendant leur consommation.

## 10. Documentation complémentaire

- `docs/data_dictionary.md` : description des tables et des champs OULAD.
- `docs/kafka_events.md` : contrats et structure des événements Kafka.
- `docs/setup_guide.md` : guide de configuration et de démarrage.
- `docker/ingestion/README.md` : instructions relatives à l'exécution avec Docker.
