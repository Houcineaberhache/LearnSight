# Guide de configuration — Data Ingestion

## 1. Prérequis

- Python 3.11
- Docker Desktop
- Git
- Dataset OULAD original

## 2. Préparation de l'environnement Python

Depuis la racine du projet :

```bash
python -m venv venv
```

Activation sous Windows :

```cmd
venv\Scripts\activate
```

Installation des dépendances :

```bash
pip install -r ingestion/requirements.txt
```

## 3. Organisation des données

Placer les fichiers CSV originaux du dataset OULAD dans :

```text
data/raw/
```

Les données nettoyées seront générées dans :

```text
data/processed/
```

## 4. Nettoyage des données OULAD

Depuis la racine du projet :

```bash
python ingestion/clean_oulad.py --data-dir data/raw --output-dir data/processed
```

Pour consulter les options disponibles :

```bash
python ingestion/clean_oulad.py --help
```

Les fichiers nettoyés doivent être disponibles dans `data/processed/` avant l'exécution des producteurs Kafka.

## 5. Architecture des événements Kafka

Deux flux sont publiés par les producteurs :

| Topic Kafka | Source OULAD |
|---|---|
| `clics_vle` | `studentVle.csv` |
| `evaluations` | `studentAssessment.csv` et `assessments.csv` |

`studentInfo.csv` est une donnée de référence statique. Elle n'est pas publiée dans Kafka.

Les labels cibles (`final_result`, `is_dropout`) ne doivent pas être intégrés aux événements utilisés comme données d'entrée, afin d'éviter les fuites de données (*data leakage*).

## 6. Exécution de l'ingestion Kafka

La configuration Docker, la construction de l'image, l'initialisation des topics et l'exécution des producteurs sont documentées dans :

[`docker/ingestion/README.md`](../docker/ingestion/README.md)

## 7. Tests

Pour exécuter les tests du composant Ingestion :

```bash
python -m pytest ingestion/tests -v
```

Pour consulter les options des producteurs :

```bash
python -m ingestion.producers.vle_producer --help
python -m ingestion.producers.evaluations_producer --help
```
