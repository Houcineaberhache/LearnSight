# Ingestion — Docker

Image d'exécution des producteurs Kafka LearnSight (Étudiant 1).

## Contenu

| Élément | Rôle |
|---|---|
| `Dockerfile` | Image Python 3.11 avec les dépendances d'ingestion et le code |
| `kafka-init` (Compose) | Crée les deux topics Kafka configurés |
| `ingestion-producer` (Compose) | Rejoue les données OULAD nettoyées vers Kafka |

L'image est construite depuis la racine du projet :

```bash
docker build -f docker/ingestion/Dockerfile -t learnsight-ingestion:local .
```

## Services Compose

Les services `kafka-init` et `ingestion-producer` sont placés derrière le profil `ingestion`. Ils ne démarrent donc pas avec un `docker compose up` classique.

### Démarrage de l'infrastructure Kafka et de l'initialisation des topics

```bash
docker compose up -d zookeeper kafka
docker compose --profile ingestion run --rm kafka-init
```

### Exécution des producteurs

```bash
docker compose --profile ingestion run --rm ingestion-producer
```

### Exécution avec une limite de messages pour un test

```bash
docker compose --profile ingestion run --rm ingestion-producer ingestion.producers.run_all_producers --data-dir /data/processed --limit 2 --delay 0.01 --validate
```

### Consultation des logs

```bash
docker compose logs -f kafka
```

Pour les services exécutés avec `docker compose run`, les logs sont directement affichés dans le terminal.

### Arrêt des services Kafka

```bash
docker compose stop kafka zookeeper
```

Éviter `docker compose --profile ingestion down` si l'objectif est uniquement d'arrêter l'ingestion : cette commande arrête également les autres services du projet inclus dans le périmètre Compose.

## Variables d'environnement

| Variable | Valeur par défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` dans Compose | Adresse interne du broker Kafka |
| `KAFKA_TOPIC_CLICS_VLE` | `clics_vle` | Topic des clics VLE |
| `KAFKA_TOPIC_EVALUATIONS` | `evaluations` | Topic des évaluations |
| `INGESTION_DELAY` | `0.01` | Délai entre deux messages (secondes) |

## Données

Le dossier `./data` de l'hôte est monté en lecture seule dans le conteneur sur `/data`.

Les fichiers CSV nettoyés doivent être présents dans `data/processed/` avant l'exécution des producteurs.

Pour nettoyer et préparer les données OULAD :

```bash
python ingestion/clean_oulad.py --data-dir data/raw --output-dir data/processed
```

Aucune donnée OULAD n'est copiée dans l'image Docker. Les données sont fournies au conteneur via le volume monté (voir `.dockerignore`).

> **Remarque :** `studentInfo.csv` contient les informations de référence des étudiants. Il s'agit d'une donnée statique qui n'est pas publiée dans Kafka et ne possède pas de topic dédié.

## Vérification des messages Kafka

L'entrypoint du Dockerfile est `python -m`. Il permet d'exécuter les modules Python de l'application.

Exemple de vérification du topic `clics_vle` depuis un conteneur Docker :

```bash
docker compose --profile ingestion run --rm ingestion-producer \
  ingestion.producers.kafka_consumer \
  --topic clics_vle \
  --bootstrap-servers kafka:9092 \
  --from-beginning \
  --max-messages 5 \
  --validate
```

Pour vérifier le topic `evaluations`, remplacer `clics_vle` par `evaluations`.

L'option `--validate` vérifie les messages lus à l'aide du schéma JSON correspondant au topic.