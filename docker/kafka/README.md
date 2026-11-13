# Kafka — Docker Configuration

Ce dossier fait partie de l'infrastructure Docker du projet **LearnSight**.

## Architecture Kafka

Le projet n'utilise pas de Dockerfile Kafka personnalisé.

Kafka est déployé via le fichier `docker-compose.yml` situé à la racine du projet, en utilisant les images Confluent Platform 7.5.0.

## Services déployés

| Service | Image | Ports |
|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092 (interne), 29092 (hôte) |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 |

## Lancement rapide

Depuis la racine du projet :

```bash
# Démarrer uniquement les services Kafka
docker compose up -d zookeeper kafka kafka-ui

# Vérifier les topics existants
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Créer les topics configurés et afficher la liste
python -m ingestion.producers.kafka_admin --bootstrap-servers localhost:29092 --create --list
```

## Configuration des topics

La configuration détaillée est définie dans `config/kafka/kafka_topics.yml`.

Le projet utilise deux topics Kafka :

| Topic | Description | Partitions | Replication Factor | Rétention |
|---|---|---:|---:|---:|
| `clics_vle` | Événements de clics des étudiants sur les ressources VLE (OULAD) | 3 | 1 | 7 jours |
| `evaluations` | Événements de soumission des évaluations (OULAD) | 3 | 1 | 7 jours |

Les événements sont produits à partir des données OULAD simulées en streaming.

## Connexion au broker Kafka

Deux adresses sont utilisées selon l'environnement d'exécution :

- Depuis la machine hôte (Windows) : `localhost:29092`
- Depuis les conteneurs Docker : `kafka:9092`

La variable `KAFKA_BOOTSTRAP_SERVERS` permet de personnaliser l'adresse du broker.

## Interface Kafka UI

Kafka UI est accessible à l'adresse :

[http://localhost:8080](http://localhost:8080)

Elle permet de visualiser les topics, les messages et les groupes de consommateurs.

## Variables d'environnement

| Variable | Description | Valeur par défaut |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Adresse du broker Kafka | `localhost:9092` |
| `KAFKA_TOPIC_CLICS_VLE` | Nom du topic des clics VLE | `clics_vle` |
| `KAFKA_TOPIC_EVALUATIONS` | Nom du topic des évaluations | `evaluations` |

Pour configurer l'environnement :

```bash
# Copier le fichier d'exemple
cp .env.example .env
```

Sous Windows CMD :

```cmd
copy .env.example .env
```

Adapter ensuite les valeurs selon l'environnement d'exécution.