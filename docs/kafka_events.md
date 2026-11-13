# 📡 Contrats d'événements Kafka — LearnSight

> **Composant :** Ingestion (Étudiant 1)
> **Destinataires :** Étudiant 2 (Lakehouse), Étudiant 3 (ML), Étudiant 4 (Airflow), Étudiant 5 (Dashboard)
> **Version des schémas :** `1.0`

Ce document est le **contrat d'interface** entre le système d'ingestion et le
reste de la plateforme. Les schémas JSON Draft-07 des **deux topics de
streaming** se trouvent dans [`ingestion/schemas/`](../ingestion/schemas) et
sont appliqués automatiquement par les producteurs, le consommateur de
vérification et les tests.

---

## 1. Enveloppe commune

Tous les messages partagent la même enveloppe `metadata` :

```json
{
  "metadata": {
    "event_id": "4f1c2f9e-6a0e-4d0e-9f4e-2b7a1c8d5e10",
    "event_type": "vle_click",
    "produced_at": "2026-11-12T19:30:00.123456+00:00",
    "source_system": "oulad_simulation_producer",
    "schema_version": "1.0"
  },
  "payload": { }
}
```

| Champ | Type | Obligatoire | Description |
|---|---|---|---|
| `event_id` | string (UUIDv4) | ✅ | Identifiant unique de l'événement — clé de déduplication |
| `event_type` | string (enum) | ✅ | `vle_click` ou `student_assessment` |
| `produced_at` | string (RFC-3339) | ✅ | Horodatage de production en UTC |
| `source_system` | string | ✅ | Toujours `oulad_simulation_producer` |
| `schema_version` | string | ✅ | Version du contrat (`1.0`) |

> **Déduplication côté Lakehouse :** utiliser `metadata.event_id` comme clé
> primaire technique. Deux rejeux produisent de nouveaux `event_id` : pour une
> déduplication métier, utiliser la clé naturelle décrite par topic.

---

## 2. Topics Kafka

| Topic | Schéma | Partitions | Rétention | Clé Kafka | Producteur |
|---|---|---|---|---|---|
| `clics_vle` | `clics_vle_schema.json` | 3 | 7 jours | `id_student` | `vle_producer.py` |
| `evaluations` | `evaluations_schema.json` | 3 | 7 jours | `id_student` | `evaluations_producer.py` |

> ℹ️ `studentInfo.csv` est un **référentiel statique** (dimension), pas un flux
> d'événements. Il n'existe volontairement aucun topic Kafka `student_profiles` :
> le Lakehouse le charge depuis les fichiers nettoyés.

Configuration de référence : [`config/kafka/kafka_topics.yml`](../config/kafka/kafka_topics.yml).

La clé `id_student` garantit que **tous les événements d'un même étudiant
arrivent dans la même partition**, ce qui préserve l'ordre par étudiant pour
les agrégations d'engagement.

---

## 3. Topic `clics_vle` — Interactions VLE

**Source OULAD :** `studentVle.csv` (10,6 M de lignes)
**Clé naturelle :** `(code_module, code_presentation, id_student, id_site, date)`
**Granularité :** une ligne agrégée = un étudiant / une ressource / un jour.

### Payload

| Champ | Type | Obligatoire | Contrainte |
|---|---|---|---|
| `code_module` | string | ✅ | 3 caractères (ex. `AAA`) |
| `code_presentation` | string | ✅ | 5 caractères (ex. `2013J`) |
| `id_student` | integer | ✅ | Identifiant étudiant |
| `id_site` | integer | ✅ | Identifiant de la ressource VLE |
| `date` | integer | ✅ | Jour relatif au début du cours — **négatif autorisé** |
| `sum_click` | integer | ✅ | Nombre de clics, `>= 1` |

```json
{
  "metadata": {
    "event_id": "8d6f0b3a-2c4e-4a7b-9f11-5e2d7c9a1b44",
    "event_type": "vle_click",
    "produced_at": "2026-11-12T19:30:00.123456+00:00",
    "source_system": "oulad_simulation_producer",
    "schema_version": "1.0"
  },
  "payload": {
    "code_module": "AAA",
    "code_presentation": "2013J",
    "id_student": 28400,
    "id_site": 546652,
    "date": -10,
    "sum_click": 4
  }
}
```

### Règles métier

- **Dates négatives préservées** : `date = -10` signifie 10 jours avant le
  début du cours. Ne jamais appliquer `abs()` ni remplacer par `0`.
- `sum_click >= 1` : les lignes agrégées OULAD ne contiennent pas de clic nul.
- Le topic ne contient **aucune** information de résultat (`final_result`).

---

## 4. Topic `evaluations` — Soumissions et notes

**Sources OULAD :** `studentAssessment.csv` + enrichissement `assessments.csv`
**Clé naturelle :** `(id_assessment, id_student)`

### Payload

| Champ | Type | Obligatoire | Contrainte |
|---|---|---|---|
| `id_assessment` | integer | ✅ | Identifiant de l'évaluation |
| `id_student` | integer | ✅ | Identifiant étudiant |
| `date_submitted` | integer | ✅ | Jour relatif de soumission |
| `is_banked` | boolean | ✅ | Crédits reportés (`0/1` converti en booléen) |
| `score` | number \| null | ✅ | Note 0–100, `null` si copie non notée |
| `assessment_type` | string | ❌ | `TMA`, `CMA` ou `Exam` (si métadonnées disponibles) |
| `weight` | number | ❌ | Poids de l'évaluation 0–100 |

```json
{
  "metadata": {
    "event_id": "c1a9e7d2-3b6f-4c8e-a5d0-7f2e9b1c6a33",
    "event_type": "student_assessment",
    "produced_at": "2026-11-12T19:31:02.654321+00:00",
    "source_system": "oulad_simulation_producer",
    "schema_version": "1.0"
  },
  "payload": {
    "id_assessment": 1752,
    "id_student": 11391,
    "date_submitted": 18,
    "is_banked": false,
    "score": 78.0,
    "assessment_type": "TMA",
    "weight": 15.0
  }
}
```

### Règles métier

- `score = null` est **valide** et significatif (copie non notée / absente).
  Ne jamais remplacer par `0`, qui fausserait les moyennes ML.
- `assessment_type` et `weight` sont optionnels : si `assessments.csv` est
  absent, l'événement reste conforme au contrat.
- Aucune note finale de module (`final_result`) n'est diffusée.

---

## 5. Référentiel statique `studentInfo.csv` — hors Kafka

**Source OULAD :** `studentInfo.csv` (sans `final_result`)
**Nature :** données statiques de dimension, chargées **une seule fois** par le
Lakehouse depuis `data/processed/studentInfo.csv`.
**Aucun topic Kafka n'est créé** : ces informations ne sont pas des événements
de streaming.

### Champs du référentiel

| Champ | Type | Obligatoire | Contrainte |
|---|---|---|---|
| `code_module` | string | ✅ | 3 caractères |
| `code_presentation` | string | ✅ | 5 caractères |
| `id_student` | integer | ✅ | Identifiant étudiant |
| `gender` | string | ✅ | `M` ou `F` |
| `region` | string | ✅ | Région déclarée |
| `highest_education` | string | ✅ | Niveau d'éducation |
| `imd_band` | string \| null | ✅ | Tranche IMD, `null` si inconnue (1 111 cas OULAD) |
| `age_band` | string | ✅ | `0-35`, `35-55`, `55<=` |
| `num_of_prev_attempts` | integer | ✅ | `>= 0` |
| `studied_credits` | integer | ✅ | `>= 0` |
| `disability` | string | ✅ | `Y` ou `N` |

> Le schéma [`student_profiles_schema.json`](../ingestion/schemas/student_profiles_schema.json)
> est conservé comme référence de validation de ce fichier statique. Il ne
> correspond à aucun topic Kafka et n'est pas utilisé par les producteurs.

### 🔒 Anti-data-leakage

`final_result` (et sa dérivée `is_dropout`) est la **variable cible ML**. Elle
est stockée séparément dans `data/processed/student_target_labels.csv` et :

- n'est **jamais** incluse dans les messages Kafka ni dans le référentiel ;
- est **interdite** par le schéma (`additionalProperties: false`) ;
- est validée par les tests (`test_schemas.py`).

---

## 6. Contrat de consommation (Étudiant 2 — Spark)

1. Lire le topic avec `KafkaConsumer` / `readStream.format("kafka")`.
2. Désérialiser la valeur en JSON (UTF-8).
3. Extraire `metadata.event_id`, `metadata.event_type`, `payload.*`.
4. Écrire les données brutes en **Bronze** avec le message complet.
5. En **Silver**, dédupliquer sur `event_id` et appliquer les règles de
   nettoyage ; conserver `score = null`, `imd_band = null` et les dates négatives.
6. Utiliser `id_student` comme clé de partitionnement / jointure.
7. Ne jamais joindre `student_target_labels.csv` avant l'étape ML.

---

## 7. Exploitation

### Créer les topics

```bash
python -m ingestion.producers.kafka_admin --create --list
```

### Produire

```bash
# Les deux producteurs en parallèle
python -m ingestion.producers.run_all_producers --data-dir data/processed

# Un producteur seul
python -m ingestion.producers.vle_producer --limit 1000
python -m ingestion.producers.evaluations_producer --limit 500
```

### Vérifier les messages et les contrats

```bash
python -m ingestion.producers.kafka_consumer --topic clics_vle \
    --from-beginning --max-messages 10 --validate

python -m ingestion.producers.kafka_consumer --topic evaluations \
    --from-beginning --validate
```

### Docker

```bash
# Démarrer Kafka, créer les topics et lancer les producteurs
docker compose --profile ingestion up -d zookeeper kafka kafka-init ingestion-producer

# Suivre les logs du service ingestion-producer
docker compose logs -f ingestion-producer
```

**Remarque :** Le profil `ingestion` active les services `kafka-init` et `ingestion-producer`. Les services Kafka et Zookeeper sont démarrés explicitement. Le démarrage de l'ensemble de la plateforme (Spark, PostgreSQL, Airflow, Superset et MinIO) doit être effectué séparément, en coordination avec les membres responsables de ces composants.
## 8. Évolution des schémas

- Toute modification de payload doit être **rétrocompatible** ou introduire
  une nouvelle `schema_version`.
- Ne jamais renommer ou supprimer un champ existant sans coordination avec les
  étudiants 2, 3 et 5.
- Toute évolution doit être accompagnée d'une mise à jour :
  - du fichier JSON Schema ;
  - de ce document ;
  - des tests `test_schemas.py` / `test_validator.py`.
