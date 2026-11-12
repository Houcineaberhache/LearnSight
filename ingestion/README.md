# 🎓 LearnSight — Ingestion & Prétraitement des Données OULAD

> **Composant :** Ingestion, Nettoyage & Contrats d'Interface Événementiels (Streaming Kafka)  
> **Rôle :** Data Ingestion Engineer (Étudiant 1)  
> **Jeu de données :** [Open University Learning Analytics Dataset (OULAD)](https://analyse.kmi.open.ac.uk/open_dataset)  
> **Statut :** Pipeline de nettoyage validé & 17 tests unitaires passés (100% succès)

---

## 📌 Présentation du Module

Ce module constitue le **point d'entrée de la plateforme LearnSight**. Il est responsable de transformer les données brutes de l'OULAD en flux d'événements propres, normalisés et validés, prêts à être diffusés via **Apache Kafka** vers le Data Lakehouse (Bronze / Silver / Gold).

### 🎯 Responsabilités Clés
1. **Exploration & Audit Qualité** : Détection des anomalies, distributions, types et valeurs manquantes dans les données OULAD brutes (`data/raw/`).
2. **Nettoyage & Normalisation Stricte** : Prétraitement sans perte de données (`df.dropna()` interdit) et conservation scrupuleuse des dates relatives pré-rentrée ($J < 0$).
3. **Prévention du Data Leakage** : Isolation systématique de la variable cible `final_result` / `is_dropout` hors des flux de streaming.
4. **Définition des Contrats d'Interface Kafka** : Schémas JSON Draft-07 stricts pour les topics de streaming `clics_vle`, `evaluations` et `student_profiles`.
5. **Assurance Qualité & Tests** : Suite complète de 17 tests unitaires (`pytest`) assurant la non-régression et la validation des contrats.

---

## 🏗️ Structure du Dossier `ingestion/`

```text
ingestion/
├── producers/                        # Simulateurs et producteurs streaming Kafka (à venir)
│   └── .gitkeep
├── schemas/                          # Contrats d'interface Kafka (JSON Schema Draft-07)
│   ├── clics_vle_schema.json         # Schéma des clics et interactions VLE
│   ├── evaluations_schema.json       # Schéma des soumissions de devoirs/examens
│   └── student_profiles_schema.json  # Schéma des profils étudiants (anti-leakage)
├── tests/                            # Tests unitaires du module d'ingestion
│   ├── test_clean_oulad.py           # Tests des 6 règles d'or de nettoyage
│   └── test_schemas.py               # Tests de conformité des schémas JSON et formats
├── utils/                            # Utilitaires réutilisables d'ingestion
│   └── .gitkeep
├── clean_oulad.py                    # Pipeline de nettoyage, typage et séparation ML
├── explore_oulad.py                  # Script d'audit et analyse exploratoire
├── requirements.txt                  # Dépendances requises pour l'ingestion
└── README.md                         # Documentation spécifique du module
```

---

## 🛡️ Règles d'Or & Contraintes Métier

| # | Règle Métier | Détail d'Implémentation |
|---|---|---|
| **1** | **Zéro suppression de lignes** | `df.dropna()` formellement interdit. Une valeur absente possède une forte sémantique métier (ex. `date_unregistration = null` = étudiant n'ayant pas abandonné, 69.1% des étudiants). |
| **2** | **Préservation des dates négatives** | Ne jamais appliquer `abs()` ni tronquer les valeurs négatives. Une date `-10` indique une activité 10 jours avant le début du cours ($J_0$), indicateur clé d'anticipation. |
| **3** | **Anti-Data Leakage ML** | `final_result` (`studentInfo.csv`) est la cible ML (Ground Truth). Elle est isolée dans `student_target_labels.csv` et exclue des profils et flux d'entrée. |
| **4** | **Typage entier nullable (`Int64`)** | Utilisation systématique de `Int64` pour les dates et semaines contenant des valeurs manquantes (`assessments.date`, `vle.week_from`, `studentRegistration.date_*`). |
| **5** | **Optimisation Haut Débit (`studentVle.csv`)** | Traitement vectorisé par blocs (`chunksize=1_000_000`) et schéma `VLE_DTYPES` pour absorber sans saturation mémoire les 10,6M de clics. |
| **6** | **Score numérique nullable (`Float64`)** | Cast explicite de `score` en `Float64` après remplacement des sentinelles `?` par `NaN`. |

---

## ⚙️ Contrats d'Événements Kafka (`ingestion/schemas/`)

Chaque événement produit dans Kafka respecte un format standardisé composé de **métadonnées** et d'un **payload métier** :

### 1. Clics VLE (`clics_vle_schema.json`)
* **Topic Kafka :** `clics_vle`
* **Contenu :** `code_module`, `code_presentation`, `id_student`, `id_site`, `date`, `sum_click`
* **Validation :** format UUIDv4 pour `event_id`, format RFC-3339 pour `produced_at`.

### 2. Évaluations (`evaluations_schema.json`)
* **Topic Kafka :** `evaluations`
* **Contenu :** `id_assessment`, `id_student`, `date_submitted`, `is_banked`, `score` (nullable), et options enrichies `assessment_type`, `weight`.

### 3. Profils Étudiants (`student_profiles_schema.json`)
* **Contenu :** Caractéristiques socio-démographiques statiques (`gender`, `region`, `highest_education`, `imd_band`, `age_band`, `studied_credits`, etc.).
* **Anti-Leakage :** `additionalProperties: false` et interdiction de `final_result`.

---

## 🧪 Tests & Validation Automatisée

La suite de tests contient **17 tests unitaires** exécutables avec `pytest` :

```powershell
# Exécution depuis la racine du projet
pytest ingestion/tests/ -v
```

* **`test_clean_oulad.py` (7 tests)** : Intégrité des 7 tables OULAD, vérification du zéro perte, typage `Int64`/`Float64`, préservation des dates négatives et isolation de la cible ML.
* **`test_schemas.py` (10 tests)** : Conformité des schémas JSON Draft-07, rejet des formats UUID/dates invalides via `FormatChecker`, rejet de toute fuite de données, et validation sur des échantillons réels de données OULAD.

---

## 🚀 Utilisation

### Nettoyage des données OULAD :
```powershell
# Exécution avec les chemins par défaut (data/raw vers data/processed)
python ingestion/clean_oulad.py --data-dir "data/raw" --output-dir "data/processed"

# Exécution avec taille de bloc personnalisée pour studentVle
python ingestion/clean_oulad.py --chunk-size 500000
```

### Exploration :
```powershell
python ingestion/explore_oulad.py
```
