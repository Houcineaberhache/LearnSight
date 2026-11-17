# 📚 Dictionnaire de données — Ingestion OULAD

> **Composant :** Ingestion (Étudiant 1)
> **Source :** [OULAD](https://analyse.kmi.open.ac.uk/open_dataset)
> **Périmètre :** tables brutes, tables nettoyées et événements Kafka

Ce document décrit les données manipulées par la couche d'ingestion. Les
contrats Kafka sont détaillés dans [`kafka_events.md`](kafka_events.md).

---

## 1. Vue d'ensemble

```text
data/raw/                  data/processed/              Kafka
──────────                 ───────────────              ─────
courses.csv          ──►   courses.csv
assessments.csv      ──►   assessments.csv       ──┐
vle.csv              ──►   vle.csv                 │
studentInfo.csv      ──►   studentInfo.csv  ───────┼──► (référentiel statique, hors Kafka)
                     └──►   student_target_labels.csv  (cible ML isolée)
studentRegistration  ──►   studentRegistration.csv
studentAssessment    ──►   studentAssessment.csv ──┴──► evaluations
studentVle.csv       ──►   studentVle.csv       ──────► clics_vle
```

---

## 2. Tables brutes OULAD

| Fichier | Volume de référence | Clé | Description |
|---|---|---|---|
| `courses.csv` | 22 présentations | `(code_module, code_presentation)` | Modules et sessions |
| `assessments.csv` | 206 évaluations | `id_assessment` | Métadonnées des évaluations |
| `vle.csv` | 6 364 ressources | `id_site` | Ressources du VLE |
| `studentInfo.csv` | 32 593 inscriptions | `(code_module, code_presentation, id_student)` | Profil et résultat final |
| `studentRegistration.csv` | 32 593 inscriptions | `(code_module, code_presentation, id_student)` | Inscription / désinscription |
| `studentAssessment.csv` | 173 912 soumissions | `(id_assessment, id_student)` | Notes par évaluation |
| `studentVle.csv` | 10 655 280 interactions | `(code_module, code_presentation, id_student, id_site, date)` | Clics quotidiens |

### Colonnes principales

| Table | Colonne | Type | Sémantique |
|---|---|---|---|
| `courses` | `module_presentation_length` | int | Durée de la présentation (jours) |
| `assessments` | `date` | int \| null | Date limite relative à $J_0$ ; `null` pour certains examens |
| `assessments` | `weight` | float | Poids de l'évaluation (%) |
| `vle` | `week_from` / `week_to` | int \| null | Semaines planifiées d'utilisation |
| `studentInfo` | `imd_band` | string \| null | Tranche IMD ; `null` si inconnue |
| `studentInfo` | `final_result` | string | **Cible ML** : `Pass`, `Fail`, `Withdrawn`, `Distinction` |
| `studentRegistration` | `date_registration` | int \| null | Inscription relative à $J_0$ (négatif = avant le cours) |
| `studentRegistration` | `date_unregistration` | int \| null | Désinscription ; `null` = pas d'abandon enregistré |
| `studentAssessment` | `date_submitted` | int | Soumission relative à $J_0$ |
| `studentAssessment` | `is_banked` | int (0/1) | Résultat reporté d'une session précédente |
| `studentAssessment` | `score` | float \| null | Note 0–100 ; `null` si non notée |
| `studentVle` | `date` | int | Jour relatif à $J_0$ (négatif = pré-rentrée) |
| `studentVle` | `sum_click` | int | Nombre de clics ce jour-là |

---

## 3. Tables nettoyées (`data/processed/`)

Le pipeline [`ingestion/clean_oulad.py`](../ingestion/clean_oulad.py) applique
les règles suivantes : aucune ligne supprimée, sentinelles `?` converties en
`null`, typage nullable, dates négatives conservées.

| Fichier | Lignes | Colonnes | Typage notable |
|---|---|---|---|
| `courses.csv` | 22 | 3 | `module_presentation_length` : `int64` |
| `assessments.csv` | 206 | 6 | `date` : `Int64` (nullable) |
| `vle.csv` | 6 364 | 6 | `week_from`, `week_to` : `Int64` |
| `studentInfo.csv` | 32 593 | 11 | `imd_band` : string + `NaN` ; **sans `final_result`** |
| `studentRegistration.csv` | 32 593 | 5 | `date_*` : `Int64` |
| `studentAssessment.csv` | 173 912 | 5 | `score` : `Float64` (nullable) |
| `studentVle.csv` | 10 655 280 | 6 | tous les identifiants/dates : `int64` |

### `student_target_labels.csv` — cible ML isolée

| Colonne | Type | Description |
|---|---|---|
| `code_module` | string | Module |
| `code_presentation` | string | Session |
| `id_student` | int64 | Étudiant |
| `final_result` | string | Résultat final OULAD |
| `is_dropout` | int (0/1) | `1` si `final_result == "Withdrawn"`, sinon `0` |

> ⚠️ Ce fichier ne doit **jamais** être publié dans Kafka ni joint aux features
> avant l'étape d'évaluation ML (prévention du data leakage).

---

## 4. Correspondance vers Kafka

| Table nettoyée | Topic | Clé Kafka | Filtre / transformation |
|---|---|---|---|
| `studentVle.csv` | `clics_vle` | `id_student` | 1 ligne → 1 événement |
| `studentAssessment.csv` + `assessments.csv` | `evaluations` | `id_student` | Jointure sur `id_assessment`, ajout `assessment_type` / `weight` |

> ℹ️ `studentInfo.csv` n'est **pas** un flux Kafka. C'est un référentiel
> statique chargé directement par le Lakehouse depuis `data/processed/`
> (`final_result` exclu, `imd_band` nullable).

---

## 5. Qualité des données

| Contrôle | Attendu |
|---|---|
| Lignes perdues au nettoyage | `0` pour les 7 tables |
| Dates négatives | conservées (activité pré-rentrée) |
| `score` manquant | conservé en `null` (jamais `0`) |
| `date_unregistration` manquant | conservé en `null` (pas d'abandon) |
| `imd_band` manquant | conservé en `null` |
| `final_result` dans un flux | interdit par les schémas et les tests |

### Valeurs de référence OULAD

- Inscriptions : 32 593
- Interactions VLE : 10 655 280
- Évaluations : 206
- Soumissions : 173 912
- Ressources VLE : 6 364
- Présentations : 22

---

## 6. Relations

```text
courses (code_module, code_presentation)
   │
   ├── assessments (id_assessment)
   │        │
   │        └── studentAssessment (id_student)
   │
   ├── vle (id_site)
   │        │
   │        └── studentVle (id_student, date)
   │
   ├── studentInfo (id_student)  ──► cible ML
   │
   └── studentRegistration (id_student)
```

`id_student` identifie un étudiant **au sein d'une inscription** : un même
étudiant peut apparaître dans plusieurs modules/présentations. Les jointures
doivent donc inclure `code_module` et `code_presentation`.
