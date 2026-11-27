# Spark & ML (Student 3)

This part of LearnSight predicts which students are likely to drop out of a course.

## How it works

Every week, we look at what a student did so far (clicks, assignments) and give them a **risk score**. Teachers can then contact the highest-risk students first.

- **What we predict:** `is_dropout` (1 if the student withdrew). It comes from `student_target_labels.csv` and is never used as an input, because that would be cheating.
- **When we predict:** at a chosen week `W` (default 8). We only use data from before that week.
- **Who is included:** students still enrolled at week `W`. Students who already left are skipped, because we already know their outcome.

## The scripts

| Script | What it does |
|---|---|
| `features/compute_features.py` | Builds one row per student with numbers describing their behavior: clicks, active days, missed assignments, average score... |
| `ml/train_model.py` | Trains two models (Logistic Regression and Random Forest), tests them, and saves the model and the metrics |
| `ml/run_predictions.py` | Loads the saved model and gives each student a risk score and a risk group (1 = highest risk) |
| `ml/publish_predictions.py` | Writes the scores to the Gold layer so the dashboard can show them |

Every script accepts `--date YYYY-MM-DD`, as the Airflow pipeline expects.

## Run it on your computer

```bash
python spark/features/compute_features.py --date 2026-11-25 --week 8
python spark/ml/train_model.py
python spark/ml/run_predictions.py --date 2026-11-25 --week 8
pytest spark/tests -v
```

Results are saved in `data/features/`, `models/` and `data/predictions/`. Git ignores these folders.

## Results (week 8, tested on the 2014J course period)

We use PR-AUC to measure quality. A model guessing at random scores 0.16, and a perfect one scores 1.

| Model | PR-AUC |
|---|---|
| Random guessing | 0.163 |
| Logistic Regression | 0.261 |
| Random Forest | 0.267 |

Both models do better than guessing, but the signal is moderate. Predicting dropout from the first 8 weeks is hard.

In practice: the 20% of students with the highest scores really dropped out 30% of the time, against 6% for the 20% with the lowest scores.

> The score is a **ranking**, not a percentage chance. It tells us who to look at first. It does not judge any student.

## Setup (Windows)

You need Python 3.11, Java 17, and `pyspark==3.5.3`, `numpy<2`, `pandas<2.3`.

To save or load a model on Windows, put `winutils.exe` and `hadoop.dll` in `D:\hadoop\bin`. Then, in each new Git Bash terminal, run:

```bash
export HADOOP_HOME='D:\hadoop'
export PATH="$PATH:/d/hadoop/bin"
export PYSPARK_PYTHON='D:\LearnSight\.venv\Scripts\python.exe'
export PYSPARK_DRIVER_PYTHON='D:\LearnSight\.venv\Scripts\python.exe'
```

## What is not done yet

- The features are built from the CSV files in `data/processed/`. Switching to Student 2's Silver/Gold tables is still to do.
- `publish_predictions.py` has not been tested yet, because it needs MinIO and the Hudi packages.
- The trend features (`clicks_trend`, `weeks_inactive`) have no unit tests yet.