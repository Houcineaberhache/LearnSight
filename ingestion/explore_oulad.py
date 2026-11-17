import pandas as pd
from pathlib import Path

def find_default_data_dir() -> Path:
    candidates = [
        Path("data/raw"),
        Path("../data/raw"),
        Path("Data"),
        Path("../Data")
    ]
    for c in candidates:
        if c.exists() and (c / "studentVle.csv").exists():
            return c
    return Path("data/raw")

DATA_DIR = find_default_data_dir()

files = [
    "studentVle.csv",
    "studentAssessment.csv",
    "assessments.csv",
    "studentInfo.csv",
    "studentRegistration.csv",
    "courses.csv",
    "vle.csv"
]

print(f"Audit des données depuis : {DATA_DIR.resolve()}\n")

for filename in files:
    path = DATA_DIR / filename

    print("\n" + "=" * 60)
    print(f"FICHIER : {filename}")
    print("=" * 60)

    if not path.exists():
        print("Fichier introuvable !")
        continue

    df = pd.read_csv(path)

    print("Dimensions :", df.shape)
    print("\nColonnes :")
    print(df.dtypes)

    print("\nValeurs manquantes :")
    print(df.isnull().sum())

    print("\nExemples :")
    print(df.head(3))
