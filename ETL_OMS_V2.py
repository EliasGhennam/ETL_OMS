import pandas as pd
import os
import re

# Configuration
FINAL_PATH = "./Résultat de l'ETL/final.csv"
VISUAL_PATH = "./Résultat de l'ETL/visual.csv"

# Lecture du fichier final long format
df = pd.read_csv(FINAL_PATH)

# Nettoyage basique : on s'assure que les dates soient bien des datetime
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
else:
    raise ValueError("La colonne 'date' est manquante dans le fichier final.csv")

# Filtrage des colonnes clés
colonnes_essentielles = ["date", "country", "pandemic", "indicator", "value"]
for col in colonnes_essentielles:
    if col not in df.columns:
        raise ValueError(f"Colonne manquante : {col}")

# Pivot du format long vers format large : chaque ligne = 1 pays + 1 date + 1 pandémie
pivot_df = df.pivot_table(
    index=["date", "country", "pandemic"],
    columns="indicator",
    values="value",
    aggfunc="first"
).reset_index()

# Nettoyage des noms de colonnes (supprimer l’attribut name de l’index pivoté)
pivot_df.columns.name = None

# Valeurs manquantes → 0 par défaut pour une visualisation complète
pivot_df.fillna(0, inplace=True)

# Export vers CSV visualisable directement dans Power BI
os.makedirs(os.path.dirname(VISUAL_PATH), exist_ok=True)
pivot_df.to_csv(VISUAL_PATH, index=False)

print("✅ Fichier visual.csv généré avec succès dans :", VISUAL_PATH)
print(pivot_df.head())