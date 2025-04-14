import pandas as pd
import os
import re
import json
import argparse

# Configuration
result_folder = "./Résultat de l'ETL"
os.makedirs(result_folder, exist_ok=True)

# Synonymes possibles pour des colonnes hétérogènes
standard_column_map = {
    "country": ["location", "country", "Country/Region", "Country", "nation", "region"],
    "date": ["date", "Date", "date_reported", "date_of_observation", "report_date", "observation_date"],
    "confirmed": ["confirmed", "total_cases", "ConfirmedCases", "TotalCases", "case_count", "cases", "cases_total"],
    "deaths": ["deaths", "total_deaths", "Deaths", "Fatalities", "TotalDeaths", "dead", "deathcount"],
    "recovered": ["recovered", "Recoveries", "Recovered", "total_recovered"],
    "active": ["active", "ActiveCases", "currently_active", "Active", "ongoing_cases"],
    "new_cases": ["new_cases", "Daily confirmed", "NewCases", "daily_confirmed", "cases_new", "confirmed_today"],
    "new_deaths": ["new_deaths", "Daily deaths", "NewDeaths", "daily_deaths", "new_deaths_smoothed", "deaths_today"]
}

def extract(file_path):
    if file_path.endswith(".json"):
        return pd.read_json(file_path)
    else:
        return pd.read_csv(file_path)

def normalize_column_name(col):
    return re.sub(r"[^a-z0-9]+", "_", col.strip().lower())

def apply_flexible_mapping(df):
    mapping = {}
    normalized_cols = {normalize_column_name(col): col for col in df.columns}
    unmatched = set(df.columns)

    for standard, candidates in standard_column_map.items():
        for candidate in candidates:
            norm_candidate = normalize_column_name(candidate)
            if norm_candidate in normalized_cols:
                mapping[normalized_cols[norm_candidate]] = standard
                unmatched.discard(normalized_cols[norm_candidate])
                break

    if unmatched:
        print("🔎 Colonnes non reconnues :", list(unmatched))

    return df.rename(columns=mapping)

def transform(df, pandemic_name):
    df = apply_flexible_mapping(df)
    
    if "country" not in df.columns:
        raise Exception("La colonne 'country' est manquante après le mapping !")

    df["date"] = pd.to_datetime(df.get("date"), errors='coerce')
    df = df.dropna(subset=["date"])
    df["pandemic"] = pandemic_name

    for col in ["confirmed", "deaths", "recovered", "active", "new_cases", "new_deaths"]:
        if col not in df.columns:
            df[col] = None

    if "new_cases" in df.columns and df["new_cases"].isnull().all() and "confirmed" in df.columns:
        df["new_cases"] = df.groupby("country")["confirmed"].diff().fillna(0)

    if "new_deaths" in df.columns and df["new_deaths"].isnull().all() and "deaths" in df.columns:
        df["new_deaths"] = df.groupby("country")["deaths"].diff().fillna(0)

    # **Ajout clé ici : ne garder QUE les colonnes utiles**
    return df[["country", "date", "confirmed", "deaths", "recovered", "active", "new_cases", "new_deaths", "pandemic"]]


def create_tables(df, pandemic_name):
    countries = df[["country"]].drop_duplicates().reset_index(drop=True)
    countries["id_pays"] = countries.index + 1

    regions = countries.copy()
    regions["id_region"] = regions.index + 1
    regions["nom_region"] = regions["country"]
    regions = regions[["id_region", "nom_region", "id_pays"]]

    maladie = pd.DataFrame({
        "id_maladie": [1],
        "nom_maladie": [pandemic_name]
    })

    df = df.merge(countries, on="country")
    df = df.merge(regions, left_on=["id_pays", "country"], right_on=["id_pays", "nom_region"])

    df["id_maladie"] = 1   # <<< 🛠️ AJOUT DE CETTE LIGNE

    statistiques = df[["id_maladie", "id_region", "date", "new_deaths", "new_cases", "deaths"]]
    statistiques = statistiques.rename(columns={
        "deaths": "total_mort",
        "new_deaths": "nouveau_mort",
        "new_cases": "nouveau_cas"
    })

    return countries, regions, maladie, statistiques


def save_table(df, name):
    csv_path = os.path.join(result_folder, f"{name}.csv")
    json_path = os.path.join(result_folder, f"{name}.json")
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", date_format="iso")
    print(f"✅ Table {name} sauvegardée en CSV et JSON.")

def run_etl(file_path, pandemic_name):
    print(f"\n📄 Traitement du fichier : {file_path}")
    try:
        df_raw = extract(file_path)
        print("Colonnes d'origine :", df_raw.columns.tolist())
        df_clean = transform(df_raw, pandemic_name)
        countries, regions, maladie, statistiques = create_tables(df_clean, pandemic_name)

        save_table(countries, "Pays")
        save_table(regions, "Region")
        save_table(maladie, "Maladie")
        save_table(statistiques, "Statistique")

        print("\n✅ Toutes les tables ont été générées avec succès.")
    except Exception as e:
        print(f"❌ Erreur lors du traitement de {file_path} : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pandémie vers Base de Données")
    parser.add_argument("--file", type=str, help="Chemin du fichier à traiter")
    parser.add_argument("--pandemic_name", type=str, help="Nom de la pandémie", default="pandemic")
    args = parser.parse_args()

    if not args.file:
        print("⚠️ Aucune entrée fournie. Passage en mode TEST LOCAL...")
        test_file = "./DATASETS/owid-monkeypox-data.csv"
        test_name = "Monkeypox Test"
        run_etl(test_file, test_name)
    else:
        run_etl(args.file, args.pandemic_name)