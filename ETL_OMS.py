import pandas as pd
import os
import re
import json
import argparse

# Configuration
result_path_csv = "./Résultat de l'ETL/final.csv"
result_path_json = "./Résultat de l'ETL/final.json"

# Synonymes possibles pour des colonnes hétérogènes (plus large)
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

indicator_mapping = {
    "confirmed": {"indicator": "confirmed", "unit": "cases"},
    "deaths": {"indicator": "deaths", "unit": "deaths"},
    "recovered": {"indicator": "recovered", "unit": "patients"},
    "active": {"indicator": "active", "unit": "patients"},
    "new_cases": {"indicator": "new_cases", "unit": "cases"},
    "new_deaths": {"indicator": "new_deaths", "unit": "deaths"}
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
    df["date"] = pd.to_datetime(df.get("date"), errors='coerce')
    df = df.dropna(subset=["date"])
    df["pandemic"] = pandemic_name

    for col in ["confirmed", "deaths", "recovered", "active", "new_cases", "new_deaths"]:
        if col not in df.columns:
            df[col] = None

    df = df.sort_values(by=["country", "date"])
    if "new_cases" in df.columns and df["new_cases"].isnull().all() and "confirmed" in df.columns:
        df["new_cases"] = df.groupby("country")["confirmed"].diff().fillna(0)

    if "new_deaths" in df.columns and df["new_deaths"].isnull().all() and "deaths" in df.columns:
        df["new_deaths"] = df.groupby("country")["deaths"].diff().fillna(0)

    df_long = df.melt(
        id_vars=["country", "date", "pandemic"],
        value_vars=["confirmed", "deaths", "recovered", "active", "new_cases", "new_deaths"],
        var_name="indicator",
        value_name="value"
    )

    df_long["unit"] = df_long["indicator"].map(lambda x: indicator_mapping.get(x, {}).get("unit", "unknown"))
    df_long = df_long.dropna(subset=["value", "date"])
    return df_long

def run_etl(file_path, pandemic_name):
    print(f"\n📄 Traitement du fichier : {file_path}")
    try:
        df_raw = extract(file_path)
        print("Colonnes d'origine :", df_raw.columns.tolist())
        df_clean = transform(df_raw, pandemic_name)
        os.makedirs(os.path.dirname(result_path_csv), exist_ok=True)
        df_clean.to_csv(result_path_csv, index=False)
        df_clean.to_json(result_path_json, orient="records", date_format="iso")
        print("\n✅ Fichier final CSV sauvegardé dans :", result_path_csv)
        print("✅ Fichier final JSON sauvegardé dans :", result_path_json)
        print(df_clean.head())
    except Exception as e:
        print(f"❌ Erreur lors du traitement de {file_path} : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pandémie")
    parser.add_argument("--file", type=str, help="Chemin du fichier à traiter")
    parser.add_argument("--pandemic_name", type=str, help="Nom de la pandémie", default="pandemic")
    args = parser.parse_args()

    # Mode test manuel local si aucun argument fourni
    if not args.file:
        print("⚠️ Aucune entrée fournie. Passage en mode TEST LOCAL...")
        test_file = "./DATASETS/owid-monkeypox-data.csv"
        test_name = "Monkeypox Test"
        run_etl(test_file, test_name)
    else:
        run_etl(args.file, args.pandemic_name)
