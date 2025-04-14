import pandas as pd
import os
import re
import psycopg2

# Configuration
datasets_folder = "./DATASETS"
temp_csv = "temp_statistique.csv"

connection_params = {
    "dbname": "mspr_etl",
    "user": "postgres",
    "password": "P@ssw0rd",
    "host": "localhost",
    "port": "5432"
}

maladies_mapping = {
    "covid": "COVID-19",
    "coronavirus": "COVID-19",
    "covid19": "COVID-19",
    "monkeypox": "Monkeypox",
    "mpox": "Monkeypox",
    "ebola": "Ebola Virus Disease"
}

standard_columns = ["country", "date", "confirmed", "deaths", "recovered", "active", "new_cases", "new_deaths"]


def normalize_column_name(col):
    return re.sub(r"[^a-z0-9]+", "_", col.strip().lower())


def apply_flexible_mapping(df):
    column_synonyms = {
        "country": ["country", "location", "region", "Country/Region"],
        "date": ["date", "observation_date", "report_date"],
        "confirmed": ["confirmed", "total_cases", "cases"],
        "deaths": ["deaths", "total_deaths", "fatalities"],
        "recovered": ["recovered", "total_recoveries", "Recoveries"],
        "active": ["active", "active_cases"],
        "new_cases": ["new_cases", "daily_confirmed", "cases_new"],
        "new_deaths": ["new_deaths", "daily_deaths"]
    }
    mapping = {}
    normalized_cols = {normalize_column_name(col): col for col in df.columns}

    for std_col, synonyms in column_synonyms.items():
        for syn in synonyms:
            if normalize_column_name(syn) in normalized_cols:
                mapping[normalized_cols[normalize_column_name(syn)]] = std_col
                break

    df = df.rename(columns=mapping)
    return df


def extract(file_path):
    return pd.read_csv(file_path) if file_path.endswith(".csv") else pd.read_json(file_path)


def detect_maladie(file_name):
    name = file_name.lower()
    for keyword, maladie in maladies_mapping.items():
        if keyword in name:
            return maladie
    return "Inconnue"


def complete_missing_columns(df):
    for col in standard_columns:
        if col not in df.columns:
            if col == "country":
                df[col] = "Unknown"
            elif col == "date":
                df[col] = pd.Timestamp.now()
            else:
                df[col] = 0
    return df


def transform(df):
    df = apply_flexible_mapping(df)
    df = complete_missing_columns(df)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    if df["new_cases"].isnull().all():
        df["new_cases"] = df.groupby("country")["confirmed"].diff().fillna(0)
    if df["new_deaths"].isnull().all():
        df["new_deaths"] = df.groupby("country")["deaths"].diff().fillna(0)

    return df


def connect_db():
    return psycopg2.connect(**connection_params)


def prepare_temp_csv(all_rows):
    df_temp = pd.DataFrame(all_rows, columns=["id_maladie", "id_region", "date", "nouveau_mort", "nouveau_cas", "total_mort"])
    df_temp = df_temp.drop_duplicates(subset=["id_region", "date"], keep="last")
    df_temp.to_csv(temp_csv, index=False, header=False)


def copy_into_temp_statistique():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS temp_statistique;
        CREATE TEMP TABLE temp_statistique (
            id_maladie INTEGER,
            id_region INTEGER,
            date DATE,
            nouveau_mort INTEGER,
            nouveau_cas INTEGER,
            total_mort INTEGER
        );
    """)
    conn.commit()

    with open(temp_csv, "r") as f:
        cur.copy_expert("""
            COPY temp_statistique(id_maladie, id_region, date, nouveau_mort, nouveau_cas, total_mort)
            FROM STDIN WITH CSV
        """, f)
    conn.commit()

    cur.execute("""
        INSERT INTO statistique (id_maladie, id_region, date, nouveau_mort, nouveau_cas, total_mort)
        SELECT id_maladie, id_region, date, nouveau_mort, nouveau_cas, total_mort
        FROM temp_statistique
        ON CONFLICT (id_region, date) DO UPDATE SET
            nouveau_mort = EXCLUDED.nouveau_mort,
            nouveau_cas = EXCLUDED.nouveau_cas,
            total_mort = EXCLUDED.total_mort;
    """)
    conn.commit()
    cur.close()
    conn.close()


def run_etl():
    conn = connect_db()
    cur = conn.cursor()
    all_rows = []

    for file_name in os.listdir(datasets_folder):
        if file_name.endswith((".csv", ".json")):
            file_path = os.path.join(datasets_folder, file_name)
            maladie_name = detect_maladie(file_name)

            print(f"\n📄 Traitement: {file_name} ➞ Maladie: {maladie_name}")
            df_raw = extract(file_path)
            df_clean = transform(df_raw)

            cur.execute("SELECT id_maladie FROM maladie WHERE nom_maladie = %s", (maladie_name,))
            maladie_record = cur.fetchone()
            if maladie_record:
                id_maladie = maladie_record[0]
            else:
                cur.execute("INSERT INTO maladie (nom_maladie) VALUES (%s) RETURNING id_maladie", (maladie_name,))
                id_maladie = cur.fetchone()[0]

            for _, row in df_clean.iterrows():
                country_name = row["country"]
                cur.execute("SELECT id_pays FROM pays WHERE nom_pays = %s", (country_name,))
                pays_record = cur.fetchone()
                if pays_record:
                    id_pays = pays_record[0]
                else:
                    cur.execute("INSERT INTO pays (nom_pays) VALUES (%s) RETURNING id_pays", (country_name,))
                    id_pays = cur.fetchone()[0]

                cur.execute("SELECT id_region FROM region WHERE nom_region = %s", (country_name,))
                region_record = cur.fetchone()
                if region_record:
                    id_region = region_record[0]
                else:
                    cur.execute("INSERT INTO region (nom_region, id_pays) VALUES (%s, %s) RETURNING id_region", (country_name, id_pays))
                    id_region = cur.fetchone()[0]

                nouveau_mort = int(row["new_deaths"]) if not pd.isna(row["new_deaths"]) else 0
                nouveau_cas = int(row["new_cases"]) if not pd.isna(row["new_cases"]) else 0
                total_mort = int(row["deaths"]) if not pd.isna(row["deaths"]) else 0

                all_rows.append((id_maladie, id_region, row["date"], nouveau_mort, nouveau_cas, total_mort))

    prepare_temp_csv(all_rows)
    copy_into_temp_statistique()

    if os.path.exists(temp_csv):
        os.remove(temp_csv)

    cur.close()
    conn.close()
    print("\n🎉 Tous les fichiers ont été traités et insérés avec succès via COPY et table temporaire!")


if __name__ == "__main__":
    run_etl()
