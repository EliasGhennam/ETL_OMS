import pandas as pd
import os
import re
import time
import psycopg2
import io

# Configuration
datasets_folder = "./DATASETS"
temp_csv = "temp_statistique.csv"
connection_params = {
    "dbname": "bpziqzdsvdgpbxyvg2qg",
    "user": "uzjzegjp9kw0jmrmjr0s",
    "password": "7LOGZG8w6D929HCLAEYyKI86SA14Xd",
    "host": "bpziqzdsvdgpbxyvg2qg-postgresql.services.clever-cloud.com",
    "port": "50013"
}

maladies_mapping = {
    "covid": "COVID-19", "coronavirus": "COVID-19", "covid19": "COVID-19",
    "monkeypox": "Monkeypox", "mpox": "Monkeypox",
    "ebola": "Ebola Virus Disease"
}

# Colonnes standard attendues
standard_columns = [
    "country", "date", "confirmed", "deaths", "recovered", "active",
    "new_cases", "new_deaths", "latitude", "longitude"
]

def normalize_column_name(col):
    return re.sub(r"[^a-z0-9]+", "_", col.strip().lower())

def apply_flexible_mapping(df):
    # Synonymes pour mapper vers nos champs
    column_synonyms = {
        "country": ["country", "location", "region", "country_region", "province_state"],
        "date": ["date", "observation_date", "report_date"],
        "confirmed": ["confirmed", "total_cases", "cases"],
        "deaths": ["deaths", "total_deaths", "fatalities"],
        "recovered": ["recovered", "total_recoveries", "recoveries"],
        "active": ["active", "active_cases"],
        "new_cases": ["new_cases", "daily_confirmed", "cases_new", "new_cases_smoothed"],
        "new_deaths": ["new_deaths", "daily_deaths", "new_deaths_smoothed"],
        "latitude": ["lat", "latitude"],
        "longitude": ["long", "longitude"]
    }
    mapping = {}
    normalized_cols = {normalize_column_name(c): c for c in df.columns}

    for std_col, synonyms in column_synonyms.items():
        for syn in synonyms:
            n = normalize_column_name(syn)
            if n in normalized_cols:
                mapping[normalized_cols[n]] = std_col
                break

    return df.rename(columns=mapping)

def extract(fp):
    return pd.read_csv(fp) if fp.lower().endswith(".csv") else pd.read_json(fp)

def detect_maladie(fname):
    n = fname.lower()
    for k, v in maladies_mapping.items():
        if k in n:
            return v
    return "Inconnue"

def get_population(country_name, cur):
    """À implémenter : retourner la population de la région/pays depuis la BD"""
    # Ex : cur.execute("SELECT population FROM region r JOIN pays p ON r.id_pays=p.id_pays WHERE p.nom_pays=%s", (country_name,))
    #      return cur.fetchone()[0] or None
    return None

def complete_missing_columns(df):
    for col in standard_columns:
        if col not in df.columns:
            df[col] = pd.NA if col in ("latitude","longitude") else 0
    return df

def transform(df, cur):
    df = apply_flexible_mapping(df)
    df = complete_missing_columns(df)

    # 🗓️ Conversion robuste des dates
    try:
        # On tente d'abord le format ISO explicite
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="raise")
    except Exception:
        # Sinon fallback plus souple avec dayfirst désactivé
        df["date"] = pd.to_datetime(df["date"], dayfirst=False, errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["date"] >= pd.Timestamp("2019-01-01")]

    # 🧮 Conversion per_100k ou per_million en valeurs absolues
    pop = None
    if "country" in df.columns:
        pop = get_population(df["country"].iloc[0], cur)

    for col in df.columns:
        if "per_100_000" in col or "per_100k" in col:
            rate = pd.to_numeric(df[col], errors="coerce")
            if pop:
                abs_col = rate * pop / 100_000
                if "excess_deaths" in col:
                    df["deaths"] = abs_col.round().astype(int)
                else:
                    df["confirmed"] = abs_col.round().astype(int)

        if "per_million" in col:
            rate = pd.to_numeric(df[col], errors="coerce")
            if pop:
                abs_col = rate * pop / 1_000_000
                if "deaths" in col:
                    df["deaths"] = abs_col.round().astype(int)
                else:
                    df["confirmed"] = abs_col.round().astype(int)

    # 📈 Calcul des cas/morts journaliers si pas dispo
    if (df["new_cases"] == 0).all():
        df["new_cases"] = df.groupby("country")["confirmed"].diff().fillna(0).astype(int)
    if (df["new_deaths"] == 0).all():
        df["new_deaths"] = df.groupby("country")["deaths"].diff().fillna(0).astype(int)

    # 🌍 Nettoyage coordonnées géographiques
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").round(6)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").round(6)

    return df

def connect_db():
    return psycopg2.connect(**connection_params)

def prepare_temp_csv(rows):
    df = pd.DataFrame(rows, columns=[
        "id_maladie","id_region","date","nouveau_mort","nouveau_cas",
        "total_mort","total_cas"
    ])
    df = df.groupby(["id_region","date"], as_index=False).agg({
        "id_maladie":"first",
        "nouveau_mort":"sum",
        "nouveau_cas":"sum",
        "total_mort":"max",
        "total_cas":"max"
    })
    df = df[["id_region","date","id_maladie","nouveau_mort","nouveau_cas","total_mort","total_cas"]]
    df.to_csv(temp_csv, index=False, header=False)

def copy_into_temp_statistique(cur=None, conn=None):
    close_after = False

    # Si aucun curseur ou connexion n’a été fourni, on ouvre manuellement
    if cur is None or conn is None:
        conn = connect_db()
        cur = conn.cursor()
        close_after = True

    cur.execute("""
        DROP TABLE IF EXISTS temp_statistique;
        CREATE TEMP TABLE temp_statistique (
            id_region INTEGER,
            date DATE,
            id_maladie INTEGER,
            nouveau_mort INTEGER,
            nouveau_cas INTEGER,
            total_mort INTEGER,
            total_cas INTEGER
        );
    """)
    conn.commit()

    print("⏳ Copie dans temp_statistique en cours...")
    with open(temp_csv, "r") as f:
        cur.copy_expert("""
            COPY temp_statistique(id_region, date, id_maladie, nouveau_mort, nouveau_cas, total_mort, total_cas)
            FROM STDIN WITH CSV
        """, f)
    conn.commit()
    print("✅ Copie terminée !")

    cur.execute("""
        INSERT INTO statistique (id_maladie, id_region, date, nouveau_mort, nouveau_cas, total_mort, total_cas)
        SELECT id_maladie, id_region, date, nouveau_mort, nouveau_cas, total_mort, total_cas
        FROM temp_statistique
        ON CONFLICT (id_region, date) DO UPDATE
          SET nouveau_mort = EXCLUDED.nouveau_mort,
              nouveau_cas = EXCLUDED.nouveau_cas,
              total_mort = EXCLUDED.total_mort,
              total_cas = EXCLUDED.total_cas;
    """)
    conn.commit()

    if close_after:
        cur.close()
        conn.close()

def run_etl():
    conn = connect_db()
    conn.autocommit = False  # On gère la transaction manuellement
    cur = conn.cursor()

    # Chargement mapping BD existant
    cur.execute("SELECT id_maladie, nom_maladie FROM maladie")
    maladie_dict = {n: i for i, n in cur.fetchall()}
    cur.execute("SELECT id_pays, nom_pays FROM pays")
    pays_dict = {n: i for i, n in cur.fetchall()}
    cur.execute("SELECT id_region, nom_region FROM region")
    region_dict = {n: i for i, n in cur.fetchall()}

    all_rows = []  # ✅ initialise la liste des lignes à insérer

    new_pays = {}
    new_regions = {}
    latlong_updates = {}  # ✅ On le déclare ici une seule fois pour tout le run

    for fn in os.listdir(datasets_folder):
        if not fn.lower().endswith((".csv", ".json")):
            continue
        path = os.path.join(datasets_folder, fn)
        mal = detect_maladie(fn)
        print(f"📄 {fn} → {mal}")
        df_raw = extract(path)
        df = transform(df_raw, cur)

        # Upsert maladie
        if mal not in maladie_dict:
            cur.execute("INSERT INTO maladie(nom_maladie) VALUES(%s) RETURNING id_maladie", (mal,))
            maladie_dict[mal] = cur.fetchone()[0]

        id_maladie = maladie_dict[mal]

        for _, r in df.iterrows():
            country = r["country"]

            # Mémorise les pays inconnus
            if country not in pays_dict and country not in new_pays:
                new_pays[country] = None

    # Batch insert des nouveaux pays
    for country in new_pays:
        cur.execute("INSERT INTO pays(nom_pays) VALUES(%s) RETURNING id_pays", (country,))
        new_pays[country] = cur.fetchone()[0]
    pays_dict.update(new_pays)

    # Relecture fichier + traitement final avec pays/régions
    for fn in os.listdir(datasets_folder):
        if not fn.lower().endswith((".csv", ".json")):
            continue
        path = os.path.join(datasets_folder, fn)
        mal = detect_maladie(fn)
        df_raw = extract(path)
        df = transform(df_raw, cur)
        id_maladie = maladie_dict[mal]

        for _, r in df.iterrows():
            country = r["country"]
            id_pays = pays_dict[country]

            # Upsert region (1 seule fois par nom)
            if country not in region_dict and country not in new_regions:
                cur.execute(
                    "INSERT INTO region(nom_region, id_pays) VALUES(%s, %s) RETURNING id_region",
                    (country, id_pays)
                )
                new_regions[country] = cur.fetchone()[0]

            id_region = region_dict.get(country) or new_regions[country]

            # maj lat/long
            if pd.notna(r["latitude"]) and pd.notna(r["longitude"]) and id_region not in latlong_updates:
                latlong_updates[id_region] = (r["latitude"], r["longitude"])

            # valeurs
            nm = int(r["new_deaths"]) if not pd.isna(r["new_deaths"]) else 0
            nc = int(r["new_cases"]) if not pd.isna(r["new_cases"]) else 0
            tm = int(r["deaths"]) if not pd.isna(r["deaths"]) else 0
            tc = int(r["confirmed"]) if not pd.isna(r["confirmed"]) else 0

            all_rows.append((id_maladie, id_region, r["date"].date(), nm, nc, tm, tc))

    region_dict.update(new_regions)
    cur.execute("SELECT id_region, nom_region FROM region")
    region_dict = {n: i for i, n in cur.fetchall()}  # 🔁 recharge depuis BDD

    if latlong_updates:
        print(f"📌 Mise à jour de {len(latlong_updates)} régions avec coordonnées GPS...")
        update_query = """
            UPDATE region SET
                latitude = CASE id_region
                    {lat_cases}
                END,
                longitude = CASE id_region
                    {long_cases}
                END
            WHERE id_region IN ({ids}) AND (latitude IS NULL OR longitude IS NULL)
        """
        lat_cases = "\n".join([f"WHEN {id_} THEN {latlong_updates[id_][0]}" for id_ in latlong_updates])
        long_cases = "\n".join([f"WHEN {id_} THEN {latlong_updates[id_][1]}" for id_ in latlong_updates])
        ids = ",".join(map(str, latlong_updates.keys()))
        cur.execute(update_query.format(lat_cases=lat_cases, long_cases=long_cases, ids=ids))

    all_rows = [row for row in all_rows if row[1] in region_dict.values()]
    if not all_rows:
        print("⚠️ Aucun enregistrement valide à insérer (tous les id_region ont été filtrés).")
        cur.close()
        conn.close()
        return

    prepare_temp_csv(all_rows)
    copy_into_temp_statistique(cur, conn)
    os.remove(temp_csv)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ ETL terminé avec traitement optimisé !")

if __name__ == "__main__":
        start = time.time()
        run_etl()
        print(f"⏱️ Terminé en {round(time.time() - start, 2)} secondes")