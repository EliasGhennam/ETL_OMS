# build_dataset.py

import pandas as pd
import psycopg2
import os
import joblib
import pickle

connection_params = {
    "dbname": "bpziqzdsvdgpbxyvg2qg",
    "user": "uzjzegjp9kw0jmrmjr0s",
    "password": "7LOGZG8w6D929HCLAEYyKI86SA14Xd",
    "host": "bpziqzdsvdgpbxyvg2qg-postgresql.services.clever-cloud.com",
    "port": "50013"
}

def get_data_from_db():
    conn = psycopg2.connect(**connection_params)
    query = """
        SELECT 
            s.date,
            s.id_maladie,
            s.id_region,
            s.nouveau_cas,
            s.nouveau_mort,
            s.total_cas,
            s.total_mort,
            s.stringency_index,
            s.vaccinated,
            s.hospital_beds_per_thousand,
            s.population_density,
            p.nom_pays,
            m.nom_maladie,
            r.nom_region
        FROM statistique s
        JOIN region r ON s.id_region = r.id_region
        JOIN pays p ON r.id_pays = p.id_pays
        JOIN maladie m ON s.id_maladie = m.id_maladie
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df



def build_training_data(force_refresh=False):

    cache_path = "data_sources/cache_dataset.pkl"

    if not force_refresh and os.path.exists(cache_path):
        print("✅ Chargement du dataset depuis le cache local...")
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    print("🔄 Génération du dataset à partir des sources (BDD + CSV)...")
    df = get_data_from_db()
    print(f"🗓️ Date min : {df['date'].min()}, date max : {df['date'].max()}")
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['id_region', 'id_maladie', 'date'])

    # ➕ Ajout des lags (cas_j-1, cas_j-2, ...)
    df['cas_j-1'] = df.groupby(['id_region', 'id_maladie'])['nouveau_cas'].shift(1)
    df['cas_j-2'] = df.groupby(['id_region', 'id_maladie'])['nouveau_cas'].shift(2)
    df['cas_j-3'] = df.groupby(['id_region', 'id_maladie'])['nouveau_cas'].shift(3)
    df['target'] = df.groupby(['id_region', 'id_maladie'])['nouveau_cas'].shift(-7)

    # 🔍 Détection automatique du fichier mobilité
    weather_path = None
    for f in os.listdir("data_sources"):
        if "weather" in f.lower() and f.endswith(".csv"):
            weather_path = os.path.join("data_sources", f)
            print(f"📂 Fichier météo détecté : {weather_path}")
            break

    if not weather_path:
        raise FileNotFoundError("❌ Aucun fichier météo trouvé dans 'data_sources/'. Il doit contenir 'weather' dans son nom.")


    # 🔍 Détection automatique du fichier météo
    mobility_path = None
    for f in os.listdir("data_sources"):
        if "mobility" in f.lower() and f.endswith(".csv"):
            mobility_path = os.path.join("data_sources", f)
            print(f"📂 Fichier de mobilité détecté : {mobility_path}")
            break

    if not mobility_path:
        raise FileNotFoundError("❌ Aucun fichier de mobilité trouvé dans 'data_sources/'. Il doit contenir 'mobility' dans son nom.")
    
    df_mob = pd.read_csv(mobility_path, parse_dates=["date"])
    df_mob = df_mob.rename(columns={
        "country_region": "pays",
        "workplaces_percent_change_from_baseline": "work_mob",
        "transit_stations_percent_change_from_baseline": "transit_mob",
        "residential_percent_change_from_baseline": "home_mob"
    })
    df_mob = df_mob[["pays", "date", "work_mob", "transit_mob", "home_mob"]]

    df_meteo_raw = pd.read_csv(weather_path, sep=None, engine='python', encoding='utf-8')
    df_meteo = df_meteo_raw.iloc[:, :9]
    df_meteo.columns = ["id", "province", "pays", "lat", "long", "date", "confirmed", "deaths", "day_from_jan"]
    df_meteo["temperature"] = df_meteo["lat"].apply(lambda x: 35 - abs(x - 20))
    df_meteo["humidity"] = 60 + (df_meteo["long"] % 40)
    df_meteo = df_meteo[["pays", "date", "temperature", "humidity"]]
    df_meteo["date"] = pd.to_datetime(df_meteo["date"], dayfirst=True)

    # 🧬 Fusion
    df = df.merge(df_mob, left_on=["nom_pays", "date"], right_on=["pays", "date"], how="left")
    df = df.merge(df_meteo, left_on=["nom_pays", "date"], right_on=["pays", "date"], how="left")

    # Nettoyage final
    df = df.dropna(subset=["cas_j-1", "cas_j-2", "cas_j-3", "target"])

    # Remplir les colonnes OWID avec des valeurs par défaut si manquantes
    df["stringency_index"] = df["stringency_index"].fillna(0)
    df["vaccinated"] = df["vaccinated"].fillna(0)
    df["hospital_beds_per_thousand"] = df["hospital_beds_per_thousand"].fillna(df["hospital_beds_per_thousand"].median())
    df["population_density"] = df["population_density"].fillna(df["population_density"].median())

    # ❗ Supprimer les lignes où target = 0, car inutiles pour l'apprentissage
    initial_len = len(df)
    df = df[df["target"] > 0]
    print(f"🧹 Lignes conservées après suppression des target = 0 : {len(df)} / {initial_len}")

    # Supprime les lignes où le nombre de cas futurs (target) est nul
    df = df[df["target"] > 0]

    # Vérifie la date maximale pour chaque couple (id_region, id_maladie)
    last_dates = df.groupby(["id_region", "id_maladie"])["date"].max()
    print("\n📅 10 dernières dates exploitables par groupe :")
    print(last_dates.sort_values().tail(10))

    print("💾 Sauvegarde du dataset en cache...")
    with open(cache_path, "wb") as f:
        pickle.dump(df, f)

    return df
