import pandas as pd
import psycopg2

connection_params = {
    "dbname": "bpziqzdsvdgpbxyvg2qg",
    "user": "uzjzegjp9kw0jmrmjr0s",
    "password": "7LOGZG8w6D929HCLAEYyKI86SA14Xd",
    "host": "bpziqzdsvdgpbxyvg2qg-postgresql.services.clever-cloud.com",
    "port": "50013"
}

def export_to_csv():
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
    
    df.to_csv("export/statistique_full_export.csv", index=False)
    print("✅ Export terminé : export/statistique_full_export.csv")

if __name__ == "__main__":
    export_to_csv()
