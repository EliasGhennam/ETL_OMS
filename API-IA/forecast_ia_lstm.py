import pandas as pd
import torch
import torch.nn as nn
import joblib
from datetime import timedelta
import os


def generate_forecast():
    # Définir les features utilisées
    features = [
        "cas_j-1", "cas_j-2", "cas_j-3",
        "temperature", "humidity",
        "work_mob", "transit_mob", "home_mob",
        "stringency_index", "vaccinated",
        "hospital_beds_per_thousand", "population_density"
    ]

    # Charger le dataset cache complet
    df = pd.read_pickle("data_sources/cache_dataset.pkl")

    # Remplir les valeurs manquantes
    df[features] = df[features].fillna(0)

    # Ne garder que les données valides
    df = df.dropna(subset=features + ["target"])
    df = df[df["target"] > 0]

    # Marquer les données comme historiques
    df["source"] = "historique"
    df["date"] = pd.to_datetime(df["date"])

    # Prendre la dernière ligne par (id_region, id_maladie)
    df_latest = df.sort_values("date").groupby(["id_region", "id_maladie"]).tail(1).copy()

    # Charger le scaler et le modèle
    scaler = joblib.load("models/lstm_scaler.pkl")

    class LSTMModel(nn.Module):
        def __init__(self, input_size):
            super().__init__()
            self.lstm = nn.LSTM(input_size, hidden_size=64, batch_first=True)
            self.fc = nn.Linear(64, 1)

        def forward(self, x):
            out, _ = self.lstm(x)
            out = self.fc(out[:, -1, :])
            return out.squeeze()

    model = LSTMModel(input_size=len(features))
    model.load_state_dict(torch.load("models/model_lstm.pt", map_location=torch.device('cpu')))
    model.eval()

    # Lancer les prédictions pour 365 jours
    future_predictions = []

    for day in range(365):
        X = df_latest[features].values
        X_scaled = scaler.transform(X)
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32).unsqueeze(1)

        with torch.no_grad():
            preds = model(X_tensor).numpy()

        df_latest["nouveau_cas"] = preds
        df_latest["total_cas"] += preds
        df_latest["date"] += timedelta(days=1)
        df_latest["source"] = "prediction"

        for _, row in df_latest.iterrows():
            future_predictions.append({
                "id_maladie": row["id_maladie"],
                "id_region": row["id_region"],
                "date": row["date"],
                "nouveau_cas": round(row["nouveau_cas"]),
                "total_cas": round(row["total_cas"]),
                "nouveau_mort": 0,
                "total_mort": row.get("total_mort", 0),
                "source": row["source"],
                "cas_j-1": row["cas_j-1"],
                "cas_j-2": row["cas_j-2"],
                "cas_j-3": row["cas_j-3"],
                "temperature": row["temperature"],
                "humidity": row["humidity"],
                "work_mob": row["work_mob"],
                "transit_mob": row["transit_mob"],
                "home_mob": row["home_mob"],
                "stringency_index": row["stringency_index"],
                "vaccinated": row["vaccinated"],
                "hospital_beds_per_thousand": row["hospital_beds_per_thousand"],
                "population_density": row["population_density"]
            })

    # Combine les historiques + prédictions
    colonnes_finales = [
        "id_maladie", "id_region", "date",
        "nouveau_cas", "total_cas", "nouveau_mort", "total_mort", "source",
        "cas_j-1", "cas_j-2", "cas_j-3", "temperature", "humidity",
        "work_mob", "transit_mob", "home_mob",
        "stringency_index", "vaccinated",
        "hospital_beds_per_thousand", "population_density"
    ]
    df_all = pd.concat([
        df[colonnes_finales],
        pd.DataFrame(future_predictions)
    ])
    df_all.sort_values(by=["id_region", "id_maladie", "date"], inplace=True)

    from build_dataset import build_training_data

    # On recharge le cache complet, sans filtrage
    df_historical = build_training_data(force_refresh=False)
    df_historical["source"] = "historique"

    # Sauvegarde
    os.makedirs("generated_data", exist_ok=True)

    # Forcer le bon type sur toutes les colonnes numériques
    colonnes_float = [
        "cas_j-1", "cas_j-2", "cas_j-3",
        "temperature", "humidity",
        "work_mob", "transit_mob", "home_mob",
        "stringency_index", "vaccinated",
        "hospital_beds_per_thousand", "population_density",
        "nouveau_cas", "total_cas", "nouveau_mort", "total_mort"
    ]

    for col in colonnes_float:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0).astype(float)

    df_all.to_csv("generated_data/statistique_predict_lstm.csv", sep=",", index=False)
    print("✅ Fichier généré avec colonnes source=historique ou prediction")

    # Préparer un format JSON pour le front (ex: date et nouveau_cas)
    # On ne retourne que les prédictions (source == 'prediction')
    df_pred = df_all[df_all['source'] == 'prediction'].copy()
    # On ne garde que les colonnes utiles pour le chart
    df_pred = df_pred[['date', 'nouveau_cas']]
    df_pred['date'] = df_pred['date'].astype(str)
    result = df_pred.rename(columns={'nouveau_cas': 'valeur'}).to_dict(orient='records')
    return result


def main():
    """Fonction principale pour l'exécution en ligne de commande."""
    result = generate_forecast()
    print(f"✅ Prédictions générées : {len(result)} points de données")


# Permet d'appeler le script à la fois en CLI et comme module
if __name__ == "__main__":
    main()
