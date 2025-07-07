import pandas as pd
import torch
import pickle
from train_ia_lstm import LSTMModel

# === CONFIGURATION DU SCÉNARIO ===
SCENARIO_NAME = "temperature_plus2"
TEMP_OFFSET = 2.0  # +2°C sur toute la période
WORK_MOB_MULTIPLIER = 1.0  # pas de changement ici pour l'instant
VACCIN_OFFSET = 0.0  # pas d'ajout de vaccination

# === CHARGEMENT DU MODÈLE ET SCALER ===
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("📟 Prédiction sur :", device)

model = LSTMModel(input_size=12).to(device)
model.load_state_dict(torch.load("models/model_lstm.pt", map_location=device))
model.eval()

with open("models/lstm_scaler.pkl", "rb") as f:
    scaler = pickle.load(f)

# === CHARGEMENT DES DONNÉES DE BASE ===
cache = pd.read_pickle("cache_dataset.pkl")
latest_df = cache.sort_values("date").groupby(["id_region", "id_maladie"]).tail(1).copy()

# === DUPLICATION POUR SIMULER 365 JOURS ===
dates = pd.date_range("2025-04-23", periods=365)
forecast_rows = []

for date in dates:
    for _, row in latest_df.iterrows():
        row_copy = row.copy()
        row_copy["date"] = date
        forecast_rows.append(row_copy)

df_forecast = pd.DataFrame(forecast_rows)

# === APPLICATION DU SCÉNARIO ===
df_forecast["temperature"] += TEMP_OFFSET
df_forecast["work_mob"] *= WORK_MOB_MULTIPLIER
df_forecast["vaccinated"] += VACCIN_OFFSET

# === PRÉPARATION DES FEATURES ===
features = [
    "cas_j-1", "cas_j-2", "cas_j-3", "temperature", "humidity",
    "work_mob", "transit_mob", "home_mob", "stringency_index",
    "vaccinated", "hospital_beds_per_thousand", "population_density"
]

X = scaler.transform(df_forecast[features])
X_tensor = torch.tensor(X, dtype=torch.float32).to(device)

# === PRÉDICTION ===
with torch.no_grad():
    y_pred = model(X_tensor).cpu().numpy().flatten()

# === EXPORT ===
df_forecast["prediction"] = y_pred
df_forecast["source"] = "forecast"
df_forecast["scenario"] = SCENARIO_NAME

output_path = f"export/statistique_predict_{SCENARIO_NAME}.csv"
df_forecast.to_csv(output_path, index=False)

print(f"✅ Prédictions enregistrées dans {output_path}")
