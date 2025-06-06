from flask import Flask, request, jsonify
import joblib
import pandas as pd
import numpy as np
from datetime import timedelta
import os

from build_dataset import build_training_data
from train_ia import entrainer_modele

# === SETUP ===
app = Flask(__name__)

MODEL_PATH = "models/model_rf.pkl"
CACHE_PATH = "data_sources/cache_dataset.pkl"

def load_model():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError("Modèle introuvable. Lancez d'abord /train")
    return joblib.load(MODEL_PATH)


# === RANDOM FOREST : PRÉDICTION MANUELLE ===
@app.route("/predict/manual", methods=["POST"])
def predict_manual():
    data = request.get_json()
    try:
        features = [[
            float(data["cas_j-1"]),
            float(data["cas_j-2"]),
            float(data["cas_j-3"])
        ]]
    except (KeyError, ValueError):
        return jsonify({"error": "Entrée invalide. Requiert cas_j-1, cas_j-2, cas_j-3"}), 400

    model = load_model()
    pred = model.predict(features)[0]
    return jsonify({"prediction_j+7": round(pred)})


# === RANDOM FOREST : PRÉDICTION SUR 365 JOURS ===
@app.route("/predict/future", methods=["GET"])
def predict_future():
    if not os.path.exists(CACHE_PATH):
        return jsonify({"error": "Cache de données introuvable."}), 500

    model = load_model()
    df = pd.read_pickle(CACHE_PATH)
    df = df.dropna(subset=["cas_j-1", "cas_j-2", "cas_j-3"])
    df_latest = df.sort_values("date").groupby(["id_region", "id_maladie"]).tail(1).copy()

    features = ["cas_j-1", "cas_j-2", "cas_j-3"]
    future = []

    for _ in range(365):
        pred_date = df_latest["date"] + timedelta(days=1)
        preds = model.predict(df_latest[features])
        df_latest = df_latest.reset_index(drop=True)

        for idx, row in df_latest.iterrows():
            nouveau_cas = round(preds[idx])
            total_cas = row.get("total_cas", 0) + nouveau_cas
            total_mort = row.get("total_mort", 0)

            future.append({
                "id_maladie": row["id_maladie"],
                "id_region": row["id_region"],
                "date": row["date"] + timedelta(days=1),
                "nouveau_cas": nouveau_cas,
                "total_cas": total_cas,
                "nouveau_mort": 0,
                "total_mort": total_mort
            })

            df_latest.at[idx, "total_cas"] = total_cas

        df_latest["cas_j-3"] = df_latest["cas_j-2"]
        df_latest["cas_j-2"] = df_latest["cas_j-1"]
        df_latest["cas_j-1"] = preds
        df_latest["date"] = pred_date

    forecast_df = pd.DataFrame(future)
    os.makedirs("generated_data", exist_ok=True)
    forecast_df.to_csv("generated_data/statistique_predict.csv", index=False)
    return jsonify({"message": "Prédictions générées", "n_lignes": len(forecast_df)})


# === RANDOM FOREST : ENTRAÎNEMENT ===
@app.route("/train", methods=["POST"])
def train():
    entrainer_modele()
    return jsonify({"message": "Modèle entraîné et sauvegardé."})


# === SANTÉ API ===
@app.route("/")
def health():
    return jsonify({"status": "API en ligne 🚀"})


# === LSTM : PRÉDICTIONS ===
import torch
import torch.nn as nn

LSTM_MODEL_PATH = "models/model_lstm.pt"
LSTM_SCALER_PATH = "models/lstm_scaler.pkl"
LSTM_CSV_PATH = "generated_data/statistique_predict_lstm.csv"
LSTM_FEATURES = ["cas_j-1", "cas_j-2", "cas_j-3", "temperature", "humidity", "work_mob", "transit_mob", "home_mob"]

class LSTMModel(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size=64, batch_first=True)
        self.fc = nn.Linear(64, 1)
    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out.squeeze()

@app.route("/predict/lstm/cas", methods=["POST"])
def predict_lstm_unitaire():
    data = request.get_json()
    try:
        X = np.array([[data[feat] for feat in LSTM_FEATURES]])
        scaler = joblib.load(LSTM_SCALER_PATH)
        X_scaled = scaler.transform(X)
        model = LSTMModel(input_size=len(LSTM_FEATURES))
        model.load_state_dict(torch.load(LSTM_MODEL_PATH))
        model.eval()
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32).unsqueeze(1)
        with torch.no_grad():
            pred = model(X_tensor).item()
        return jsonify({"prediction_j+7": round(pred)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/predict/lstm/future", methods=["GET"])
def predict_lstm_batch():
    try:
        os.system("python forecast_ia_lstm.py")
        if not os.path.exists(LSTM_CSV_PATH):
            return jsonify({"error": "Le fichier de prédiction LSTM n’a pas été généré."}), 500
        df = pd.read_csv(LSTM_CSV_PATH)
        preview = df.tail(10).to_dict(orient="records")
        return jsonify({
            "message": "✅ Prédiction LSTM sur 365 jours générée.",
            "extrait": preview
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/predict', methods=['GET'])
def predict():
    try:
        generate_forecast()
        return jsonify({'message': 'Prévision générée avec succès'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



# === LANCEMENT SERVEUR ===
if __name__ == "__main__":
    app.run(debug=True)
