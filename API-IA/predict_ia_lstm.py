import torch
import torch.nn as nn
import joblib
import numpy as np

# Chargement du modèle LSTM
class LSTMModel(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size=64, batch_first=True)
        self.fc = nn.Linear(64, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out.squeeze()

# Exemple d'entrée : à remplacer par tes valeurs réelles
# Format : [cas_j-1, cas_j-2, cas_j-3, temperature, humidity, work_mob, transit_mob, home_mob]
example_input = np.array([[110, 95, 80, 15.2, 72, -18, -40, 12]])

# Charger le scaler
scaler = joblib.load("models/lstm_scaler.pkl")
X_scaled = scaler.transform(example_input)

# Charger le modèle
model = LSTMModel(input_size=X_scaled.shape[1])
model.load_state_dict(torch.load("models/model_lstm.pt"))
model.eval()

# Transformer l'entrée pour PyTorch
X_tensor = torch.tensor(X_scaled, dtype=torch.float32).unsqueeze(1)  # batch_size=1, seq_len=1, features

# Prédiction
with torch.no_grad():
    prediction = model(X_tensor).item()

print(f"📈 Prédiction du nombre de cas à J+7 : {round(prediction)}")
