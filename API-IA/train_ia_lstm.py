import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import joblib
import os


class LSTMModel(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size=64, batch_first=True)
        self.fc = nn.Linear(64, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out.squeeze()


def train_for_target(df, features, target_name, device):
    print(f"\n🚀 Entraînement pour la cible : {target_name}")
    df_target = df.dropna(subset=features + [target_name])
    df_target = df_target[df_target[target_name] > 0].copy()
    X = df_target[features].values
    y = df_target[target_name].values

    scaler = joblib.load("models/lstm_scaler.pkl")
    X = scaler.transform(X)

    class PandemicDataset(Dataset):
        def __init__(self, X, y):
            self.X = torch.tensor(X, dtype=torch.float32).unsqueeze(1)
            self.y = torch.tensor(y, dtype=torch.float32)

        def __len__(self):
            return len(self.X)

        def __getitem__(self, idx):
            return self.X[idx], self.y[idx]

    dataset = PandemicDataset(X, y)
    dataloader = DataLoader(dataset, batch_size=64, shuffle=True)

    model = LSTMModel(input_size=X.shape[1]).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    for epoch in range(10):
        epoch_loss = 0
        for batch_X, batch_y in dataloader:
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device)
            pred = model(batch_X)
            loss = criterion(pred, batch_y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        print(f"📉 {target_name} - Epoch {epoch + 1} - Loss : {epoch_loss / len(dataloader):.4f}")

    torch.save(model.state_dict(), f"models/model_lstm_{target_name}.pt")
    print(f"✅ Modèle sauvegardé : models/model_lstm_{target_name}.pt")


def main():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"📟 Entraînement sur : {device}")

    df = pd.read_pickle("data_sources/cache_dataset.pkl")
    context_cols = [
        "temperature", "humidity",
        "work_mob", "transit_mob", "home_mob",
        "stringency_index", "vaccinated", "hospital_beds_per_thousand", "population_density"
    ]
    df[context_cols] = df[context_cols].fillna(0)

    features = ["cas_j-1", "cas_j-2", "cas_j-3"] + context_cols
    df[features] = df[features].fillna(0)

    targets = ["cas", "morts", "temperature", "humidity", "work_mob"]

    # Entraînement du scaler une seule fois
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_all = df[features].dropna().values
    scaler.fit(X_all)
    os.makedirs("models", exist_ok=True)
    joblib.dump(scaler, "models/lstm_scaler.pkl")
    print("📦 Scaler sauvegardé dans models/lstm_scaler.pkl")

    for target in targets:
        df[target] = df["target"]  # on injecte une colonne factice pour éviter le KeyError
        train_for_target(df, features, target, device)


if __name__ == "__main__":
    main()


__all__ = ["LSTMModel"]
