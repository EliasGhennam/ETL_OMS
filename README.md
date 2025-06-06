# Plateforme ETL & IA - Prévision Pandémique

Ce projet est une solution complète de traitement de données sanitaires et de prévision épidémiologique.  
Il repose sur deux microservices Python :

- **API-ETL** : dépôt et traitement de fichiers (nettoyage, insertion en base)
- **API-IA** : génération de datasets, entraînement LSTM, prédictions

Le tout est orchestré via Docker, avec une base PostgreSQL centrale.

---

## 🚀 Démarrage rapide

### ✅ Prérequis

- Docker & Docker Compose installés
- Python (optionnel, pour exécutions manuelles)

### 🧱 Structure des dossiers

```
projet/
├── API-ETL/         # Microservice pour traitement ETL
├── API-IA/          # Microservice pour traitement IA
├── docker-compose.yml
```

---

## ⚙️ Installation & Exécution

```bash
git clone https://github.com/EliasGhennam/ETL_OMS
cd ETL

# Lancer toute la stack avec base PostgreSQL
docker-compose up --build
```

> Les services exposent :
> - `http://localhost:5000` → API-ETL
> - `http://localhost:5001` → API-IA
> - `localhost:5432` → PostgreSQL (`user=user`, `password=password`, `db=pandemie`)

# Lancer le service sans passer par le build :

```bash
docker-compose up
```

---

## 🔌 Endpoints disponibles

### API-ETL (port 5000)
| Méthode | Route           | Description                         |
|---------|------------------|-------------------------------------|
| POST    | `/upload-etl`    | Dépose un fichier dans `/DATASETS` |
| POST    | `/run_etl`       | Lance le traitement et l'insertion |

### API-IA (port 5001)
| Méthode | Route             | Description                         |
|---------|-------------------|-------------------------------------|
| POST    | `/etl/cache`      | Génère le cache de données IA       |
| POST    | `/etl/train`      | Entraîne le modèle LSTM             |
| POST    | `/etl/forecast`   | Génère des prédictions sur 1 an    |
| POST    | `/predict_lstm`   | Renvoie les prédictions en JSON    |

---

## 🧪 Tests (optionnel)

```bash
cd API-IA
python -m pytest
```

---

## 🧠 Auteur

**Développeur IA / Données : Elias GHENNAM**

- ✉️ eliasghennam707@gmail.com  
- 🔗 [GitHub](https://github.com/EliasGhennam)  
- 💼 [LinkedIn](https://www.linkedin.com/in/elias-ghennam/)

---
