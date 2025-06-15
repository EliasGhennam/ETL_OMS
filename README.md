# Plateforme ETL & IA - Prévision Pandémique

Ce projet est une solution complète de traitement de données sanitaires et de prévision épidémiologique.  
Il repose sur trois microservices :

- **API-ETL** : dépôt et traitement de fichiers (nettoyage, insertion en base)
- **API-IA** : génération de datasets, entraînement LSTM, prédictions
- **API-JAVA** : API REST pour la gestion des données et statistiques
- **FRONT** : Interface utilisateur Angular pour la visualisation des données

Le tout est orchestré via Docker, avec une base PostgreSQL centrale.

---

## 🚀 Démarrage rapide

### ✅ Prérequis

- Docker & Docker Compose installés
- Python 3.8+ (optionnel, pour exécutions manuelles)
- Java 17+ (optionnel, pour exécutions manuelles)
- Node.js 16+ (optionnel, pour exécutions manuelles)

### 🧱 Structure des dossiers

```
projet/
├── API-ETL/         # Microservice Python pour traitement ETL
├── API-IA/          # Microservice Python pour traitement IA
├── API-JAVA/        # API REST Spring Boot
├── FRONT/           # Application Angular
├── tests/           # Tests unitaires et d'intégration
├── docker-compose.yml
└── README.md
```

---

## ⚙️ Installation & Exécution

```bash
git clone https://github.com/EliasGhennam/ETL_OMS
cd ETL_OMS

# Lancer toute la stack avec base PostgreSQL
docker-compose up --build
```

> Les services exposent :
> - `http://localhost:5000` → API-ETL
> - `http://localhost:5001` → API-IA
> - `http://localhost:8080` → API-JAVA
> - `http://localhost:4200` → Frontend Angular
> - `localhost:5432` → PostgreSQL (`user=user`, `password=password`, `db=pandemie`)

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

### API-JAVA (port 8080)
| Méthode | Route                    | Description                    |
|---------|--------------------------|--------------------------------|
| GET     | `/api/pays`             | Liste des pays                 |
| GET     | `/api/maladies`         | Liste des maladies             |
| GET     | `/api/statistiques/*`   | Statistiques et analyses       |

---

## 🧪 Tests

```bash
# Tests Python
cd API-IA
python -m pytest

# Tests Java
cd API-JAVA
./mvnw test

# Tests Angular
cd FRONT
npm test
```

---

## 🛠️ Développement

### Configuration de l'environnement

1. Créer un fichier `.env` à la racine du projet
2. Copier le contenu de `.env.example`
3. Remplir les variables d'environnement

### Développement local

```bash
# API-ETL
cd API-ETL
python -m venv venv
source venv/bin/activate  # ou `venv\Scripts\activate` sur Windows
pip install -r requirements.txt
python app.py

# API-IA
cd API-IA
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py

# API-JAVA
cd API-JAVA
./mvnw spring-boot:run

# Frontend
cd FRONT
npm install
ng serve
```

---

## 🧠 Auteur

**Développeur IA / Données : Elias GHENNAM**

- ✉️ eliasghennam707@gmail.com  
- 🔗 [GitHub](https://github.com/EliasGhennam)  
- 💼 [LinkedIn](https://www.linkedin.com/in/elias-ghennam/)

---

## 📝 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.
