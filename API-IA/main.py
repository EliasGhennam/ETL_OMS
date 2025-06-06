import os
import pandas as pd
from flask import Flask, jsonify, request
from build_dataset import build_training_data
from train_ia_lstm import main as train_lstm_model
from forecast_ia_lstm import generate_forecast
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = 'data_sources'
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"message": "pong"}), 200

@app.route("/etl/cache", methods=["POST"])
def build_cache():
    try:
        result = build_training_data()
        return jsonify({"status": "success", "details": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/etl/train", methods=["POST"])
def train_lstm():
    try:
        result = train_lstm_model()
        return jsonify({"status": "success", "details": "Modèles entraînés"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/etl/forecast", methods=["POST"])
def forecast_lstm():
    try:
        result = generate_forecast()
        return jsonify({"status": "success", "details": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/predict_lstm', methods=['POST'])
def predict_lstm():
    result = generate_forecast()
    return jsonify({"message": result})

@app.route('/upload-reference', methods=['POST'])
def upload_reference_data():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Aucun fichier reçu'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'Nom de fichier vide'}), 400

    if file and allowed_file(file.filename):
        # Lire directement le contenu du fichier
        df = pd.read_csv(file)  # ou pd.read_excel(file) selon type
        # Traitement ici sans sauvegarde
        return jsonify({'status': 'success', 'message': f'Fichier {file.filename} traité en mémoire'}), 200

    return jsonify({'status': 'error', 'message': 'Extension non autorisée'}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
