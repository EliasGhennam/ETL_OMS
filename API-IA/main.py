import os
from flask import Flask, jsonify, request, send_from_directory
from build_dataset import build_training_data
from train_ia_lstm import main as train_lstm_model
from forecast_ia_lstm import generate_forecast
from werkzeug.utils import secure_filename
from flask_cors import CORS

UPLOAD_FOLDER = 'data_sources'
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"message": "pong"}), 200


@app.route("/etl/cache", methods=["POST"])
def build_cache():
    try:
        build_training_data()
        return jsonify({"status": "success", "details": "Cache construit avec succès"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/etl/train", methods=["POST"])
def train_lstm():
    try:
        train_lstm_model()
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


@app.route('/upload-data', methods=['POST'])
def upload_data():
    if 'files' not in request.files:
        return jsonify({'status': 'error', 'message': 'Aucun fichier reçu'}), 400

    files = request.files.getlist('files')

    if not files or all(f.filename == '' for f in files):
        return jsonify({'status': 'error', 'message': 'Aucun fichier sélectionné'}), 400

    saved_files = []
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            saved_files.append(filename)

    if not saved_files:
        return jsonify({'status': 'error', 'message': 'Aucun fichier valide ou extension non autorisée'}), 400

    return jsonify({'status': 'success', 'message': f'{len(saved_files)} fichier(s) uploadé(s) avec succès', 'files': saved_files}), 200


@app.route('/download-prediction', methods=['GET'])
def download_prediction():
    file_path = 'generated_data'
    filename = 'statistique_predict_lstm.csv'
    return send_from_directory(file_path, filename, as_attachment=True)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
