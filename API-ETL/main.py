from flask import Flask, request, jsonify
import os
from werkzeug.utils import secure_filename
from ETL_OMS_OPERATIONNEL import run_etl

UPLOAD_FOLDER = 'DATASETS'
ALLOWED_EXTENSIONS = {'csv', 'json', 'xlsx'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"message": "pong"}), 200

@app.route('/upload-etl', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Aucun fichier reçu'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'Nom de fichier vide'}), 400
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(save_path)
        return jsonify({'status': 'success', 'message': f'Fichier {filename} enregistré'}), 200
    return jsonify({'status': 'error', 'message': 'Extension de fichier non autorisée'}), 400

@app.route('/run_etl', methods=['POST'])
def run_etl_route():
    try:
        run_etl()
        return jsonify({"status": "success", "message": "ETL terminé avec succès"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
