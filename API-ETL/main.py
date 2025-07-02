from flask import Flask, request, jsonify, Response
import os
from werkzeug.utils import secure_filename
from ETL_OMS_OPERATIONNEL import run_etl
from flask_cors import CORS
import io
import sys
import threading
import time
import json

UPLOAD_FOLDER = 'DATASETS'
ALLOWED_EXTENSIONS = {'csv', 'json', 'xlsx'}

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Variable globale pour stocker les logs et la progression
etl_progress = {
    'logs': '',
    'current': 0,
    'total': 0,
    'done': False,
    'error': None
}

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

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
    log_buffer = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = log_buffer
    try:
        run_etl()
        sys.stdout = sys_stdout
        logs = log_buffer.getvalue()
        return jsonify({"status": "success", "message": "ETL terminé avec succès", "logs": logs}), 200
    except Exception as e:
        sys.stdout = sys_stdout
        logs = log_buffer.getvalue()
        return jsonify({"status": "error", "message": str(e), "logs": logs}), 500

# Fonction de traitement ETL avec logs en temps réel
def run_etl_with_progress():
    global etl_progress
    etl_progress['logs'] = ''
    etl_progress['current'] = 0
    etl_progress['done'] = False
    etl_progress['error'] = None
    log_buffer = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = log_buffer
    try:
        # Compte les fichiers à traiter
        files = [fn for fn in os.listdir(UPLOAD_FOLDER) if fn.lower().endswith((".csv", ".json"))]
        etl_progress['total'] = len(files)
        etl_progress['current'] = 0
        def progress_callback(current, total):
            etl_progress['current'] = current
            etl_progress['total'] = total
        run_etl(progress_callback=progress_callback)
        sys.stdout = sys_stdout
        etl_progress['logs'] = log_buffer.getvalue()
        etl_progress['done'] = True
    except Exception as e:
        sys.stdout = sys_stdout
        etl_progress['logs'] = log_buffer.getvalue()
        etl_progress['error'] = str(e)
        etl_progress['done'] = True

@app.route('/upload', methods=['POST'])
def upload_and_run_etl():
    global etl_progress
    files = request.files.getlist('files')
    if not files or len(files) == 0:
        file = request.files.get('file')
        if file and file.filename != '':
            files = [file]
    if not files or len(files) == 0:
        return jsonify({'status': 'error', 'message': 'Aucun fichier reçu'}), 400
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    saved_files = []
    for file in files:
        if file.filename == '':
            continue
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(save_path)
            saved_files.append(filename)
    if not saved_files:
        return jsonify({'status': 'error', 'message': 'Aucun fichier valide à traiter'}), 400
    # Lance le traitement ETL en arrière-plan
    thread = threading.Thread(target=run_etl_with_progress)
    thread.start()
    return jsonify({'status': 'started', 'message': f'{len(saved_files)} fichiers à traiter', 'fichiers': saved_files}), 202

@app.route('/upload/progress')
def upload_progress():
    def event_stream():
        last_logs = ''
        from flask import stream_with_context
        while not etl_progress['done']:
            if etl_progress['logs'] != last_logs:
                logs_clean = etl_progress['logs'].replace(chr(10), '\\n').replace(chr(13), '')
                yield f"data: {{\"logs\": \"{logs_clean}\", \"current\": {etl_progress['current']}, \"total\": {etl_progress['total']}, \"done\": false, \"error\": {json.dumps(etl_progress['error']) if etl_progress['error'] else 'null'} }}\n\n"
                last_logs = etl_progress['logs']
                import sys; sys.stdout.flush()
            time.sleep(0.5)
        # Envoie le dernier état
        logs_clean = etl_progress['logs'].replace(chr(10), '\\n').replace(chr(13), '')
        yield f"data: {{\"logs\": \"{logs_clean}\", \"current\": {etl_progress['current']}, \"total\": {etl_progress['total']}, \"done\": true, \"error\": {json.dumps(etl_progress['error']) if etl_progress['error'] else 'null'} }}\n\n"
        import sys; sys.stdout.flush()
    headers = {'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    return Response(event_stream(), mimetype='text/event-stream', headers=headers)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
