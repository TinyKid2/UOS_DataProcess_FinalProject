from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from kafka import KafkaProducer
import json
import uuid
import os
from datetime import datetime
import pymysql

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', '/uploads')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'etl_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'etl_password')
DB_NAME = os.environ.get('DB_NAME', 'model_evaluation')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize Kafka Producer
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")

# HTML Template for upload page
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>YOLO Model Upload</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; }
        .container { max-width: 600px; margin: auto; }
        .upload-box { border: 2px dashed #ccc; padding: 30px; text-align: center; }
        .btn { background: #4CAF50; color: white; padding: 10px 20px; border: none; cursor: pointer; }
        .btn:hover { background: #45a049; }
        .results { margin-top: 20px; padding: 10px; background: #f0f0f0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>YOLO Model Evaluation System</h1>
        <div class="upload-box">
            <h2>Upload Your Model</h2>
            <form method="POST" enctype="multipart/form-data" action="/upload">
                <input type="file" name="model" accept=".pt" required><br><br>
                <input type="text" name="name" placeholder="Your Name (optional)"><br><br>
                <button type="submit" class="btn">Upload Model</button>
            </form>
        </div>
        <div id="results"></div>
    </div>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/upload', methods=['POST'])
def upload_model():
    try:
        if 'model' not in request.files:
            return jsonify({'error': 'No model file provided'}), 400
        
        file = request.files['model']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Generate unique ID
        submission_id = str(uuid.uuid4())
        
        # Save file
        filename = f"{submission_id}_{file.filename}"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # Send to Kafka
        message = {
            'id': submission_id,
            'filename': filename,
            'filepath': filepath,
            'timestamp': datetime.now().isoformat(),
            'name': request.form.get('name', 'Anonymous')
        }
        
        if producer:
            producer.send('model_submission', message)
            producer.flush()
        
        # Store in database
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO submissions (id, filename, filepath, submitted_at, submitter_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (submission_id, filename, filepath, datetime.now(), message['name']))
            conn.commit()
            conn.close()
        except Exception as db_error:
            print(f"Database error: {db_error}")
        
        return jsonify({
            'success': True,
            'submission_id': submission_id,
            'message': 'Model uploaded successfully. Check back for results.'
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/results/<submission_id>')
def get_results(submission_id):
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM evaluation_results WHERE submission_id = %s
        """, (submission_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return jsonify({
                'submission_id': result[0],
                'f1_score': result[1],
                'precision': result[2],
                'recall': result[3],
                'evaluated_at': result[4].isoformat() if result[4] else None
            })
        else:
            return jsonify({'message': 'Results not yet available'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)