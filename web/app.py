from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Producer 설정
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
KAFKA_TOPIC = 'power_generation_data'

# Kafka 연결 시도 (여러 주소 시도)
producer = None
kafka_addresses = [
    KAFKA_BOOTSTRAP_SERVERS,
    'localhost:19092',  # Docker 외부 포트
    'kafka:29092',      # Docker 내부
    'localhost:9092',   # 기본 포트
]

for address in kafka_addresses:
    try:
        logger.info(f"Attempting Kafka connection to {address}...")
        producer = KafkaProducer(
            bootstrap_servers=address,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )
        KAFKA_BOOTSTRAP_SERVERS = address  # 성공한 주소 저장
        logger.info(f"✅ Kafka Producer connected to {address}")
        break
    except Exception as e:
        logger.warning(f"Failed to connect to {address}: {e}")
        continue

if producer is None:
    logger.error("❌ Failed to connect to Kafka on all addresses")
    logger.info("System will run in degraded mode (no Kafka)")

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Microgrid Power Control System</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; background: #f5f5f5; }
        .container { max-width: 800px; margin: auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #333; text-align: center; }
        .power-form { margin: 20px 0; }
        .form-group { margin: 15px 0; }
        label { display: block; margin-bottom: 5px; color: #555; font-weight: bold; }
        input[type="number"], input[type="text"], select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        .btn { background: #4CAF50; color: white; padding: 12px 30px; border: none; border-radius: 4px; cursor: pointer; width: 100%; font-size: 16px; }
        .btn:hover { background: #45a049; }
        .status { margin-top: 20px; padding: 15px; border-radius: 4px; }
        .success { background: #d4edda; color: #155724; }
        .error { background: #f8d7da; color: #721c24; }
        .info-box { background: #e7f3ff; padding: 15px; border-radius: 4px; margin: 20px 0; }
        .metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
        .metric-card { background: #f8f9fa; padding: 15px; border-radius: 4px; text-align: center; }
        .metric-value { font-size: 24px; font-weight: bold; color: #007bff; }
        .metric-label { color: #666; margin-top: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ Microgrid Power Control System</h1>
        
        <div class="info-box">
            <h3>시스템 개요</h3>
            <p>태양광 및 풍력 발전량 데이터를 입력하면 마이크로그리드의 최적 전력 제어 방식을 산출합니다.</p>
        </div>
        
        <div class="power-form">
            <h2>발전량 데이터 입력</h2>
            <form id="powerForm">
                <div class="form-group">
                    <label for="location">마이크로그리드 ID</label>
                    <select id="location" required>
                        <option value="MicroGrid-01">MicroGrid-01 (서울)</option>
                        <option value="MicroGrid-02">MicroGrid-02 (대전)</option>
                        <option value="MicroGrid-03">MicroGrid-03 (부산)</option>
                    </select>
                </div>
                
                <div class="metrics">
                    <div class="form-group">
                        <label for="solar_power">태양광 발전량 (kW)</label>
                        <input type="number" id="solar_power" step="0.01" min="0" required>
                    </div>
                    
                    <div class="form-group">
                        <label for="wind_power">풍력 발전량 (kW)</label>
                        <input type="number" id="wind_power" step="0.01" min="0" required>
                    </div>
                </div>
                
                <h3>날씨 정보 (선택사항)</h3>
                <div class="metrics">
                    <div class="form-group">
                        <label for="temperature">온도 (°C)</label>
                        <input type="number" id="temperature" step="0.1">
                    </div>
                    
                    <div class="form-group">
                        <label for="wind_speed">풍속 (m/s)</label>
                        <input type="number" id="wind_speed" step="0.1" min="0">
                    </div>
                </div>
                
                <button type="submit" class="btn">데이터 전송</button>
            </form>
        </div>
        
        <div id="status"></div>
        
        <div class="metrics" id="currentMetrics" style="display:none;">
            <div class="metric-card">
                <div class="metric-value" id="totalPower">0</div>
                <div class="metric-label">총 발전량 (kW)</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" id="timestamp">-</div>
                <div class="metric-label">처리 시간</div>
            </div>
        </div>
    </div>
    
    <script>
        document.getElementById('powerForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const data = {
                location: document.getElementById('location').value,
                solar_power: parseFloat(document.getElementById('solar_power').value),
                wind_power: parseFloat(document.getElementById('wind_power').value),
                weather: {}
            };
            
            const temperature = document.getElementById('temperature').value;
            const windSpeed = document.getElementById('wind_speed').value;
            
            if (temperature) data.weather.temperature = parseFloat(temperature);
            if (windSpeed) data.weather.wind_speed = parseFloat(windSpeed);
            
            try {
                const response = await fetch('/api/power-data', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    document.getElementById('status').innerHTML = 
                        `<div class="status success">
                            <strong>✅ 성공!</strong><br>
                            ${result.message}
                        </div>`;
                    
                    // Update metrics display
                    const totalPower = data.solar_power + data.wind_power;
                    document.getElementById('totalPower').textContent = totalPower.toFixed(2);
                    document.getElementById('timestamp').textContent = new Date().toLocaleTimeString();
                    document.getElementById('currentMetrics').style.display = 'grid';
                } else {
                    document.getElementById('status').innerHTML = 
                        `<div class="status error">
                            <strong>❌ 오류</strong><br>
                            ${result.message}
                        </div>`;
                }
            } catch (error) {
                document.getElementById('status').innerHTML = 
                    `<div class="status error">
                        <strong>❌ 네트워크 오류</strong><br>
                        ${error.message}
                    </div>`;
            }
        });
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    """메인 페이지"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/power-data', methods=['POST'])
def receive_power_data():
    """
    태양광 및 풍력 발전량 데이터 수신
    
    Expected JSON format:
    {
        "timestamp": "2024-12-13T10:30:00",
        "solar_power": 1234.56,  # kW
        "wind_power": 2345.67,   # kW
        "location": "MicroGrid-01",
        "weather": {
            "temperature": 25.5,
            "humidity": 60,
            "wind_speed": 5.2
        }
    }
    """
    try:
        data = request.json
        
        # 데이터 검증
        required_fields = ['solar_power', 'wind_power', 'location']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # 타임스탬프 추가 (없으면)
        if 'timestamp' not in data:
            data['timestamp'] = datetime.now().isoformat()
        
        # 메타데이터 추가
        data['received_at'] = datetime.now().isoformat()
        data['data_type'] = 'power_generation'
        
        # Kafka로 전송
        if producer:
            future = producer.send(KAFKA_TOPIC, value=data)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Data sent to Kafka - Topic: {record_metadata.topic}, "
                       f"Partition: {record_metadata.partition}, "
                       f"Offset: {record_metadata.offset}")
            
            return jsonify({
                'status': 'success',
                'message': 'Power generation data received and queued for processing',
                'data': {
                    'timestamp': data['timestamp'],
                    'solar_power': data['solar_power'],
                    'wind_power': data['wind_power'],
                    'location': data['location']
                }
            }), 200
        else:
            logger.error("Kafka Producer not available")
            return jsonify({
                'status': 'error',
                'message': 'Message queue service unavailable'
            }), 503
            
    except Exception as e:
        logger.error(f"Error processing power data: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/power-data/batch', methods=['POST'])
def receive_batch_power_data():
    """
    배치 발전량 데이터 수신 (여러 시점의 데이터를 한번에)
    """
    try:
        batch_data = request.json
        
        if not isinstance(batch_data, list):
            return jsonify({
                'status': 'error',
                'message': 'Expected array of power data'
            }), 400
        
        success_count = 0
        failed_count = 0
        
        for data in batch_data:
            try:
                # 데이터 검증 및 전송
                required_fields = ['solar_power', 'wind_power', 'location']
                if all(field in data for field in required_fields):
                    if 'timestamp' not in data:
                        data['timestamp'] = datetime.now().isoformat()
                    
                    data['received_at'] = datetime.now().isoformat()
                    data['data_type'] = 'power_generation'
                    
                    if producer:
                        producer.send(KAFKA_TOPIC, value=data)
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing batch item: {e}")
                failed_count += 1
        
        # 모든 메시지 전송 완료 대기
        if producer:
            producer.flush()
        
        return jsonify({
            'status': 'success',
            'message': f'Batch processing completed',
            'results': {
                'success': success_count,
                'failed': failed_count,
                'total': len(batch_data)
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing batch data: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/health')
def health_check():
    """헬스 체크 엔드포인트"""
    global producer, KAFKA_BOOTSTRAP_SERVERS
    
    kafka_status = 'connected' if producer else 'disconnected'
    
    # Kafka 재연결 시도
    if not producer:
        for address in ['localhost:19092', 'kafka:29092', 'localhost:9092']:
            try:
                test_producer = KafkaProducer(
                    bootstrap_servers=address,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    max_block_ms=2000,
                    request_timeout_ms=5000
                )
                producer = test_producer
                KAFKA_BOOTSTRAP_SERVERS = address
                kafka_status = 'reconnected'
                logger.info(f"Kafka reconnected to {address}")
                break
            except:
                continue
    
    return jsonify({
        'status': 'healthy',
        'kafka': kafka_status,
        'kafka_server': KAFKA_BOOTSTRAP_SERVERS if producer else 'none',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)