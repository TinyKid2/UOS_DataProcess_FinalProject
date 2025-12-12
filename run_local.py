#!/usr/bin/env python3
"""
로컬 개발/테스트용 스크립트
Kafka 없이도 시스템 테스트 가능
"""

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import json
import logging
from datetime import datetime
import threading
import time
import random
from queue import Queue

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# 메모리 큐 (Kafka 대체)
message_queue = {
    'power_generation_data': Queue(),
    'control_optimization_request': Queue(),
    'control_result': Queue()
}

# 통계
stats = {
    'messages_received': 0,
    'messages_processed': 0,
    'last_efficiency': 0,
    'last_cost': 0
}

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Microgrid Power Control (Local Test)</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; background: #f5f5f5; }
        .container { max-width: 800px; margin: auto; background: white; padding: 20px; border-radius: 10px; }
        .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
        .success { background: #d4edda; color: #155724; }
        .warning { background: #fff3cd; color: #856404; }
        .stats { background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 20px 0; }
        button { background: #4CAF50; color: white; padding: 10px 20px; border: none; cursor: pointer; }
        button:hover { background: #45a049; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔌 로컬 테스트 모드</h1>
        <div class="status warning">
            ⚠️ Kafka 없이 실행 중 (메모리 큐 사용)
        </div>
        
        <div class="stats">
            <h3>시스템 상태</h3>
            <p>수신 메시지: <span id="received">0</span></p>
            <p>처리 메시지: <span id="processed">0</span></p>
            <p>마지막 효율: <span id="efficiency">-</span></p>
            <p>예상 비용: <span id="cost">-</span></p>
        </div>
        
        <h2>테스트 데이터 전송</h2>
        <button onclick="sendTestData()">테스트 데이터 전송</button>
        <button onclick="sendBatch()">배치 전송 (10개)</button>
        <button onclick="startStream()">스트리밍 시작</button>
        <button onclick="stopStream()">스트리밍 중지</button>
        
        <div id="result"></div>
    </div>
    
    <script>
        let streaming = false;
        let streamInterval;
        
        async function sendTestData() {
            const data = {
                solar_power: Math.random() * 2000,
                wind_power: Math.random() * 1000,
                location: "MicroGrid-0" + (Math.floor(Math.random() * 3) + 1),
                weather: {
                    temperature: 20 + Math.random() * 10,
                    wind_speed: Math.random() * 10
                }
            };
            
            const response = await fetch('/api/power-data', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            document.getElementById('result').innerHTML = 
                '<div class="status success">✅ ' + result.message + '</div>';
            updateStats();
        }
        
        async function sendBatch() {
            const batch = [];
            for (let i = 0; i < 10; i++) {
                batch.push({
                    solar_power: Math.random() * 2000,
                    wind_power: Math.random() * 1000,
                    location: "MicroGrid-0" + (Math.floor(Math.random() * 3) + 1)
                });
            }
            
            const response = await fetch('/api/power-data/batch', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(batch)
            });
            
            const result = await response.json();
            document.getElementById('result').innerHTML = 
                '<div class="status success">✅ 배치 전송: ' + result.results.success + '/' + result.results.total + '</div>';
            updateStats();
        }
        
        function startStream() {
            if (streaming) return;
            streaming = true;
            streamInterval = setInterval(sendTestData, 2000);
            document.getElementById('result').innerHTML = 
                '<div class="status warning">📡 스트리밍 중...</div>';
        }
        
        function stopStream() {
            streaming = false;
            clearInterval(streamInterval);
            document.getElementById('result').innerHTML = 
                '<div class="status success">⏹️ 스트리밍 중지</div>';
        }
        
        async function updateStats() {
            const response = await fetch('/api/stats');
            const stats = await response.json();
            document.getElementById('received').textContent = stats.messages_received;
            document.getElementById('processed').textContent = stats.messages_processed;
            document.getElementById('efficiency').textContent = stats.last_efficiency.toFixed(2);
            document.getElementById('cost').textContent = stats.last_cost.toFixed(0) + '원';
        }
        
        setInterval(updateStats, 1000);
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/power-data', methods=['POST'])
def receive_power_data():
    """발전량 데이터 수신"""
    try:
        data = request.json
        data['timestamp'] = datetime.now().isoformat()
        data['received_at'] = datetime.now().isoformat()
        
        # 메모리 큐에 저장
        message_queue['power_generation_data'].put(data)
        stats['messages_received'] += 1
        
        logger.info(f"Data received: {data['location']}, Solar: {data['solar_power']}, Wind: {data['wind_power']}")
        
        return jsonify({
            'status': 'success',
            'message': 'Data received (local mode)',
            'data': data
        }), 200
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/power-data/batch', methods=['POST'])
def receive_batch_power_data():
    """배치 데이터 수신"""
    try:
        batch_data = request.json
        success_count = 0
        
        for data in batch_data:
            data['timestamp'] = datetime.now().isoformat()
            data['received_at'] = datetime.now().isoformat()
            message_queue['power_generation_data'].put(data)
            success_count += 1
            stats['messages_received'] += 1
        
        return jsonify({
            'status': 'success',
            'message': 'Batch received',
            'results': {
                'success': success_count,
                'failed': 0,
                'total': len(batch_data)
            }
        }), 200
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """통계 조회"""
    return jsonify(stats)

@app.route('/health')
def health():
    """헬스 체크"""
    return jsonify({
        'status': 'healthy',
        'mode': 'local',
        'kafka': 'not_used',
        'queues': {
            'power_generation_data': message_queue['power_generation_data'].qsize(),
            'control_optimization_request': message_queue['control_optimization_request'].qsize(),
            'control_result': message_queue['control_result'].qsize()
        },
        'timestamp': datetime.now().isoformat()
    })

def mock_preprocessing_service():
    """전처리 서비스 시뮬레이션"""
    while True:
        try:
            if not message_queue['power_generation_data'].empty():
                data = message_queue['power_generation_data'].get()
                
                # 데이터 검증 및 정규화
                processed_data = {
                    'request_id': f"req_{time.time()}",
                    'location': data['location'],
                    'power_data': {
                        'solar': data['solar_power'],
                        'wind': data['wind_power'],
                        'total': data['solar_power'] + data['wind_power']
                    },
                    'timestamp': data['timestamp']
                }
                
                message_queue['control_optimization_request'].put(processed_data)
                logger.info(f"Preprocessed data for {data['location']}")
                
        except Exception as e:
            logger.error(f"Preprocessing error: {e}")
        
        time.sleep(0.5)

def mock_optimization_engine():
    """최적화 엔진 시뮬레이션"""
    while True:
        try:
            if not message_queue['control_optimization_request'].empty():
                request = message_queue['control_optimization_request'].get()
                
                # 간단한 최적화 계산
                total_power = request['power_data']['total']
                efficiency = min(0.95, total_power / 3000) if total_power > 0 else 0
                cost = max(0, 2000 - total_power) * 100  # 부족 전력 비용
                
                result = {
                    'result_id': f"ctrl_{time.time()}",
                    'request_id': request['request_id'],
                    'location': request['location'],
                    'efficiency_score': efficiency,
                    'estimated_cost': cost,
                    'timestamp': datetime.now().isoformat()
                }
                
                message_queue['control_result'].put(result)
                stats['messages_processed'] += 1
                stats['last_efficiency'] = efficiency
                stats['last_cost'] = cost
                
                logger.info(f"Optimized: {request['location']}, Efficiency: {efficiency:.2f}, Cost: {cost:.0f}")
                
        except Exception as e:
            logger.error(f"Optimization error: {e}")
        
        time.sleep(0.5)

def mock_storage_service():
    """DB 저장 서비스 시뮬레이션"""
    results_storage = []
    
    while True:
        try:
            if not message_queue['control_result'].empty():
                result = message_queue['control_result'].get()
                results_storage.append(result)
                logger.info(f"Stored result: {result['result_id']}")
                
                # 최근 10개만 유지 (메모리 절약)
                if len(results_storage) > 10:
                    results_storage.pop(0)
                    
        except Exception as e:
            logger.error(f"Storage error: {e}")
        
        time.sleep(0.5)

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 로컬 테스트 모드 시작")
    print("=" * 60)
    print("📌 Kafka 없이 실행 (메모리 큐 사용)")
    print("🌐 웹 UI: http://localhost:5000")
    print("💡 모든 서비스가 로컬에서 시뮬레이션됩니다")
    print("=" * 60)
    
    # 백그라운드 서비스 시작
    services = [
        threading.Thread(target=mock_preprocessing_service, daemon=True),
        threading.Thread(target=mock_optimization_engine, daemon=True),
        threading.Thread(target=mock_storage_service, daemon=True)
    ]
    
    for service in services:
        service.start()
    
    # Flask 앱 실행
    app.run(host='0.0.0.0', port=5000, debug=False)