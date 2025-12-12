from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from datetime import datetime
import numpy as np
import os
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessingService:
    def __init__(self):
        self.kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.input_topic = 'power_generation_data'
        self.output_topic = 'control_optimization_request'
        
        # Kafka Consumer 설정
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.kafka_bootstrap,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='preprocessing_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Kafka Producer 설정
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info(f"Data Preprocessing Service initialized - Bootstrap: {self.kafka_bootstrap}")
        
    def validate_data(self, data):
        """데이터 유효성 검증"""
        errors = []
        
        # 필수 필드 확인
        required_fields = ['solar_power', 'wind_power', 'location', 'timestamp']
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")
        
        # 발전량 값 유효성 검증
        if 'solar_power' in data:
            if data['solar_power'] < 0:
                errors.append("Solar power cannot be negative")
            if data['solar_power'] > 10000:  # 10MW 제한
                errors.append("Solar power exceeds maximum capacity")
                
        if 'wind_power' in data:
            if data['wind_power'] < 0:
                errors.append("Wind power cannot be negative")
            if data['wind_power'] > 10000:  # 10MW 제한
                errors.append("Wind power exceeds maximum capacity")
        
        return len(errors) == 0, errors
    
    def detect_anomalies(self, data):
        """이상치 탐지"""
        anomalies = []
        
        # 발전량 급격한 변화 탐지
        total_power = data.get('solar_power', 0) + data.get('wind_power', 0)
        
        # 시간대별 이상치 검사
        try:
            hour = datetime.fromisoformat(data['timestamp']).hour
            
            # 태양광 발전: 밤 시간대 발전량 확인
            if 20 <= hour or hour <= 5:
                if data.get('solar_power', 0) > 10:  # 밤에 10kW 이상
                    anomalies.append({
                        'type': 'solar_night_generation',
                        'message': f"Solar generation at night: {data['solar_power']}kW at hour {hour}"
                    })
            
            # 풍력 발전: 비정상적인 변동
            if 'weather' in data and 'wind_speed' in data['weather']:
                wind_speed = data['weather']['wind_speed']
                wind_power = data.get('wind_power', 0)
                
                # 바람 속도 대비 발전량 확인
                if wind_speed < 3 and wind_power > 100:  # 낮은 풍속에 높은 발전
                    anomalies.append({
                        'type': 'wind_power_mismatch',
                        'message': f"High wind power ({wind_power}kW) with low wind speed ({wind_speed}m/s)"
                    })
                    
        except Exception as e:
            logger.warning(f"Error in anomaly detection: {e}")
        
        return anomalies
    
    def normalize_data(self, data):
        """데이터 정규화 및 보정"""
        normalized = data.copy()
        
        # 발전량 단위 통일 (kW)
        normalized['solar_power'] = float(data.get('solar_power', 0))
        normalized['wind_power'] = float(data.get('wind_power', 0))
        normalized['total_power'] = normalized['solar_power'] + normalized['wind_power']
        
        # 효율성 계산
        if 'weather' in data:
            weather = data['weather']
            
            # 태양광 효율 추정 (온도 기반)
            if 'temperature' in weather:
                temp = weather['temperature']
                # 25도 기준, 1도 상승 시 0.5% 효율 감소
                solar_efficiency = max(0.5, 1 - 0.005 * abs(temp - 25))
                normalized['solar_efficiency'] = solar_efficiency
            
            # 풍력 효율 추정 (풍속 기반)
            if 'wind_speed' in weather:
                wind_speed = weather['wind_speed']
                if wind_speed < 3:
                    wind_efficiency = 0
                elif wind_speed < 15:
                    wind_efficiency = min(1.0, wind_speed / 15)
                else:
                    wind_efficiency = max(0.3, 1 - (wind_speed - 15) * 0.1)
                normalized['wind_efficiency'] = wind_efficiency
        
        # 시간대 정보 추가
        try:
            dt = datetime.fromisoformat(data['timestamp'])
            normalized['hour'] = dt.hour
            normalized['day_of_week'] = dt.weekday()
            normalized['is_weekend'] = dt.weekday() >= 5
        except:
            pass
        
        return normalized
    
    def prepare_optimization_request(self, normalized_data, anomalies):
        """최적화 요청 메시지 준비"""
        request = {
            'request_id': f"{normalized_data['location']}_{datetime.now().timestamp()}",
            'timestamp': datetime.now().isoformat(),
            'location': normalized_data['location'],
            'power_data': {
                'solar': normalized_data['solar_power'],
                'wind': normalized_data['wind_power'],
                'total': normalized_data['total_power']
            },
            'efficiencies': {
                'solar': normalized_data.get('solar_efficiency', 1.0),
                'wind': normalized_data.get('wind_efficiency', 1.0)
            },
            'temporal_features': {
                'hour': normalized_data.get('hour', 0),
                'day_of_week': normalized_data.get('day_of_week', 0),
                'is_weekend': normalized_data.get('is_weekend', False)
            },
            'weather': normalized_data.get('weather', {}),
            'anomalies': anomalies,
            'quality_score': 1.0 - len(anomalies) * 0.1,  # 이상치마다 0.1 감점
            'original_timestamp': normalized_data['timestamp']
        }
        
        return request
    
    def process_message(self, message):
        """메시지 처리 파이프라인"""
        try:
            # 1. 데이터 유효성 검증
            is_valid, errors = self.validate_data(message)
            if not is_valid:
                logger.error(f"Invalid data: {errors}")
                return False
            
            # 2. 이상치 탐지
            anomalies = self.detect_anomalies(message)
            if anomalies:
                logger.warning(f"Anomalies detected: {anomalies}")
            
            # 3. 데이터 정규화
            normalized_data = self.normalize_data(message)
            
            # 4. 최적화 요청 준비
            optimization_request = self.prepare_optimization_request(normalized_data, anomalies)
            
            # 5. Kafka로 전송
            future = self.producer.send(self.output_topic, value=optimization_request)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Processed and sent optimization request - "
                       f"Location: {message['location']}, "
                       f"Total Power: {normalized_data['total_power']}kW, "
                       f"Quality Score: {optimization_request['quality_score']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def run(self):
        """서비스 실행"""
        logger.info("Starting Data Preprocessing Service...")
        
        try:
            for message in self.consumer:
                data = message.value
                logger.info(f"Received message from topic '{self.input_topic}'")
                
                # 메시지 처리
                success = self.process_message(data)
                
                if not success:
                    logger.error(f"Failed to process message: {data}")
                
        except KeyboardInterrupt:
            logger.info("Shutting down Data Preprocessing Service...")
        except Exception as e:
            logger.error(f"Service error: {e}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    # Kafka가 준비될 때까지 대기
    time.sleep(10)
    
    service = DataPreprocessingService()
    service.run()