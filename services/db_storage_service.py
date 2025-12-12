from kafka import KafkaConsumer
import json
import logging
from datetime import datetime
import pymysql
import os
import time
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseStorageService:
    def __init__(self):
        self.kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.input_topic = 'control_result'
        
        # Database 설정
        self.db_config = {
            'host': os.environ.get('DB_HOST', 'mariadb'),
            'user': os.environ.get('DB_USER', 'microgrid_user'),
            'password': os.environ.get('DB_PASSWORD', 'microgrid_password'),
            'database': os.environ.get('DB_NAME', 'microgrid_control'),
            'charset': 'utf8mb4'
        }
        
        # Kafka Consumer 설정
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.kafka_bootstrap,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='storage_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Database 연결 초기화
        self.connection = None
        self.connect_database()
        
        logger.info(f"Database Storage Service initialized - Bootstrap: {self.kafka_bootstrap}")
    
    def connect_database(self):
        """데이터베이스 연결"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.connection = pymysql.connect(**self.db_config)
                logger.info(f"Connected to database: {self.db_config['host']}")
                
                # 테이블 생성 (없으면)
                self.create_tables()
                return
                
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception("Failed to connect to database after multiple attempts")
    
    def create_tables(self):
        """필요한 테이블 생성"""
        try:
            with self.connection.cursor() as cursor:
                # 전력 제어 결과 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS control_results (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        result_id VARCHAR(100) UNIQUE,
                        request_id VARCHAR(100),
                        location VARCHAR(50),
                        timestamp DATETIME,
                        original_timestamp DATETIME,
                        solar_power FLOAT,
                        wind_power FLOAT,
                        total_power FLOAT,
                        battery_charge_rate FLOAT,
                        battery_discharge_rate FLOAT,
                        battery_soc FLOAT,
                        grid_import FLOAT,
                        grid_export FLOAT,
                        load_shedding FLOAT,
                        renewable_curtailment FLOAT,
                        estimated_cost FLOAT,
                        efficiency_score FLOAT,
                        renewable_utilization FLOAT,
                        grid_dependency FLOAT,
                        electricity_price FLOAT,
                        quality_score FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_location (location),
                        INDEX idx_timestamp (timestamp),
                        INDEX idx_efficiency (efficiency_score)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # 시간별 집계 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS hourly_aggregates (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        location VARCHAR(50),
                        hour_timestamp DATETIME,
                        avg_solar_power FLOAT,
                        avg_wind_power FLOAT,
                        total_energy_generated FLOAT,
                        total_battery_charged FLOAT,
                        total_battery_discharged FLOAT,
                        total_grid_import FLOAT,
                        total_grid_export FLOAT,
                        avg_efficiency_score FLOAT,
                        total_cost FLOAT,
                        record_count INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_location_hour (location, hour_timestamp),
                        INDEX idx_hour (hour_timestamp)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # 일별 집계 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS daily_summary (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        location VARCHAR(50),
                        date DATE,
                        total_solar_energy FLOAT,
                        total_wind_energy FLOAT,
                        total_energy_generated FLOAT,
                        total_energy_consumed FLOAT,
                        total_grid_import FLOAT,
                        total_grid_export FLOAT,
                        avg_efficiency_score FLOAT,
                        max_efficiency_score FLOAT,
                        min_efficiency_score FLOAT,
                        total_cost FLOAT,
                        total_revenue FLOAT,
                        net_profit FLOAT,
                        peak_power FLOAT,
                        min_power FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_location_date (location, date),
                        INDEX idx_date (date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # 알람 및 이벤트 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_events (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        event_type VARCHAR(50),
                        severity VARCHAR(20),
                        location VARCHAR(50),
                        description TEXT,
                        event_data JSON,
                        timestamp DATETIME,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_event_type (event_type),
                        INDEX idx_severity (severity),
                        INDEX idx_timestamp (timestamp)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                self.connection.commit()
                logger.info("Database tables created/verified successfully")
                
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def store_control_result(self, result: Dict[str, Any]) -> bool:
        """제어 결과 저장"""
        try:
            # 데이터베이스 연결 확인
            if not self.connection or not self.connection.open:
                self.connect_database()
            
            with self.connection.cursor() as cursor:
                # 메인 결과 저장
                sql = """
                    INSERT INTO control_results (
                        result_id, request_id, location, timestamp, original_timestamp,
                        solar_power, wind_power, total_power,
                        battery_charge_rate, battery_discharge_rate, battery_soc,
                        grid_import, grid_export, load_shedding, renewable_curtailment,
                        estimated_cost, efficiency_score, renewable_utilization,
                        grid_dependency, electricity_price, quality_score
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        timestamp = VALUES(timestamp),
                        efficiency_score = VALUES(efficiency_score),
                        estimated_cost = VALUES(estimated_cost)
                """
                
                # 타임스탬프 파싱
                timestamp = datetime.fromisoformat(result['timestamp'].replace('Z', '+00:00'))
                original_timestamp = datetime.fromisoformat(result['original_timestamp'].replace('Z', '+00:00'))
                
                values = (
                    result['result_id'],
                    result['request_id'],
                    result['location'],
                    timestamp,
                    original_timestamp,
                    result['power_data']['solar'],
                    result['power_data']['wind'],
                    result['power_data']['total'],
                    result['control_strategy']['battery']['charge_rate'],
                    result['control_strategy']['battery']['discharge_rate'],
                    result['control_strategy']['battery']['current_soc'],
                    result['control_strategy']['grid']['import'],
                    result['control_strategy']['grid']['export'],
                    result['control_strategy']['demand_response']['load_shedding'],
                    result['control_strategy']['demand_response']['renewable_curtailment'],
                    result['metrics']['estimated_cost'],
                    result['metrics']['efficiency_score'],
                    result['metrics']['renewable_utilization'],
                    result['metrics']['grid_dependency'],
                    result['optimization_params']['electricity_price'],
                    result['optimization_params']['quality_score']
                )
                
                cursor.execute(sql, values)
                
                # 시간별 집계 업데이트
                self.update_hourly_aggregate(cursor, result, timestamp)
                
                # 이벤트 확인 및 저장
                self.check_and_store_events(cursor, result)
                
                self.connection.commit()
                
                logger.info(f"Stored control result: {result['result_id']} for {result['location']}")
                return True
                
        except Exception as e:
            logger.error(f"Error storing control result: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def update_hourly_aggregate(self, cursor, result: Dict[str, Any], timestamp: datetime):
        """시간별 집계 데이터 업데이트"""
        try:
            # 시간 단위로 반올림
            hour_timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
            
            sql = """
                INSERT INTO hourly_aggregates (
                    location, hour_timestamp,
                    avg_solar_power, avg_wind_power, total_energy_generated,
                    total_battery_charged, total_battery_discharged,
                    total_grid_import, total_grid_export,
                    avg_efficiency_score, total_cost, record_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1
                ) ON DUPLICATE KEY UPDATE
                    avg_solar_power = (avg_solar_power * record_count + VALUES(avg_solar_power)) / (record_count + 1),
                    avg_wind_power = (avg_wind_power * record_count + VALUES(avg_wind_power)) / (record_count + 1),
                    total_energy_generated = total_energy_generated + VALUES(total_energy_generated),
                    total_battery_charged = total_battery_charged + VALUES(total_battery_charged),
                    total_battery_discharged = total_battery_discharged + VALUES(total_battery_discharged),
                    total_grid_import = total_grid_import + VALUES(total_grid_import),
                    total_grid_export = total_grid_export + VALUES(total_grid_export),
                    avg_efficiency_score = (avg_efficiency_score * record_count + VALUES(avg_efficiency_score)) / (record_count + 1),
                    total_cost = total_cost + VALUES(total_cost),
                    record_count = record_count + 1
            """
            
            # 시간당 에너지 계산 (5분 간격 가정)
            time_interval = 1/12  # 5분 = 1/12 시간
            
            values = (
                result['location'],
                hour_timestamp,
                result['power_data']['solar'],
                result['power_data']['wind'],
                result['power_data']['total'] * time_interval,
                result['control_strategy']['battery']['charge_rate'] * time_interval,
                result['control_strategy']['battery']['discharge_rate'] * time_interval,
                result['control_strategy']['grid']['import'] * time_interval,
                result['control_strategy']['grid']['export'] * time_interval,
                result['metrics']['efficiency_score'],
                result['metrics']['estimated_cost']
            )
            
            cursor.execute(sql, values)
            
        except Exception as e:
            logger.error(f"Error updating hourly aggregate: {e}")
    
    def check_and_store_events(self, cursor, result: Dict[str, Any]):
        """이벤트 확인 및 저장"""
        try:
            events = []
            
            # 효율성 저하 확인
            if result['metrics']['efficiency_score'] < 0.5:
                events.append({
                    'event_type': 'LOW_EFFICIENCY',
                    'severity': 'WARNING',
                    'description': f"Low efficiency score: {result['metrics']['efficiency_score']:.2f}"
                })
            
            # 높은 그리드 의존도
            if result['metrics']['grid_dependency'] > 0.7:
                events.append({
                    'event_type': 'HIGH_GRID_DEPENDENCY',
                    'severity': 'INFO',
                    'description': f"High grid dependency: {result['metrics']['grid_dependency']:.2f}"
                })
            
            # 부하 차단 발생
            if result['control_strategy']['demand_response']['load_shedding'] > 0:
                events.append({
                    'event_type': 'LOAD_SHEDDING',
                    'severity': 'WARNING',
                    'description': f"Load shedding activated: {result['control_strategy']['demand_response']['load_shedding']:.1f} kW"
                })
            
            # 재생에너지 출력 제한
            if result['control_strategy']['demand_response']['renewable_curtailment'] > 0:
                events.append({
                    'event_type': 'RENEWABLE_CURTAILMENT',
                    'severity': 'INFO',
                    'description': f"Renewable curtailment: {result['control_strategy']['demand_response']['renewable_curtailment']:.1f} kW"
                })
            
            # 배터리 SOC 경고
            battery_soc = result['control_strategy']['battery']['current_soc']
            if battery_soc < 20:
                events.append({
                    'event_type': 'LOW_BATTERY',
                    'severity': 'WARNING',
                    'description': f"Low battery SOC: {battery_soc:.1f}%"
                })
            elif battery_soc > 95:
                events.append({
                    'event_type': 'BATTERY_FULL',
                    'severity': 'INFO',
                    'description': f"Battery nearly full: {battery_soc:.1f}%"
                })
            
            # 이벤트 저장
            for event in events:
                sql = """
                    INSERT INTO system_events (
                        event_type, severity, location, description, event_data, timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    event['event_type'],
                    event['severity'],
                    result['location'],
                    event['description'],
                    json.dumps(result),
                    datetime.fromisoformat(result['timestamp'].replace('Z', '+00:00'))
                )
                
                cursor.execute(sql, values)
                
            if events:
                logger.info(f"Stored {len(events)} events for {result['location']}")
                
        except Exception as e:
            logger.error(f"Error checking/storing events: {e}")
    
    def update_daily_summary(self):
        """일별 요약 업데이트 (정기 실행용)"""
        try:
            if not self.connection or not self.connection.open:
                self.connect_database()
            
            with self.connection.cursor() as cursor:
                # 어제 날짜 계산
                yesterday = datetime.now().date()
                
                sql = """
                    INSERT INTO daily_summary (
                        location, date,
                        total_solar_energy, total_wind_energy, total_energy_generated,
                        total_grid_import, total_grid_export,
                        avg_efficiency_score, max_efficiency_score, min_efficiency_score,
                        total_cost, peak_power, min_power
                    )
                    SELECT 
                        location, DATE(timestamp) as date,
                        SUM(solar_power * 0.0833) as total_solar_energy,  -- 5분 = 0.0833시간
                        SUM(wind_power * 0.0833) as total_wind_energy,
                        SUM(total_power * 0.0833) as total_energy_generated,
                        SUM(grid_import * 0.0833) as total_grid_import,
                        SUM(grid_export * 0.0833) as total_grid_export,
                        AVG(efficiency_score) as avg_efficiency_score,
                        MAX(efficiency_score) as max_efficiency_score,
                        MIN(efficiency_score) as min_efficiency_score,
                        SUM(estimated_cost) as total_cost,
                        MAX(total_power) as peak_power,
                        MIN(total_power) as min_power
                    FROM control_results
                    WHERE DATE(timestamp) = %s
                    GROUP BY location, DATE(timestamp)
                    ON DUPLICATE KEY UPDATE
                        total_solar_energy = VALUES(total_solar_energy),
                        total_wind_energy = VALUES(total_wind_energy),
                        total_energy_generated = VALUES(total_energy_generated),
                        total_grid_import = VALUES(total_grid_import),
                        total_grid_export = VALUES(total_grid_export),
                        avg_efficiency_score = VALUES(avg_efficiency_score),
                        max_efficiency_score = VALUES(max_efficiency_score),
                        min_efficiency_score = VALUES(min_efficiency_score),
                        total_cost = VALUES(total_cost),
                        peak_power = VALUES(peak_power),
                        min_power = VALUES(min_power)
                """
                
                cursor.execute(sql, (yesterday,))
                self.connection.commit()
                
                logger.info(f"Updated daily summary for {yesterday}")
                
        except Exception as e:
            logger.error(f"Error updating daily summary: {e}")
            if self.connection:
                self.connection.rollback()
    
    def process_message(self, message: dict) -> bool:
        """메시지 처리"""
        try:
            # 제어 결과 저장
            success = self.store_control_result(message)
            
            if success:
                logger.info(f"Successfully processed and stored result: {message['result_id']}")
            else:
                logger.error(f"Failed to store result: {message['result_id']}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def run(self):
        """서비스 실행"""
        logger.info("Starting Database Storage Service...")
        
        message_count = 0
        last_daily_update = datetime.now().date()
        
        try:
            for message in self.consumer:
                data = message.value
                logger.info(f"Received control result from topic '{self.input_topic}'")
                
                # 메시지 처리
                success = self.process_message(data)
                
                if success:
                    message_count += 1
                    
                    # 100개 메시지마다 일별 요약 업데이트
                    if message_count % 100 == 0:
                        current_date = datetime.now().date()
                        if current_date != last_daily_update:
                            self.update_daily_summary()
                            last_daily_update = current_date
                
        except KeyboardInterrupt:
            logger.info("Shutting down Database Storage Service...")
        except Exception as e:
            logger.error(f"Service error: {e}")
        finally:
            if self.connection:
                self.connection.close()
            self.consumer.close()

if __name__ == "__main__":
    # 데이터베이스가 준비될 때까지 대기
    time.sleep(20)
    
    service = DatabaseStorageService()
    service.run()