#!/usr/bin/env python3
"""
마이크로그리드 시스템 모니터링 스크립트
Kafka 토픽과 데이터베이스 상태를 실시간으로 모니터링
"""

import pymysql
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
import threading
import os
import sys

class MicrogridMonitor:
    def __init__(self):
        self.kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
        self.db_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': 13306,
            'user': 'microgrid_user',
            'password': 'microgrid_password',
            'database': 'microgrid_control',
            'charset': 'utf8mb4'
        }
        
        self.topics = [
            'power_generation_data',
            'control_optimization_request', 
            'control_result'
        ]
        
        self.monitoring = True
        
    def connect_database(self):
        """데이터베이스 연결"""
        try:
            return pymysql.connect(**self.db_config)
        except Exception as e:
            print(f"❌ DB 연결 실패: {e}")
            return None
    
    def monitor_kafka_topics(self):
        """Kafka 토픽 모니터링"""
        print("\n📡 Kafka 토픽 모니터링 시작...")
        
        consumers = {}
        message_counts = {topic: 0 for topic in self.topics}
        
        try:
            # 각 토픽에 대한 컨슈머 생성
            for topic in self.topics:
                consumers[topic] = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.kafka_bootstrap,
                    auto_offset_reset='latest',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=1000
                )
                print(f"✅ {topic} 토픽 구독 완료")
        except Exception as e:
            print(f"❌ Kafka 연결 실패: {e}")
            return
        
        print("\n실시간 메시지 모니터링 (Ctrl+C로 중지):")
        print("-" * 80)
        
        while self.monitoring:
            for topic, consumer in consumers.items():
                for message in consumer:
                    message_counts[topic] += 1
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    
                    # 메시지 내용 요약
                    data = message.value
                    
                    if topic == 'power_generation_data':
                        summary = f"Location: {data.get('location')}, Solar: {data.get('solar_power')}kW, Wind: {data.get('wind_power')}kW"
                    elif topic == 'control_optimization_request':
                        summary = f"Location: {data.get('location')}, Total: {data.get('power_data', {}).get('total')}kW, Quality: {data.get('quality_score')}"
                    elif topic == 'control_result':
                        summary = f"Location: {data.get('location')}, Efficiency: {data.get('metrics', {}).get('efficiency_score'):.2f}, Cost: {data.get('metrics', {}).get('estimated_cost'):.0f}원"
                    else:
                        summary = str(data)[:100]
                    
                    print(f"[{timestamp}] 📨 {topic}: {summary}")
            
            time.sleep(0.1)
        
        # 종료 시 통계 출력
        print("\n" + "=" * 80)
        print("📊 Kafka 메시지 통계:")
        for topic, count in message_counts.items():
            print(f"  - {topic}: {count} 메시지")
        
        # 컨슈머 닫기
        for consumer in consumers.values():
            consumer.close()
    
    def monitor_database(self):
        """데이터베이스 상태 모니터링"""
        conn = self.connect_database()
        if not conn:
            return
        
        print("\n💾 데이터베이스 모니터링:")
        print("-" * 80)
        
        try:
            with conn.cursor() as cursor:
                # 1. 최근 제어 결과
                cursor.execute("""
                    SELECT location, 
                           COUNT(*) as count,
                           AVG(efficiency_score) as avg_efficiency,
                           AVG(estimated_cost) as avg_cost,
                           MAX(timestamp) as last_update
                    FROM control_results
                    WHERE timestamp > DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    GROUP BY location
                """)
                
                results = cursor.fetchall()
                
                print("\n📈 최근 1시간 제어 결과:")
                if results:
                    for row in results:
                        print(f"  • {row[0]}: {row[1]}개 레코드, 평균 효율: {row[2]:.2f}, 평균 비용: {row[3]:.0f}원")
                        print(f"    마지막 업데이트: {row[4]}")
                else:
                    print("  데이터 없음")
                
                # 2. 시스템 이벤트
                cursor.execute("""
                    SELECT event_type, severity, COUNT(*) as count
                    FROM system_events
                    WHERE timestamp > DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    GROUP BY event_type, severity
                    ORDER BY count DESC
                    LIMIT 10
                """)
                
                events = cursor.fetchall()
                
                print("\n⚠️  최근 시스템 이벤트:")
                if events:
                    for event in events:
                        severity_icon = "🔴" if event[1] == "ERROR" else "🟡" if event[1] == "WARNING" else "🔵"
                        print(f"  {severity_icon} {event[0]}: {event[2]}건")
                else:
                    print("  이벤트 없음")
                
                # 3. 마이크로그리드별 현재 상태
                cursor.execute("""
                    SELECT m.location, m.name, 
                           cr.battery_soc, cr.efficiency_score, cr.timestamp
                    FROM microgrids m
                    LEFT JOIN (
                        SELECT location, battery_soc, efficiency_score, timestamp,
                               ROW_NUMBER() OVER (PARTITION BY location ORDER BY timestamp DESC) as rn
                        FROM control_results
                    ) cr ON m.location = cr.location AND cr.rn = 1
                """)
                
                grids = cursor.fetchall()
                
                print("\n🏭 마이크로그리드 상태:")
                for grid in grids:
                    if grid[2] is not None:
                        print(f"  • {grid[0]} ({grid[1]})")
                        print(f"    배터리 SOC: {grid[2]:.1f}%, 효율: {grid[3]:.2f}")
                    else:
                        print(f"  • {grid[0]} ({grid[1]}) - 데이터 없음")
                
        except Exception as e:
            print(f"❌ 모니터링 오류: {e}")
        finally:
            conn.close()
    
    def continuous_monitoring(self, interval=30):
        """연속 모니터링 모드"""
        print("=" * 80)
        print("🔍 마이크로그리드 시스템 연속 모니터링")
        print("=" * 80)
        
        # Kafka 모니터링을 별도 스레드에서 실행
        kafka_thread = threading.Thread(target=self.monitor_kafka_topics)
        kafka_thread.daemon = True
        kafka_thread.start()
        
        # 주기적으로 DB 상태 확인
        try:
            while True:
                self.monitor_database()
                print(f"\n⏳ {interval}초 후 다음 업데이트...")
                time.sleep(interval)
                print("\n" + "=" * 80)
                
        except KeyboardInterrupt:
            print("\n\n모니터링 중지...")
            self.monitoring = False
            kafka_thread.join(timeout=2)
    
    def get_statistics(self):
        """시스템 통계 조회"""
        conn = self.connect_database()
        if not conn:
            return
        
        print("=" * 80)
        print("📊 마이크로그리드 시스템 통계")
        print("=" * 80)
        
        try:
            with conn.cursor() as cursor:
                # 전체 통계
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        AVG(solar_power) as avg_solar,
                        AVG(wind_power) as avg_wind,
                        AVG(efficiency_score) as avg_efficiency,
                        SUM(estimated_cost) as total_cost,
                        MIN(timestamp) as first_record,
                        MAX(timestamp) as last_record
                    FROM control_results
                """)
                
                stats = cursor.fetchone()
                
                if stats[0] > 0:
                    print(f"\n📈 전체 통계:")
                    print(f"  • 총 레코드: {stats[0]:,}개")
                    print(f"  • 평균 태양광 발전: {stats[1]:.1f}kW")
                    print(f"  • 평균 풍력 발전: {stats[2]:.1f}kW")
                    print(f"  • 평균 효율성: {stats[3]:.2%}")
                    print(f"  • 총 비용: {stats[4]:,.0f}원")
                    print(f"  • 데이터 기간: {stats[5]} ~ {stats[6]}")
                
                # 시간대별 패턴
                cursor.execute("""
                    SELECT 
                        HOUR(timestamp) as hour,
                        AVG(solar_power) as avg_solar,
                        AVG(wind_power) as avg_wind,
                        AVG(efficiency_score) as avg_efficiency
                    FROM control_results
                    GROUP BY HOUR(timestamp)
                    ORDER BY hour
                """)
                
                hourly = cursor.fetchall()
                
                if hourly:
                    print(f"\n⏰ 시간대별 평균 패턴:")
                    print(f"  시간 | 태양광(kW) | 풍력(kW) | 효율성")
                    print(f"  -----|-----------|---------|-------")
                    for row in hourly:
                        print(f"  {row[0]:2d}시 | {row[1]:9.1f} | {row[2]:7.1f} | {row[3]:.2%}")
                
        except Exception as e:
            print(f"❌ 통계 조회 오류: {e}")
        finally:
            conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='마이크로그리드 시스템 모니터')
    parser.add_argument('--mode', choices=['continuous', 'stats', 'kafka', 'db'], 
                       default='continuous', help='모니터링 모드')
    parser.add_argument('--interval', type=int, default=30, 
                       help='업데이트 간격(초)')
    
    args = parser.parse_args()
    
    monitor = MicrogridMonitor()
    
    try:
        if args.mode == 'continuous':
            monitor.continuous_monitoring(args.interval)
        elif args.mode == 'stats':
            monitor.get_statistics()
        elif args.mode == 'kafka':
            monitor.monitor_kafka_topics()
        elif args.mode == 'db':
            monitor.monitor_database()
            
    except KeyboardInterrupt:
        print("\n\n모니터링 종료")
        sys.exit(0)

if __name__ == "__main__":
    main()