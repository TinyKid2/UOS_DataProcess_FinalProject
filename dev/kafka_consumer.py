from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from datetime import datetime
import sqlite3
import os
from typing import Dict, List, Optional
import signal
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SolarDataKafkaConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', group_id: str = 'solar_consumer_group'):
        """
        Kafka Consumer for solar power generation data
        Based on textbook Chapter 7 - Message Queue System
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.db_path = os.path.join(os.path.dirname(__file__), "..", "data", "kafka_consumed_data.db")
        self.consumer = None
        self.running = False
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_database()
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """신호 처리를 위한 핸들러"""
        logger.info("종료 신호 수신, Consumer 종료 중...")
        self.running = False
        if self.consumer:
            self.consumer.close()
        sys.exit(0)
    
    def init_database(self):
        """소비된 데이터를 저장할 데이터베이스를 초기화합니다"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS consumed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT,
                    partition INTEGER,
                    offset INTEGER,
                    timestamp TEXT,
                    key TEXT,
                    transaction_date TEXT,
                    transaction_hour INTEGER,
                    region TEXT,
                    solar_generation_mwh REAL,
                    consumed_at TEXT,
                    UNIQUE(topic, partition, offset)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS consumption_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT,
                    messages_consumed INTEGER,
                    start_time TEXT,
                    end_time TEXT,
                    avg_processing_time_ms REAL
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"데이터베이스 초기화 완료: {self.db_path}")
        except Exception as e:
            logger.error(f"데이터베이스 초기화 실패: {str(e)}")
            raise
    
    def create_consumer(self, topics: List[str]):
        """Kafka Consumer를 생성합니다"""
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"Kafka Consumer 생성 완료 - Topics: {topics}, Group ID: {self.group_id}")
            return True
        except Exception as e:
            logger.error(f"Consumer 생성 실패: {str(e)}")
            return False
    
    def process_message(self, message) -> bool:
        """개별 메시지를 처리합니다"""
        try:
            start_time = datetime.now()
            
            value = message.value
            if isinstance(value, dict) and 'record' in value:
                record = value['record']
            else:
                record = value
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO consumed_messages 
                    (topic, partition, offset, timestamp, key, 
                     transaction_date, transaction_hour, region, solar_generation_mwh, consumed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    message.topic,
                    message.partition,
                    message.offset,
                    datetime.fromtimestamp(message.timestamp/1000).isoformat(),
                    message.key,
                    record.get('거래일자'),
                    record.get('거래시간'),
                    record.get('지역'),
                    record.get('태양광 발전량(MWh)'),
                    datetime.now().isoformat()
                ))
                
                conn.commit()
                
                processing_time = (datetime.now() - start_time).total_seconds() * 1000
                
                logger.debug(f"메시지 처리 완료 - Topic: {message.topic}, "
                           f"Partition: {message.partition}, Offset: {message.offset}, "
                           f"처리시간: {processing_time:.2f}ms")
                
                return True
                
            except sqlite3.IntegrityError:
                logger.debug(f"중복 메시지 스킵 - Offset: {message.offset}")
                return False
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"메시지 처리 실패: {str(e)}")
            return False
    
    def consume_messages(self, topics: List[str], max_messages: Optional[int] = None):
        """메시지를 소비합니다"""
        if not self.create_consumer(topics):
            return
        
        self.running = True
        messages_processed = 0
        start_time = datetime.now()
        success_count = 0
        
        logger.info(f"메시지 소비 시작 - Topics: {topics}")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                if self.process_message(message):
                    success_count += 1
                
                messages_processed += 1
                
                if messages_processed % 100 == 0:
                    logger.info(f"처리 진행 상황: {messages_processed} 메시지 처리됨")
                
                if max_messages and messages_processed >= max_messages:
                    logger.info(f"최대 메시지 수 도달: {max_messages}")
                    break
                    
        except KeyboardInterrupt:
            logger.info("사용자에 의한 중단")
        except Exception as e:
            logger.error(f"메시지 소비 중 오류: {str(e)}")
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.save_consumption_stats(
                topics[0] if topics else 'unknown',
                messages_processed,
                start_time,
                end_time,
                (duration / messages_processed * 1000) if messages_processed > 0 else 0
            )
            
            logger.info(f"메시지 소비 종료 - 총 처리: {messages_processed}, "
                       f"성공: {success_count}, 소요시간: {duration:.2f}초")
            
            if self.consumer:
                self.consumer.close()
    
    def save_consumption_stats(self, topic: str, messages: int, start_time: datetime, 
                              end_time: datetime, avg_time: float):
        """소비 통계를 저장합니다"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO consumption_stats 
                (topic, messages_consumed, start_time, end_time, avg_processing_time_ms)
                VALUES (?, ?, ?, ?, ?)
            ''', (topic, messages, start_time.isoformat(), end_time.isoformat(), avg_time))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"통계 저장 실패: {str(e)}")
    
    def get_consumption_report(self) -> str:
        """소비 리포트를 생성합니다"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*), 
                       COUNT(DISTINCT region),
                       AVG(solar_generation_mwh),
                       MAX(solar_generation_mwh),
                       MIN(consumed_at),
                       MAX(consumed_at)
                FROM consumed_messages
            ''')
            
            stats = cursor.fetchone()
            
            cursor.execute('''
                SELECT region, COUNT(*), AVG(solar_generation_mwh)
                FROM consumed_messages
                WHERE solar_generation_mwh IS NOT NULL
                GROUP BY region
                ORDER BY AVG(solar_generation_mwh) DESC
                LIMIT 5
            ''')
            
            top_regions = cursor.fetchall()
            
            conn.close()
            
            report = []
            report.append("=" * 60)
            report.append("Kafka Consumer 리포트")
            report.append("=" * 60)
            report.append(f"총 소비 메시지: {stats[0]:,}")
            report.append(f"지역 수: {stats[1]}")
            report.append(f"평균 발전량: {stats[2]:.2f} MWh" if stats[2] else "평균 발전량: N/A")
            report.append(f"최대 발전량: {stats[3]:.2f} MWh" if stats[3] else "최대 발전량: N/A")
            report.append(f"소비 기간: {stats[4]} ~ {stats[5]}")
            report.append("\nTop 5 지역별 평균 발전량:")
            
            for region, count, avg in top_regions:
                report.append(f"  - {region}: {avg:.2f} MWh ({count} 메시지)")
            
            report.append("=" * 60)
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"리포트 생성 실패: {str(e)}")
            return "리포트 생성 실패"
    
    def reset_offset(self, topic: str):
        """특정 토픽의 오프셋을 리셋합니다"""
        try:
            from kafka import TopicPartition
            
            temp_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id
            )
            
            partitions = temp_consumer.partitions_for_topic(topic)
            if partitions:
                for partition in partitions:
                    tp = TopicPartition(topic, partition)
                    temp_consumer.seek_to_beginning(tp)
                logger.info(f"토픽 '{topic}'의 오프셋을 처음으로 리셋했습니다")
            
            temp_consumer.close()
            
        except Exception as e:
            logger.error(f"오프셋 리셋 실패: {str(e)}")


def main():
    """메인 실행 함수"""
    print("\n태양광 발전량 Kafka Consumer")
    print("=" * 50)
    print("1. 메시지 소비 시작 (무제한)")
    print("2. 메시지 소비 시작 (제한된 수)")
    print("3. 소비 리포트 보기")
    print("4. 오프셋 리셋")
    print("5. 종료")
    print("=" * 50)
    
    consumer = None
    
    try:
        kafka_server = input("Kafka 서버 주소 (기본값: localhost:9092): ").strip() or "localhost:9092"
        group_id = input("Consumer Group ID (기본값: solar_consumer_group): ").strip() or "solar_consumer_group"
        
        consumer = SolarDataKafkaConsumer(bootstrap_servers=kafka_server, group_id=group_id)
        
        while True:
            choice = input("\n선택하세요 (1-5): ").strip()
            
            if choice == '1':
                topics = input("구독할 토픽 (쉼표로 구분, 기본값: solar_raw_data): ").strip() or "solar_raw_data"
                topic_list = [t.strip() for t in topics.split(',')]
                print("메시지 소비를 시작합니다. 중단하려면 Ctrl+C를 누르세요.")
                consumer.consume_messages(topic_list)
                
            elif choice == '2':
                topics = input("구독할 토픽 (쉼표로 구분, 기본값: solar_raw_data): ").strip() or "solar_raw_data"
                max_messages = int(input("최대 메시지 수 (기본값: 100): ") or "100")
                topic_list = [t.strip() for t in topics.split(',')]
                consumer.consume_messages(topic_list, max_messages=max_messages)
                
            elif choice == '3':
                report = consumer.get_consumption_report()
                print(report)
                
            elif choice == '4':
                topic = input("리셋할 토픽 이름: ").strip()
                if topic:
                    consumer.reset_offset(topic)
                
            elif choice == '5':
                print("프로그램을 종료합니다.")
                break
                
            else:
                print("올바른 선택지를 입력해주세요 (1-5)")
                
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류: {str(e)}")
    finally:
        if consumer and consumer.consumer:
            consumer.consumer.close()


if __name__ == "__main__":
    main()