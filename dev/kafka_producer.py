from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import time
from datetime import datetime
import requests
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SolarDataKafkaProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """
        Kafka Producer for solar power generation data
        Based on textbook Chapter 7 - Message Queue System
        """
        self.bootstrap_servers = bootstrap_servers
        self.api_url = "https://api.odcloud.kr/api/15065269/v1/uddi:166a7ee3-161a-4dc2-9f59-cedb6b1c7213"
        self.service_key = "9562cdd6a0ca84905cca10836104720138ae6edf962921c1d29c67f6e72fc8db"
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            logger.info(f"Kafka Producer 초기화 완료 - 서버: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Kafka Producer 초기화 실패: {str(e)}")
            raise
    
    def fetch_solar_data(self, page: int = 1, per_page: int = 100) -> List[Dict]:
        """API로부터 태양광 데이터를 수집합니다"""
        try:
            params = {
                'page': page,
                'perPage': per_page,
                'serviceKey': self.service_key
            }
            
            response = requests.get(self.api_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    logger.info(f"페이지 {page} 데이터 수집 완료 - {len(data['data'])} 건")
                    return data['data']
            else:
                logger.error(f"API 요청 실패 - 상태코드: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"데이터 수집 중 오류: {str(e)}")
            return []
    
    def produce_to_topic(self, topic: str, data: List[Dict], partition_key: str = None):
        """데이터를 Kafka 토픽으로 전송합니다"""
        success_count = 0
        error_count = 0
        
        for record in data:
            try:
                enriched_record = {
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'solar_api',
                    'record': record
                }
                
                if partition_key:
                    key = f"{partition_key}_{record.get('지역', 'unknown')}"
                else:
                    key = record.get('지역', 'unknown')
                
                future = self.producer.send(
                    topic,
                    key=key,
                    value=enriched_record
                )
                
                record_metadata = future.get(timeout=10)
                success_count += 1
                
                logger.debug(f"메시지 전송 성공 - Topic: {record_metadata.topic}, "
                           f"Partition: {record_metadata.partition}, "
                           f"Offset: {record_metadata.offset}")
                
            except KafkaError as e:
                error_count += 1
                logger.error(f"Kafka 메시지 전송 실패: {str(e)}")
            except Exception as e:
                error_count += 1
                logger.error(f"예상치 못한 오류: {str(e)}")
        
        self.producer.flush()
        
        logger.info(f"Kafka 전송 완료 - 성공: {success_count}, 실패: {error_count}")
        return {'success': success_count, 'failed': error_count}
    
    def stream_realtime_data(self, topic: str = 'solar_raw_data', interval_seconds: int = 60):
        """실시간 스트리밍 모드로 데이터를 지속적으로 전송합니다"""
        logger.info(f"실시간 스트리밍 시작 - Topic: {topic}, 간격: {interval_seconds}초")
        
        page = 1
        max_pages = 100
        
        try:
            while True:
                data = self.fetch_solar_data(page=page, per_page=10)
                
                if data:
                    result = self.produce_to_topic(topic, data, partition_key='stream')
                    logger.info(f"스트림 페이지 {page} 전송 완료 - {result}")
                
                page = (page % max_pages) + 1
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("스트리밍 중단 요청")
        except Exception as e:
            logger.error(f"스트리밍 중 오류 발생: {str(e)}")
        finally:
            self.close()
    
    def produce_batch_data(self, topic: str = 'solar_raw_data', pages: int = 5, per_page: int = 100):
        """배치 모드로 다량의 데이터를 한번에 전송합니다"""
        logger.info(f"배치 전송 시작 - Topic: {topic}, Pages: {pages}")
        
        total_success = 0
        total_failed = 0
        
        for page in range(1, pages + 1):
            data = self.fetch_solar_data(page=page, per_page=per_page)
            
            if data:
                result = self.produce_to_topic(topic, data, partition_key='batch')
                total_success += result['success']
                total_failed += result['failed']
                
            time.sleep(0.5)
        
        logger.info(f"배치 전송 완료 - 총 성공: {total_success}, 총 실패: {total_failed}")
        return {'total_success': total_success, 'total_failed': total_failed}
    
    def send_control_message(self, topic: str, message_type: str, payload: Dict):
        """제어 메시지를 전송합니다 (ETL 파이프라인 제어용)"""
        control_message = {
            'message_type': message_type,
            'timestamp': datetime.now().isoformat(),
            'payload': payload
        }
        
        try:
            future = self.producer.send(topic, key='control', value=control_message)
            metadata = future.get(timeout=10)
            logger.info(f"제어 메시지 전송 성공 - Type: {message_type}")
            return True
        except Exception as e:
            logger.error(f"제어 메시지 전송 실패: {str(e)}")
            return False
    
    def close(self):
        """Producer를 안전하게 종료합니다"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka Producer 종료 완료")
        except Exception as e:
            logger.error(f"Producer 종료 중 오류: {str(e)}")


def main():
    """메인 실행 함수"""
    print("\n태양광 발전량 Kafka Producer")
    print("=" * 50)
    print("1. 배치 데이터 전송")
    print("2. 실시간 스트리밍 시작")
    print("3. 테스트 메시지 전송")
    print("4. 종료")
    print("=" * 50)
    
    producer = None
    
    try:
        kafka_server = input("Kafka 서버 주소 (기본값: localhost:9092): ").strip() or "localhost:9092"
        producer = SolarDataKafkaProducer(bootstrap_servers=kafka_server)
        
        while True:
            choice = input("\n선택하세요 (1-4): ").strip()
            
            if choice == '1':
                topic = input("토픽 이름 (기본값: solar_raw_data): ").strip() or "solar_raw_data"
                pages = int(input("전송할 페이지 수 (기본값: 5): ") or "5")
                result = producer.produce_batch_data(topic=topic, pages=pages)
                print(f"배치 전송 결과: {result}")
                
            elif choice == '2':
                topic = input("토픽 이름 (기본값: solar_raw_data): ").strip() or "solar_raw_data"
                interval = int(input("전송 간격(초) (기본값: 60): ") or "60")
                print("스트리밍을 시작합니다. 중단하려면 Ctrl+C를 누르세요.")
                producer.stream_realtime_data(topic=topic, interval_seconds=interval)
                
            elif choice == '3':
                test_data = [{
                    '거래일자': datetime.now().strftime('%Y-%m-%d'),
                    '거래시간': datetime.now().hour,
                    '지역': 'TEST',
                    '태양광 발전량(MWh)': 100.0
                }]
                result = producer.produce_to_topic('test_topic', test_data)
                print(f"테스트 메시지 전송 결과: {result}")
                
            elif choice == '4':
                print("프로그램을 종료합니다.")
                break
            else:
                print("올바른 선택지를 입력해주세요 (1-4)")
                
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류: {str(e)}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()