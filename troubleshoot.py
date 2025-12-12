#!/usr/bin/env python3
"""
Kafka 연결 문제 해결 스크립트
"""

import os
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json

def test_kafka_connection(bootstrap_servers):
    """Kafka 연결 테스트"""
    print(f"\n🔍 Kafka 연결 테스트: {bootstrap_servers}")
    
    try:
        # Producer 연결 시도
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000,
            request_timeout_ms=10000
        )
        
        # 테스트 메시지 전송
        test_message = {"test": "connection", "timestamp": time.time()}
        future = producer.send('test_topic', value=test_message)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Kafka 연결 성공!")
        print(f"   - Topic: {record_metadata.topic}")
        print(f"   - Partition: {record_metadata.partition}")
        print(f"   - Offset: {record_metadata.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka 연결 실패: {e}")
        return False

def check_kafka_topics(bootstrap_servers):
    """Kafka 토픽 확인"""
    print(f"\n📋 Kafka 토픽 확인...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000
        )
        
        # 필요한 토픽 생성
        required_topics = [
            'power_generation_data',
            'control_optimization_request',
            'control_result'
        ]
        
        # 토픽 리스트 가져오기
        metadata = admin_client.list_topics()
        existing_topics = metadata
        
        print(f"✅ 현재 토픽: {existing_topics}")
        
        # 없는 토픽 생성
        topics_to_create = []
        for topic in required_topics:
            if topic not in existing_topics:
                topics_to_create.append(
                    NewTopic(name=topic, num_partitions=1, replication_factor=1)
                )
        
        if topics_to_create:
            print(f"📝 토픽 생성 중: {[t.name for t in topics_to_create]}")
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"✅ 토픽 생성 완료")
        else:
            print(f"✅ 모든 필수 토픽이 이미 존재합니다")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ 토픽 확인/생성 실패: {e}")
        return False

def test_web_server():
    """웹서버 연결 테스트"""
    import requests
    
    print(f"\n🌐 웹서버 테스트...")
    
    try:
        response = requests.get("http://localhost:15000/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ 웹서버 상태: {data['status']}")
            print(f"   - Kafka: {data['kafka']}")
            return True
        else:
            print(f"❌ 웹서버 응답 오류: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 웹서버 연결 실패: {e}")
        return False

def main():
    print("=" * 60)
    print("🔧 마이크로그리드 시스템 트러블슈팅")
    print("=" * 60)
    
    # 다양한 Kafka 주소 시도
    kafka_addresses = [
        'localhost:19092',      # 외부 포트
        'localhost:29092',      # 내부 포트
        'kafka:29092',          # Docker 네트워크 내부
        'localhost:9092',       # 기본 포트
        '127.0.0.1:19092',     # IP 주소
    ]
    
    print("\n1️⃣ Kafka 연결 테스트")
    kafka_connected = False
    working_address = None
    
    for address in kafka_addresses:
        if test_kafka_connection(address):
            kafka_connected = True
            working_address = address
            print(f"\n🎉 작동하는 Kafka 주소: {address}")
            break
        time.sleep(1)
    
    if not kafka_connected:
        print("\n❌ Kafka에 연결할 수 없습니다.")
        print("\n해결 방법:")
        print("1. Docker Desktop이 실행 중인지 확인")
        print("2. 다음 명령 실행:")
        print("   docker-compose down")
        print("   docker-compose up -d")
        print("3. 30초 대기 후 다시 시도")
        sys.exit(1)
    
    # 토픽 확인
    if working_address:
        print("\n2️⃣ Kafka 토픽 확인")
        check_kafka_topics(working_address)
    
    # 웹서버 테스트
    print("\n3️⃣ 웹서버 상태 확인")
    test_web_server()
    
    print("\n" + "=" * 60)
    print("📌 권장 설정:")
    print(f"   KAFKA_BOOTSTRAP_SERVERS={working_address}")
    print("\n💡 웹서버 환경변수 설정:")
    print(f"   export KAFKA_BOOTSTRAP_SERVERS={working_address}")
    print("\n또는 Docker Compose 재시작:")
    print("   docker-compose down")
    print("   docker-compose up -d")
    print("=" * 60)

if __name__ == "__main__":
    main()