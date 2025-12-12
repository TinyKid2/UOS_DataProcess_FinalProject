# 태양광 발전량 ETL 파이프라인 및 예측 시스템

## 프로젝트 개요

공공데이터 포털의 태양광 발전량 데이터를 활용하여 ETL(Extract, Transform, Load) 파이프라인을 구축하고, 머신러닝 기반 발전량 예측 시스템을 구현한 프로젝트입니다.

### 주요 특징
- 실시간 데이터 수집 및 처리
- Kafka 기반 메시지 큐 시스템
- Spark를 활용한 대용량 데이터 처리
- 머신러닝 기반 발전량 예측

## 시스템 아키텍처

```
[API Data Source] 
    ↓
[Kafka Producer] → [Kafka Broker] → [Kafka Consumer]
    ↓                    ↓                ↓
[Main ETL]         [Spark Stream]    [Database]
    ↓                    ↓                ↓
[ML Model]         [Analytics]      [Reports]
```

## 파일 구조

```
project/
├── dev/
│   ├── main.py              # 메인 ETL 파이프라인
│   ├── kafka_producer.py    # Kafka 데이터 프로듀서
│   ├── kafka_consumer.py    # Kafka 데이터 컨슈머
│   ├── spark_processor.py   # Spark 기반 데이터 처리
│   └── requirements.txt     # 패키지 의존성
├── data/                     # 데이터 저장 디렉토리
└── README.md
```

## 설치 방법

### 1. 필수 요구사항
- Python 3.8+
- Apache Kafka (선택사항)
- Apache Spark (선택사항)

### 2. 패키지 설치
```bash
pip install -r dev/requirements.txt
```

### 3. Kafka 설치 (선택사항)
```bash
# Kafka 다운로드 및 실행
wget https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0

# Zookeeper 실행
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka 서버 실행
bin/kafka-server-start.sh config/server.properties
```

## 사용 방법

### 1. 기본 ETL 파이프라인 실행

```bash
cd dev
python main.py
```

메뉴 옵션:
1. ETL 파이프라인 실행 - 데이터 수집, 변환, 적재, 모델 학습
2. 발전량 예측 - 학습된 모델로 예측
3. 데이터 분석 리포트 생성
4. 종료

### 2. Kafka Producer 실행

```bash
python kafka_producer.py
```

실시간 또는 배치 모드로 데이터를 Kafka 토픽으로 전송합니다.

### 3. Kafka Consumer 실행

```bash
python kafka_consumer.py
```

Kafka 토픽에서 메시지를 소비하고 데이터베이스에 저장합니다.

### 4. Spark 처리 실행

```bash
python spark_processor.py
```

대용량 데이터 처리 및 고급 분석을 수행합니다.

## 주요 기능

### 1. 데이터 수집 (Extract)
- 공공데이터 포털 API를 통한 실시간 데이터 수집
- 페이지네이션 지원
- 에러 핸들링 및 재시도 로직

### 2. 데이터 변환 (Transform)
- 날짜/시간 파싱 및 형식 변환
- 특성 엔지니어링 (주말/평일, 시간대별 카테고리 등)
- 통계적 집계 (일별, 지역별)
- 결측치 처리

### 3. 데이터 적재 (Load)
- SQLite 데이터베이스 저장
- 다층 테이블 구조 (원본, 시간별, 일별, 지역별)
- Parquet 형식 지원 (Spark)

### 4. 머신러닝 예측
- Random Forest 기반 발전량 예측
- 특성 중요도 분석
- 모델 성능 평가 (RMSE, R2 Score)

### 5. Kafka 통합
- 실시간 스트리밍 데이터 처리
- 메시지 큐 기반 비동기 처리
- 파티셔닝 및 컨슈머 그룹 지원

### 6. Spark 처리
- 대용량 데이터 배치 처리
- 실시간 스트림 처리
- ML Pipeline 구축
- Window 함수 기반 시계열 분석

## API 정보

- **데이터 출처**: 한국전력거래소 태양광 발전량 데이터
- **API URL**: https://api.odcloud.kr/api/15065269/v1/uddi:166a7ee3-161a-4dc2-9f59-cedb6b1c7213
- **데이터 필드**:
  - 거래일자: 발전 날짜
  - 거래시간: 발전 시간 (1-24)
  - 지역: 발전 지역
  - 태양광 발전량(MWh): 시간당 발전량

## 성능 최적화

- 배치 처리로 API 호출 최소화
- 데이터베이스 인덱싱
- Kafka 압축 (gzip)
- Spark 적응형 쿼리 실행
- 파티셔닝 기반 병렬 처리

## 모니터링 및 로깅

- 상세한 로깅 (INFO, ERROR 레벨)
- 처리 통계 실시간 출력
- 에러 추적 및 리포팅
- 성능 메트릭 수집

## 확장 가능성

1. **실시간 대시보드**: Grafana/Kibana 연동
2. **고급 예측 모델**: LSTM, Prophet 등 시계열 모델
3. **분산 처리**: Kubernetes 클러스터 배포
4. **API 서버**: Flask/FastAPI 기반 REST API
5. **데이터 웨어하우스**: AWS S3, Google BigQuery 연동

## 트러블슈팅

### Kafka 연결 실패
```bash
# Kafka 서버 상태 확인
jps | grep Kafka

# 토픽 생성
bin/kafka-topics.sh --create --topic solar_raw_data --bootstrap-server localhost:9092
```

### Spark 메모리 부족
```python
# SparkSession 설정 조정
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

### API 호출 제한
- 페이지당 레코드 수 조정
- 호출 간격 증가 (time.sleep)

## 라이선스

이 프로젝트는 교육 목적으로 작성되었습니다.

## 참고 자료

- [공공데이터 포털](https://www.data.go.kr/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- 데이터사이언스를 위한 지식관리시스템 교재