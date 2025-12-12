# 주제: 임의의 데이터를 선정하여 (ETL) 수집, 변환, 적재 파이프라인 구성

## 시나리오: YOLO 모델 자동 평가 시스템

### 1. 개요
* **목표:** 수강생들이 제출한 AI 모델(YOLO) 가중치 파일을 수집하여 자동으로 성능을 평가하고, 그 결과를 데이터베이스에 적재하는 ETL 파이프라인 구축
* **작성일:** 202X.XX.XX

---

### 2. 필수 서술사항 1: 사용한 데이터 설명

본 시나리오에서는 **사용자 제출 데이터**와 파이프라인 내부에서 생성되는 **가공 데이터**를 다룹니다.

* **원본 데이터 (Source Data)**
    * **Model Weights:** 이미지 핸들링 수강생들이 제출한 YOLO 모델의 가중치 파일 (`.pt`, Binary 형태)
    * **Submission ID:** 제출자를 식별하기 위해 서버에서 발행한 임의의 UUID
* **데이터 특징**
    * 개별 Weight 파일의 크기는 **10MB 미만**임
    * 데이터는 Binary File(모델)과 Metadata(ID, 경로)로 구성됨

---

### 3. 데이터 플로우 디자인 고려사항

파이프라인 설계 단계에서 **Evaluation Node(평가 서버)로의 데이터 전송 방식**에 대해 다음과 같은 고민이 있었습니다.

| 방안 | 내용 | 장점 | 단점 | 최종 결정 |
| :--- | :--- | :--- | :--- | :---: |
| **1안** | 로컬 평가 노드에서 **직접 다운로드** | 구현이 단순함 | 클라우드 인스턴스 아웃바운드 트래픽 발생 | **채택** |
| **2안** | 구글 드라이브 **링크 제출** | 트래픽 비용 절감 | 수강생에게 개인 저장소 사용 강요 (UX 저하) | 기각 |

> **결정 사유:** 파일 크기가 10MB 미만으로 트래픽 부하가 크지 않다고 판단하여, 사용자 편의성을 위해 **1안(직접 다운로드)**을 채택하였습니다.

---

### 4. 필수 서술사항 2: 데이터 플로우 디자인

#### [Architecture Diagram]
```mermaid
graph LR
    User[수강생] -->|Upload| Web[웹 서버]
    Web -->|Log/File| NiFi1[NiFi (Source)]
    NiFi1 -->|Produce| Kafka1[Kafka: model_submission]
    
    Kafka1 -->|Consume| EvalNode[Evaluation Node]
    EvalNode -->|Inference| Model[YOLO Model]
    Model -->|Result File| NiFi2[NiFi (Eval Node)]
    NiFi2 -->|Produce| Kafka2[Kafka: eval_result]
    
    Kafka2 -->|Consume| CloudDB[Cloud Server]
    CloudDB -->|Insert| DB[(MariaDB)]
```

■ 단계 1: 수집 (Extract)
웹 서버에 업로드된 파일을 감지하여 메시지 큐로 전달하는 단계입니다.

순서 1: 웹 업로드 및 로깅

수강생이 웹사이트에 파일 업로드

서버는 UUID를 발행하고 파일을 로컬 디스크에 저장

순서 2: Kafka Producer 1 (Apache NiFi)

도구: Apache NiFi (GetFile 컴포넌트 활용)

동작: 서버의 로그/파일 디렉토리를 감지하여 새로운 제출 건을 캡처

Topic: model_submission

Payload:

JSON

{ "id": "1234-4567-123123123", "file": "test.pt" }
■ 단계 2: 변환 (Transform)
수집된 모델 파일을 기반으로 실제 평가(Evaluation)를 수행하고 결과를 포맷팅하는 단계입니다.

순서 3: Kafka Consumer 1 (Evaluation Node)

구독: model_submission 토픽 구독

처리:

전달받은 파일 다운로드

ultralytics 라이브러리로 추론(Inference) 진행

평가 결과(Metrics)를 로컬 파일로 기록

변환 데이터:

JSON

{ 
  "id": "1234-4567-123123123", 
  "file": "test.pt", 
  "f1": 90.4, 
  "precision": 93.2, 
  "recall": 88.3 
}
순서 4: Kafka Producer 2

Evaluation Node의 NiFi가 결과 파일이 생성된 디렉토리를 감지하여 클라우드 서버로 전송

■ 단계 3: 적재 (Load)
최종 평가 결과를 영구 저장소에 저장하는 단계입니다.

순서 5: Kafka Consumer 2 (Cloud Server)

클라우드 서버에서 eval_result 토픽 구독

평가 결과를 파싱하여 MariaDB에 적재 (INSERT)

■ (Optional) 결과 서빙 (Serving)
웹 서버에서 QueryID를 받아 MariaDB를 조회, 해당 ID의 평가 결과를 사용자에게 반환

5. 소감 및 회고
안정성 검증: 실제 파이프라인을 구축한 뒤, Evaluation Node 등 중간 노드를 강제로 종료해보는 테스트를 진행했습니다. Kafka의 버퍼링 덕분에 노드가 다시 켜졌을 때 데이터 유실 없이 처리가 재개되는 것을 확인하며 메시지 큐 기반 아키텍처의 강점을 체감했습니다.

향후 발전: 이번 프로젝트는 기존에 단일 서버에서 처리하던 방식의 한계를 극복하는 과정이었습니다. 이 경험을 바탕으로 추후 대용량 데이터 처리나 더 복잡한 전처리 로직이 필요한 ETL 파이프라인도 능숙하게 설계할 수 있을 것 같습니다.