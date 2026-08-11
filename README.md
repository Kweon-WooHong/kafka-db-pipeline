--------------------------------------------------------------------------------
#### DB-to-DB Data Pipeline 구축 - 질의문 ######################################
--------------------------------------------------------------------------------

# 개요
Local PC에 Kafka를 이용해  DB-to-DB Data Pipeline 구축을 시현을 통해서 학습하려고 하는데
Data Pipeline 환경을 구성하기 위해 필요한 작업이나 프로그램을
예시를 들어서 따라할 수 있도록 상세히 설명해줘.

# 환경
  ## PC : Notebook 1대
  ## 설치된 관련 S/W
    - Docker
      + Oracle Express
      + kafka (apache/kafka)
    - Oracle 11G
    - Toad for Oracle 10.6
    - Python 3.12 (Confluent-Kafka 설치됨)
    - VSCode

# 구축 아키텍처 관련 요구사항
  - Source DB : Oracle 11G
  - Target DB : Oracle Express(Docker Containter)
  - Ingestion
    + kafka(Docker Containter)를 이용한 Data 입수/적재 구현
    + Producer / Consumer 는 python을 이용하여 구현함
    + Producer / Consumer 는 batch와 real-time streaming 두가지의 경우 모두에 대해 구현할 것

# 출력 요구사항
  - python 소스는 db connection 설정과 Query 만 대체하면 바로 사용가능한 수준으로 작성해 줄 것
  - 각 단계의 작업을 따라할 수 있도록 단계별로 상세히 설명해 줄 것




----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------
#### DB-to-DB Data Pipeline 구축 - 답변 ########################################
--------------------------------------------------------------------------------

# Kafka 기반 DB-to-DB Data Pipeline 실습 가이드

Oracle 11G(Source) → Kafka → Oracle Express(Target, Docker) 로 데이터를
Batch / Real-time Streaming 두 가지 방식으로 이관하는 실습 프로젝트입니다.

## 0. 아키텍처 개요

```
[Oracle 11G]                [Kafka (Docker)]              [Oracle XE (Docker)]
  EMP_SRC   --Producer-->  emp.src.full  (배치)  --Consumer-->   EMP_TGT
            --Producer-->  emp.src.stream(실시간)--Consumer-->   EMP_TGT
```

- **배치(Batch)**: 전체 테이블을 한 번에 추출 → Kafka 적재 → 컨슈머가 일괄
  MERGE 처리 후 프로세스 종료. 초기 적재(Initial Load)에 적합합니다.
- **실시간(Streaming)**: `UPD_DT` 컬럼을 기준으로 수 초 간격 증분 폴링 →
  변경분만 Kafka로 전송 → 컨슈머가 즉시 반영. Oracle 11G는 로그 기반 CDC
  (GoldenGate 등) 없이는 진짜 실시간 캡처가 어려우므로, 여기서는
  **타임스탬프 기반 준실시간(Near Real-time) 폴링 방식**으로 CDC를
  시뮬레이션합니다.

프로젝트 구조:
```
kafka-db-pipeline/
├── requirements.txt
├── sql/
│   ├── 01_source_ddl.sql     # Oracle 11G 소스 테이블 DDL
│   └── 02_target_ddl.sql     # Oracle XE 타겟 테이블 DDL
├── config/
│   └── db_config.py          # DB/Kafka 접속 정보 (여기만 고치면 됨)
├── kafka/
│   └── create_topics.sh      # 토픽 생성 스크립트
├── batch/
│   ├── producer_batch.py
│   └── consumer_batch.py
└── streaming/
    ├── producer_stream.py
    └── consumer_stream.py
```

---

## 1. 사전 준비

### 1-1. Docker 컨테이너 확인
Oracle Express, Kafka 컨테이너가 이미 설치되어 있다는 전제이므로 기동 상태만 확인합니다.

```bash
docker ps
```

컨테이너명(예: `kafka`, `oracle-xe`)과 포트 매핑을 확인해 두세요.
`kafka/create_topics.sh` 와 `config/db_config.py` 의 값을 여기서 확인한
실제 이름/포트로 맞춰야 합니다.

```bash
# 포트 매핑 확인 예시
docker port kafka  # kafka 컨테이너 변경 kafka-server
docker port oracle-xe
```
9092/tcp -> 0.0.0.0:9092
9092/tcp -> [::]:9092
1521/tcp -> 0.0.0.0:1522
1521/tcp -> [::]:1522

### 1-2. Python 가상환경 및 패키지 설치

```bash
cd kafka-db-pipeline
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

Confluent-Kafka는 이미 설치되어 있다고 하셨지만, `oracledb` 패키지는
새로 설치가 필요합니다. (`cx_Oracle`을 이미 쓰고 계셨다면 그대로 사용하셔도
무방하며, 이 경우 코드의 `oracledb.connect(...)` 부분만 `cx_Oracle.connect(...)`
로 바꿔주면 됩니다.)

### 1-3. ⚠️ Oracle 11G 접속 관련 중요 사항

`python-oracledb`는 기본적으로 **Thin 모드**로 동작하며, Thin 모드는
**Oracle Database 12.1 이상**만 지원합니다. Source가 **Oracle 11G**이므로
반드시 **Thick 모드**로 전환해야 하고, 이를 위해 로컬 PC에
**Oracle Instant Client**가 설치되어 있어야 합니다.

- Toad for Oracle이 이미 설치되어 있다면 Oracle Client 라이브러리가
  같이 설치되어 있을 가능성이 높습니다. `ORACLE_HOME` 환경변수나 Toad
  설치 경로 하위에서 `oci.dll` (Windows) 위치를 찾아 그 경로를
  `config/db_config.py` 의 `ORACLE_CLIENT_LIB_DIR` 에 지정하세요.
- 없다면 [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client/downloads.html)
  를 별도로 받아 압축을 풀기만 하면 됩니다(설치 프로그램 불필요).

반면 Target인 **Oracle XE(Docker, 최신 버전)**는 Thin 모드로 바로
접속 가능하므로 별도 설정이 필요 없습니다.

---

## 2. Kafka 토픽 생성

`kafka/create_topics.sh` 안의 `CONTAINER_NAME` 을 본인 환경의 컨테이너
이름으로 수정한 뒤 실행합니다.

```bash
bash kafka/create_topics.sh
```

정상 생성 시 아래처럼 토픽 목록이 출력됩니다.
```
emp.src.full
emp.src.stream
```

WSL이나 Git Bash가 없다면 명령어를 하나씩 직접 실행해도 됩니다.
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server localhost:9092 \
  --topic emp.src.full --partitions 3 --replication-factor 1
```

---

## 3. Oracle 테이블 생성

Toad for Oracle로 각 DB에 접속하여 아래 순서로 실행합니다.

1. **Source(11G)** 접속 → `sql/01_source_ddl.sql` 실행
   - `EMP_SRC` 테이블 생성
   - `UPD_DT` 자동 갱신 트리거 생성 (실시간 파이프라인의 핵심 - 이 컬럼으로
     변경분을 감지합니다)
   - 테스트용 샘플 데이터 3건 INSERT
2. **Target(Oracle XE)** 접속 → `sql/02_target_ddl.sql` 실행
   - `EMP_TGT` 테이블 생성 (처음엔 비어 있는 상태)

---

## 4. `config/db_config.py` 값 채우기

이 파일 하나만 수정하면 모든 스크립트에 반영됩니다.

```python
ORACLE_SRC_CONFIG = {
    "user": "SRC_USER",
    "password": "SRC_PASSWORD",
    "dsn": "localhost:1521/ORCL",        # 실제 host:port/service_name
}
ORACLE_CLIENT_LIB_DIR = r"C:\oracle\instantclient_19_20"   # 실제 경로

ORACLE_TGT_CONFIG = {
    "user": "TGT_USER",
    "password": "TGT_PASSWORD",
    "dsn": "localhost:1522/XEPDB1",      # 실제 Docker 포트/서비스명
}

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"   # 실제 Docker 포트
```

---

## 5. 배치(Batch) 파이프라인 실행

터미널 2개(Producer용, Consumer용)를 준비합니다.

**터미널 A - Producer 실행 (Source → Kafka)**
```bash
cd kafka-db-pipeline
python batch/producer_batch.py
```
출력 예시:
```
[INFO] Oracle Thick 모드 초기화 완료
[INFO] 3 건 전송 중...
[INFO] 배치 전송 완료. 총 3 건 (topic=emp.src.full)
```

**터미널 B - Consumer 실행 (Kafka → Target)**
```bash
cd kafka-db-pipeline
python batch/consumer_batch.py
```
출력 예시:
```
[INFO] 배치 적재 완료. 총 3 건 (target=EMP_TGT)
```
(신규 메시지가 10초간 없으면 자동 종료되도록 되어 있습니다 - `IDLE_TIMEOUT_SEC`)

**검증**: Toad로 Target DB의 `EMP_TGT` 테이블을 조회해 3건이 정상적으로
적재되었는지 확인합니다.

```sql
SELECT * FROM EMP_TGT ORDER BY EMP_ID;
```

---

## 6. 실시간(Streaming) 파이프라인 실행

터미널 2개를 준비합니다. 이번에는 두 프로세스가 **동시에, 계속 떠 있는 상태**로
동작합니다 (Ctrl+C 전까지 종료되지 않음).

**터미널 A - Streaming Consumer 먼저 기동 (Kafka → Target 대기)**
```bash
python streaming/consumer_stream.py
```
```
[INFO] 실시간 스트리밍 Consumer 시작. Ctrl+C 로 종료.
```

**터미널 B - Streaming Producer 기동 (Source 변경분 폴링 시작)**
```bash
python streaming/producer_stream.py
```
```
[INFO] 스트리밍 Producer 시작 (체크포인트: 1900-01-01 00:00:00.000000, 폴링주기: 3s)
```
최초 실행 시 체크포인트가 없으므로 기존 데이터 전체(3건)를 한 번 전송합니다.

**터미널 C - 실시간 변경 발생시키기 (Toad 또는 SQL*Plus)**

Source DB에 접속해 새 데이터를 넣거나 기존 데이터를 수정해봅니다.
```sql
INSERT INTO EMP_SRC (EMP_ID, EMP_NAME, DEPT_CD, SALARY, HIRE_DATE)
VALUES (1004, '최지훈', 'IT02', 5000000, SYSDATE);
COMMIT;

UPDATE EMP_SRC SET SALARY = SALARY * 1.1 WHERE EMP_ID = 1001;
COMMIT;
```

**결과 확인**: 3초(폴링 주기) 이내에 터미널 A(Consumer)에 아래와 같은
로그가 실시간으로 찍히는 것을 확인할 수 있습니다.
```
[INFO] EMP_ID=1004 실시간 반영 완료 (UPD_DT=2026-07-21T14:32:10.123456)
[INFO] EMP_ID=1001 실시간 반영 완료 (UPD_DT=2026-07-21T14:32:25.987654)
```

Toad로 `EMP_TGT` 를 다시 조회하면 변경 사항이 즉시 반영된 것을 볼 수 있습니다.

---

## 7. 운영/트러블슈팅 팁

| 증상 | 원인 / 해결 |
|---|---|
| `DPI-1047: Cannot locate a 64-bit Oracle Client library` | Instant Client 경로가 잘못됨. `ORACLE_CLIENT_LIB_DIR` 확인, 32/64bit 아키텍처 일치 여부 확인 |
| `ORA-12545` / 접속 안 됨 | Docker 컨테이너 포트 매핑(`docker port`) 재확인, 방화벽 확인 |
| Kafka 토픽 접속 오류 | `KAFKA_BOOTSTRAP_SERVERS` 포트가 Docker 매핑 포트와 일치하는지 확인 (컨테이너 내부 9092와 호스트 매핑 포트가 다를 수 있음) |
| 스트리밍 Producer가 같은 데이터를 계속 재전송 | `checkpoint_emp_stream.txt` 삭제 후 재기동하면 처음부터 다시 읽음. 정상 운영 중이라면 이 파일을 삭제하지 말 것 |
| 컨슈머 재기동 시 메시지를 중복 처리하는지 걱정됨 | 현재 구현은 **At-least-once**(최소 한 번 이상 처리) 방식입니다. `MERGE(Upsert)` 구조라 중복 수신되어도 최종 결과는 동일(멱등성 보장)하므로 안전합니다 |
| 대용량 초기 적재 시 배치가 느림 | `cursor.arraysize`/`fetchmany` 크기, Kafka `producer.poll(0)` 호출 주기, Consumer의 `BATCH_SIZE` 를 늘려 튜닝 |

---

## 8. 다음 단계로 확장하고 싶다면

- **Kafka Connect (JDBC Source/Sink Connector)**: Python 코드 없이도
  선언적 설정만으로 동일한 파이프라인을 구성할 수 있습니다. 다만 Oracle 11G
  JDBC 폴링 및 Sink Upsert 지원 여부는 커넥터 종류별로 확인이 필요합니다.
- **스키마 레지스트리 + Avro**: JSON 대신 Avro/Protobuf + Schema Registry를
  적용하면 스키마 변경 관리와 메시지 크기 최적화에 유리합니다. (특히 178개
  테이블 규모의 RDM 프로젝트처럼 스키마가 많을 때 유용)
  - 참고 문서: https://docs.confluent.io/platform/current/schema-registry/index.html
- **Debezium (LogMiner 기반 CDC)**: Oracle 11G 자체 로그를 활용한 진짜
  실시간 CDC가 필요하다면 Debezium Oracle Connector(LogMiner) 도입을
  검토할 수 있습니다. 다만 라이선스/버전 요구사항이 있어 별도 검증이 필요합니다.
