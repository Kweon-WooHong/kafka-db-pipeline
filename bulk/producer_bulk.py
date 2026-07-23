"""
[Bulk Producer] - 대용량 처리 최적화판
Source: Oracle 11G  ->  Kafka Topic (emp.src.full)

producer_batch.py 대비 개선 사항
1) Kafka Producer 배치 전송 튜닝
   - linger.ms / batch.size / batch.num.messages : 짧게 대기하며 메시지를 모아
     한 번의 네트워크 요청으로 전송 (기본값은 지연 없이 바로바로 보내 처리량이 낮음)
   - compression.type : 네트워크 대역폭 절감 (대량 전송 시 효과 큼)
   - queue.buffering.max.* : 로컬 큐 용량 확대 (대량 produce 시 BufferError 방지)
2) Local Queue Full(BufferError) 발생 시 poll()로 콜백을 비워 백프레셔 처리
   (기존 코드는 이 예외를 처리하지 않아, 대량 데이터에서는 예외로 프로세스가
   중단될 수 있었습니다)
3) Oracle Fetch 성능 향상을 위해 arraysize 와 prefetchrows 를 함께 설정
   (Oracle 권장: prefetchrows = arraysize + 1)

사용법
    python bulk/producer_bulk.py
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from confluent_kafka import Producer
from config.db_config import ORACLE_SRC_CONFIG, ORACLE_CLIENT_LIB_DIR, KAFKA_BOOTSTRAP_SERVERS, TOPIC_FULL

# ============ [교체 대상: 추출 대상 쿼리] ============
SOURCE_QUERY = """
    SELECT EMP_ID, EMP_NAME, DEPT_CD, SALARY, HIRE_DATE, UPD_DT
    FROM EMP_SRC
    ORDER BY EMP_ID
"""
# ======================================================

FETCH_SIZE = 5000  # 대용량 처리를 고려해 producer_batch.py(1000)보다 확대

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "1",                        # 처리량/안정성 균형 (더 엄격하게 하려면 "all")
    "linger.ms": 20,                    # 20ms 대기하며 메시지를 모아서 전송 (배치 효율 ↑)
    "batch.size": 262144,               # 배치 1개당 최대 256KB (기본 16KB 대비 확대)
    "batch.num.messages": 10000,        # 배치 1개당 최대 메시지 건수
    "compression.type": "lz4",          # 압축 전송으로 네트워크 대역폭 절감
    "queue.buffering.max.messages": 500000,
    "queue.buffering.max.kbytes": 1048576,  # 1GB
}


def init_oracle_thick():
    """Oracle 11G 접속을 위해 Thick 모드로 초기화. (반드시 connect()보다 먼저 호출)"""
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        print("[INFO] Oracle Thick 모드 초기화 완료")
    except oracledb.ProgrammingError as e:
        print(f"[WARN] Thick 모드 초기화 스킵: {e}")


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] 메시지 전송 실패: {err}")


def row_to_json(columns, row) -> str:
    data = {}
    for col, val in zip(columns, row):
        if hasattr(val, "isoformat"):
            val = val.isoformat()
        data[col] = val
    return json.dumps(data, ensure_ascii=False)


def produce_with_backpressure(producer: Producer, **kwargs):
    """
    로컬 프로듀서 큐가 가득 차면 BufferError가 발생합니다.
    이 경우 즉시 실패시키지 않고, poll()로 콜백을 처리해 큐를 비운 뒤 재시도합니다.
    (대량 전송 시 필수적인 백프레셔 처리)
    """
    while True:
        try:
            producer.produce(**kwargs)
            return
        except BufferError:
            producer.poll(0.1)  # 큐에 여유가 생길 때까지 짧게 대기하며 콜백 처리


def main():
    init_oracle_thick()

    producer = Producer(PRODUCER_CONFIG)

    conn = oracledb.connect(**ORACLE_SRC_CONFIG)
    cursor = conn.cursor()
    cursor.arraysize = FETCH_SIZE
    cursor.prefetchrows = FETCH_SIZE + 1  # Oracle 권장값: arraysize + 1

    cursor.execute(SOURCE_QUERY)
    columns = [d[0] for d in cursor.description]

    total = 0
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break

        for row in rows:
            key_val = str(row[columns.index("EMP_ID")])  # TODO: PK 컬럼명에 맞게 교체
            produce_with_backpressure(
                producer,
                topic=TOPIC_FULL,
                key=key_val,
                value=row_to_json(columns, row),
                callback=delivery_report,
            )
            total += 1

        producer.poll(0)  # 콜백(delivery report) 처리
        print(f"[INFO] {total} 건 전송 중...")

    producer.flush()  # 남아있는 모든 메시지가 전송될 때까지 대기
    cursor.close()
    conn.close()
    print(f"[INFO] Bulk 전송 완료. 총 {total} 건 (topic={TOPIC_FULL})")


if __name__ == "__main__":
    main()
