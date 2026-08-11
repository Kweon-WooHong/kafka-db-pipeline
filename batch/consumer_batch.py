"""
[Batch Consumer]
Kafka Topic (emp.src.full)  ->  Target: Oracle Express (Docker)

동작 방식
- Kafka 로부터 메시지를 지속적으로 poll
- BATCH_SIZE 만큼 모이면 MERGE(Upsert)로 일괄 반영 + offset commit
- IDLE_TIMEOUT_SEC 동안 신규 메시지가 없으면 배치 종료로 간주하고 프로세스 종료
  (실시간 상시 구동이 아니라 "배치 잡" 성격이므로 종료 조건을 둠)

사용법
    python consumer_batch.py
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from confluent_kafka import Consumer, KafkaError
from config.db_config import ORACLE_TGT_CONFIG, KAFKA_BOOTSTRAP_SERVERS, TOPIC_FULL

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "emp-batch-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,   # DB 반영 성공 후 수동 commit (At-least-once 보장)
}

BATCH_SIZE = 500
IDLE_TIMEOUT_SEC = 10  # 이 시간 동안 신규 메시지가 없으면 배치 종료

# ============ [교체 대상: 타겟 반영 MERGE 문] ============
MERGE_SQL = """
MERGE INTO EMP_TGT t
USING (SELECT :emp_id AS EMP_ID FROM dual) s
ON (t.EMP_ID = s.EMP_ID)
WHEN MATCHED THEN UPDATE SET
    t.EMP_NAME   = :emp_name,
    t.DEPT_CD    = :dept_cd,
    t.SALARY     = :salary,
    t.HIRE_DATE  = TO_DATE(:hire_date, 'YYYY-MM-DD'),
    t.SRC_UPD_DT = TO_TIMESTAMP(:upd_dt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'),
    t.LOADED_DT  = SYSDATE
WHEN NOT MATCHED THEN 
  INSERT
    (EMP_ID, EMP_NAME, DEPT_CD, SALARY, HIRE_DATE, SRC_UPD_DT, LOADED_DT)
  VALUES
    (:emp_id, :emp_name, :dept_cd, :salary,
     TO_DATE(:hire_date, 'YYYY-MM-DD'),
     TO_TIMESTAMP(:upd_dt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'),
     SYSDATE)
"""
# ==========================================================


def msg_to_bind_params(data: dict) -> dict:
    """Kafka 메시지(JSON dict) -> 오라클 바인드 변수 매핑. TODO: 컬럼 매핑 교체"""
    return {
        "emp_id": data["EMP_ID"],
        "emp_name": data["EMP_NAME"],
        "dept_cd": data["DEPT_CD"],
        "salary": data["SALARY"],
        "hire_date": data["HIRE_DATE"][:10] if data.get("HIRE_DATE") else "1900-01-01",
        "upd_dt": data["UPD_DT"],
    }


def flush_buffer(cursor, conn, buffer):
    for data in buffer:
        cursor.execute(MERGE_SQL, msg_to_bind_params(data))
    conn.commit()


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_FULL])

    conn = oracledb.connect(**ORACLE_TGT_CONFIG)  # Oracle XE는 Thin 모드로 바로 접속 가능
    cursor = conn.cursor()

    buffer = []
    processed = 0

    try:
        while True:
            msg = consumer.poll(timeout=IDLE_TIMEOUT_SEC)

            if msg is None:
                print("[INFO] 신규 메시지 없음 -> 배치 종료")
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ERROR] {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            buffer.append(data)

            if len(buffer) >= BATCH_SIZE:
                flush_buffer(cursor, conn, buffer)
                processed += len(buffer)
                consumer.commit(asynchronous=False)
                print(f"[INFO] {processed} 건 타겟 반영...")
                buffer.clear()

        if buffer:
            flush_buffer(cursor, conn, buffer)
            processed += len(buffer)
            consumer.commit(asynchronous=False)

    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print(f"[INFO] 배치 적재 완료. 총 {processed} 건 (target=EMP_TGT)")


if __name__ == "__main__":
    main()
