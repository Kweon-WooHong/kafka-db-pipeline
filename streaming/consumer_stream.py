"""
[Streaming Consumer]
Kafka Topic (emp.src.stream)  ->  Target: Oracle Express (Docker)

동작 방식
- Kafka 로부터 메시지를 짧은 timeout으로 지속 poll
- 메시지 수신 즉시 건별로 MERGE(Upsert) 반영 + commit
  (배치 컨슈머와 달리 지연 없이 바로 반영하는 것이 핵심)
- Ctrl+C 로 종료할 때까지 상시 구동

사용법
    python consumer_stream.py
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from confluent_kafka import Consumer, KafkaError
from config.db_config import ORACLE_TGT_CONFIG, KAFKA_BOOTSTRAP_SERVERS, TOPIC_STREAM

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "emp-stream-consumer-group",
    "auto.offset.reset": "latest",   # 최초 구동 시점 "이후" 변경분부터 소비 (과거분은 배치가 처리한다고 가정)
    "enable.auto.commit": False,
}

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
    """TODO: 컬럼 매핑 교체"""
    return {
        "emp_id": data["EMP_ID"],
        "emp_name": data["EMP_NAME"],
        "dept_cd": data["DEPT_CD"],
        "salary": data["SALARY"],
        "hire_date": data["HIRE_DATE"][:10] if data.get("HIRE_DATE") else "1900-01-01",
        "upd_dt": data["UPD_DT"],
    }


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_STREAM])

    conn = oracledb.connect(**ORACLE_TGT_CONFIG)
    cursor = conn.cursor()

    print("[INFO] 실시간 스트리밍 Consumer 시작. Ctrl+C 로 종료.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # 배치와 달리 메시지 없어도 계속 대기 (상시 구동)

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ERROR] {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))

            cursor.execute(MERGE_SQL, msg_to_bind_params(data))
            conn.commit()
            consumer.commit(message=msg, asynchronous=False)

            print(f"[INFO] EMP_ID={data['EMP_ID']} 실시간 반영 완료 (UPD_DT={data['UPD_DT']})")

    except KeyboardInterrupt:
        print("\n[INFO] 사용자 요청으로 종료합니다.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
