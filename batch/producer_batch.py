"""
[Batch Producer]
Source: Oracle 11G  ->  Kafka Topic (emp.src.full)

동작 방식
- SOURCE_QUERY 로 대상 데이터를 전체(또는 조건부) 추출
- fetchmany() 로 페치 사이즈 단위로 나눠 읽으며 Kafka 로 전송
- 실행이 끝나면 프로세스 종료 (1회성 배치)

사용법
    python producer_batch.py
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

FETCH_SIZE = 1000  # 오라클 fetch 단위 (네트워크 왕복 최소화)


def init_oracle_thick():
    """Oracle 11G 접속을 위해 Thick 모드로 초기화. (프로세스당 1회만 호출)"""
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        print("[INFO] Oracle Thick 모드 초기화 완료")
    except oracledb.ProgrammingError as e:
        # 이미 초기화된 경우 등은 무시
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


def main():
    init_oracle_thick()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    conn = oracledb.connect(**ORACLE_SRC_CONFIG)
    cursor = conn.cursor()
    cursor.arraysize = FETCH_SIZE

    cursor.execute(SOURCE_QUERY)
    columns = [d[0] for d in cursor.description]

    total = 0
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break

        for row in rows:
            key_col_value = str(row[columns.index("EMP_ID")])  # TODO: PK 컬럼명에 맞게 교체
            producer.produce(
                topic=TOPIC_FULL,
                key=key_col_value,
                value=row_to_json(columns, row),
                callback=delivery_report,
            )
            total += 1

        producer.poll(0)  # 콜백(delivery report) 처리
        print(f"[INFO] {total} 건 전송 중...")

    producer.flush()
    cursor.close()
    conn.close()
    print(f"[INFO] 배치 전송 완료. 총 {total} 건 (topic={TOPIC_FULL})")


if __name__ == "__main__":
    main()
