"""
[Streaming Producer]
Source: Oracle 11G  ->  Kafka Topic (emp.src.stream)

동작 방식
- Oracle 11G는 별도 CDC 툴(GoldenGate 등) 없이는 로그 기반 실시간 캡처가
  어려우므로, 여기서는 "타임스탬프(UPD_DT) 증분 폴링" 방식으로 준실시간(Near
  Real-time) 스트리밍을 구현합니다. (수 초 간격 폴링)
- 마지막으로 처리한 시점(체크포인트)을 파일에 저장해두고, 다음 실행 시
  그 이후 변경분만 조회합니다.
- Ctrl+C 로 종료할 때까지 무한 루프로 동작합니다. (상시 구동 프로세스)

사전 조건
- EMP_SRC 테이블에 UPD_DT 컬럼과 INSERT/UPDATE 트리거가 설정되어 있어야 함
  (sql/01_source_ddl.sql 참고)

사용법
    python producer_stream.py
"""

import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import oracledb
from confluent_kafka import Producer
from config.db_config import ORACLE_SRC_CONFIG, ORACLE_CLIENT_LIB_DIR, KAFKA_BOOTSTRAP_SERVERS, TOPIC_STREAM

POLL_INTERVAL_SEC = 3          # 폴링 주기 (짧을수록 실시간성 ↑, DB 부하 ↑)
CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "checkpoint_emp_stream.txt")

# ============ [교체 대상: 증분 조회 쿼리] ============
# UPD_DT > 마지막 체크포인트 조건으로 변경분만 조회
INCREMENTAL_QUERY = """
    SELECT EMP_ID, EMP_NAME, DEPT_CD, SALARY, HIRE_DATE, UPD_DT
    FROM EMP_SRC
    WHERE UPD_DT > TO_TIMESTAMP(:last_ts, 'YYYY-MM-DD HH24:MI:SS.FF')
    ORDER BY UPD_DT
"""
# ======================================================


def init_oracle_thick():
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        print("[INFO] Oracle Thick 모드 초기화 완료")
    except oracledb.ProgrammingError as e:
        print(f"[WARN] Thick 모드 초기화 스킵: {e}")


def load_checkpoint() -> str:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            saved = f.read().strip()
            if saved:
                return saved
    return "1900-01-01 00:00:00.000000"  # 최초 실행 시 전체 데이터부터 시작


def save_checkpoint(ts: str):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(ts)


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

    last_ts = load_checkpoint()
    print(f"[INFO] 스트리밍 Producer 시작 (체크포인트: {last_ts}, 폴링주기: {POLL_INTERVAL_SEC}s)")
    print("[INFO] 종료하려면 Ctrl+C 를 누르세요.")

    try:
        while True:
            cursor.execute(INCREMENTAL_QUERY, {"last_ts": last_ts})
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

            if rows:
                max_ts = None
                upd_dt_idx = columns.index("UPD_DT")

                for row in rows:
                    key_val = str(row[columns.index("EMP_ID")])  # TODO: PK 컬럼명 교체
                    producer.produce(
                        topic=TOPIC_STREAM,
                        key=key_val,
                        value=row_to_json(columns, row),
                    )
                    row_upd_dt = row[upd_dt_idx]
                    if max_ts is None or row_upd_dt > max_ts:
                        max_ts = row_upd_dt

                producer.flush()

                last_ts = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
                save_checkpoint(last_ts)
                print(f"[INFO] {len(rows)}건 전송 -> 체크포인트 갱신: {last_ts}")

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n[INFO] 사용자 요청으로 종료합니다.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
