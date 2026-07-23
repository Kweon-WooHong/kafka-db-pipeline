"""
[Bulk Consumer] - 대용량 처리 최적화판
Kafka Topic (emp.src.full)  ->  Target: Oracle Express (Docker)

consumer_batch.py 대비 개선 사항 (요건 4가지 전부 반영)

1) Consumer Polling: poll() 대신 consume(num_messages=1000) 사용
   - 1회 호출로 최대 1000건을 한 번에 버퍼로 가져와 polling 오버헤드를 최소화

2) Bulk DB Insert: executemany() 사용
   - 기존(consumer_batch.py)은 버퍼링은 배치 단위였지만 실제 DB 호출은
     "for 문 안에서 cursor.execute() 건별 호출" 이었습니다. 이는 500건이면
     500번 Network Round Trip이 발생하는 구조로, 진짜 Bulk가 아니었습니다.
   - 개선본은 cursor.executemany()로 최대 1000건을 "단 1번의 호출"로
     DB에 전송합니다. Python <-> DB 간 Round Trip을 N번에서 1번으로 줄여
     대량 처리 시 성능이 크게 향상됩니다.

3) MERGE INTO + Array Bind (batcherrors)
   - executemany에 MERGE INTO 문을 그대로 사용해 UPSERT를 Array Bind로 처리
   - batcherrors=True 옵션으로, 배치 내 일부 row에 데이터 오류(NULL 제약 위반 등)가
     있어도 전체 배치가 롤백되지 않고, 정상 row는 반영 + 오류 row만 별도 식별/로깅됩니다.
     (건별 execute 방식보다 훨씬 견고합니다)

4) 수동 Offset 관리 (Batch Commit)
   - DB 트랜잭션(conn.commit())이 "성공적으로 완료된 시점 이후"에만
     Kafka Offset을 commit 합니다. (enable.auto.commit=False)
   - 이 순서 덕분에, DB 반영 도중 프로세스가 죽더라도 Kafka Offset은 아직
     커밋되지 않은 상태이므로, 재시작 시 동일 메시지를 다시 읽어 재처리합니다.
     MERGE(Upsert) 특성상 중복 재처리되어도 최종 결과는 동일 -> At-Least-Once
     안정성이 보장됩니다.

사용법
    python bulk/consumer_bulk.py
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
    "group.id": "emp-bulk-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,        # 수동 커밋 (DB 반영 성공 후에만 커밋)
    "max.poll.interval.ms": 300000,     # executemany 처리 시간을 감안해 여유 있게 설정
}

CONSUME_MAX_MESSAGES = 1000     # 1회 consume() 호출로 가져올 최대 메시지 건수
CONSUME_TIMEOUT_SEC = 1.0       # consume() 1회 호출당 대기 시간(초)
IDLE_ROUNDS_TO_STOP = 10        # 메시지 없는 상태가 이 횟수만큼 연속되면 배치 종료로 판단
                                 # (10 * CONSUME_TIMEOUT_SEC = 약 10초 유휴 시 종료)

# ============ [교체 대상: 타겟 반영 MERGE 문 - Array Bind 대상] ============
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
WHEN NOT MATCHED THEN INSERT
    (EMP_ID, EMP_NAME, DEPT_CD, SALARY, HIRE_DATE, SRC_UPD_DT, LOADED_DT)
VALUES
    (:emp_id, :emp_name, :dept_cd, :salary,
     TO_DATE(:hire_date, 'YYYY-MM-DD'),
     TO_TIMESTAMP(:upd_dt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'),
     SYSDATE)
"""
# ============================================================================


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


def bulk_merge(cursor, conn, records: list) -> tuple:
    """
    executemany() + MERGE INTO 로 records 전체를 단 1회의 DB 호출로 반영.
    batcherrors=True 로 일부 row 오류가 있어도 나머지는 정상 반영됩니다.
    """
    cursor.executemany(MERGE_SQL, records, batcherrors=True)

    batch_errors = cursor.getbatcherrors()
    if batch_errors:
        for err in batch_errors:
            bad_record = records[err.offset]
            print(f"[WARN] row(offset={err.offset}) 반영 실패: {err.message.strip()} "
                  f"-> EMP_ID={bad_record.get('emp_id')}")

    conn.commit()  # 오류 row를 제외한 나머지가 커밋됨

    success_count = len(records) - len(batch_errors)
    return success_count, batch_errors


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_FULL])

    conn = oracledb.connect(**ORACLE_TGT_CONFIG)
    cursor = conn.cursor()

    # executemany 시 배치 첫 row만으로 타입을 추론하다 오류가 나는 것을 방지하기 위해
    # 바인드 변수 타입을 명시적으로 지정 (Python 기본 타입으로 지정 가능)
    cursor.setinputsizes(
        emp_id=int,
        emp_name=str,
        dept_cd=str,
        salary=float,
        hire_date=str,
        upd_dt=str,
    )

    total_success = 0
    total_error = 0
    idle_rounds = 0

    try:
        while True:
            # ---- (요건 1) consume()으로 다건 메시지를 한 번에 폴링 ----
            msgs = consumer.consume(num_messages=CONSUME_MAX_MESSAGES, timeout=CONSUME_TIMEOUT_SEC)

            if not msgs:
                idle_rounds += 1
                if idle_rounds >= IDLE_ROUNDS_TO_STOP:
                    print("[INFO] 신규 메시지 없음 -> 배치 종료")
                    break
                continue
            idle_rounds = 0

            records = []
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[ERROR] Kafka 메시지 오류: {msg.error()}")
                    continue
                data = json.loads(msg.value().decode("utf-8"))
                records.append(msg_to_bind_params(data))

            if not records:
                continue

            # ---- (요건 2,3) executemany + MERGE INTO Array Bind로 1회 호출 처리 ----
            success_count, batch_errors = bulk_merge(cursor, conn, records)
            total_success += success_count
            total_error += len(batch_errors)

            # ---- (요건 4) DB 커밋이 끝난 이후에만 Kafka Offset 커밋 ----
            consumer.commit(asynchronous=False)

            print(f"[INFO] {len(records)}건 처리(성공 {success_count} / 오류 {len(batch_errors)}) "
                  f"- 누적 성공 {total_success}건, offset commit 완료")

    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print(f"[INFO] Bulk 적재 완료. 총 성공 {total_success}건 / 오류 {total_error}건 (target=EMP_TGT)")


if __name__ == "__main__":
    main()
