import oracledb

"""
DB 접속 정보 공통 설정
- 실제 사용 시 아래 값들을 본인 환경에 맞게 교체하세요.
- 운영 반영 시에는 평문 대신 환경변수(os.environ) 또는 Vault 등으로 대체 권장.

[중요] python-oracledb는 Thin/Thick 모드를 "커넥션 단위"가 아니라
"프로세스 단위"로 결정합니다. 즉 한 프로그램 안에서 어떤 DB는 Thin,
어떤 DB는 Thick 으로 섞어 쓸 수 없습니다.
Source(11G)가 Thick 모드를 요구하므로, Target(XE)까지 포함해 프로세스
전체를 Thick 모드로 통일합니다. Thick 모드(Oracle Instant Client)는
구버전(11G)/신버전(XE) 서버 모두에 문제없이 접속되므로 안전합니다.

=> 반드시 최초의 connect() 호출보다 "먼저" init_oracle_client()가
   실행되어야 합니다. (한 번이라도 Thin 모드로 connect()가 먼저
   일어나면 이후 Thick 모드 전환 시 DPY-2019 오류가 발생합니다.)
"""

# -----------------------------------------------------------------
# Source: Oracle 11G
# -----------------------------------------------------------------
ORACLE_SRC_CONFIG = {
    "user": "LRM",                     # TODO: 소스 계정
    "password": "LRM",                 # TODO: 소스 비밀번호
    "dsn": "localhost:1521/ORCL11G",   # TODO: host:port/service_name
}

# Oracle Instant Client 경로 (Thick 모드 초기화용 - Source/Target 공통 사용)
ORACLE_CLIENT_LIB_DIR = r"C:\app\instantclient_19_30"   # TODO: 본인 PC의 Instant Client 경로

# -----------------------------------------------------------------
# Target: Oracle Express (Docker Container)
# -----------------------------------------------------------------
ORACLE_TGT_CONFIG = {
    "user": "LRM",                     # TODO: 타겟 계정
    "password": "LRM",                 # TODO: 타겟 비밀번호
    "dsn": "localhost:1522/XEPDB1",    # TODO: Docker 포트 매핑 및 서비스명
}

_thick_initialized = False


def init_oracle_thick():
    """
    Thick 모드 초기화. 프로세스 전체에서 딱 한 번만, 그리고
    반드시 어떤 connect() 호출보다 먼저 실행되어야 합니다.
    """
    global _thick_initialized
    if _thick_initialized:
        return
    try:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        _thick_initialized = True
        print("[INFO] Oracle Thick 모드 초기화 완료")
    except oracledb.ProgrammingError as e:
        # 이미 다른 경로로 초기화된 경우 등은 무시하고 진행
        print(f"[WARN] Thick 모드 초기화 스킵: {e}")
        _thick_initialized = True


def connect_source_db():
    conn = oracledb.connect(**ORACLE_SRC_CONFIG)
    print("Source DB 연결 성공 -> Oracle 11G, Python 3.12")
    conn.close()


def connect_target_db():
    conn = oracledb.connect(**ORACLE_TGT_CONFIG)
    print("Target DB 연결 성공 -> Oracle XE, Python 3.12")
    conn.close()


def main():
    # 어떤 connect()보다도 반드시 가장 먼저 호출 (순서 고정)
    init_oracle_thick()

    # 이후로는 Source/Target 순서는 상관없음 (둘 다 Thick 모드로 접속됨)
    connect_source_db()
    connect_target_db()

    print("[Thick Mode] DB Connection Test 완료 -> Python 3.12")


if __name__ == "__main__":
    main()
