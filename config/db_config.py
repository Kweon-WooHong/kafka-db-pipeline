"""
DB 접속 정보 공통 설정
- 실제 사용 시 아래 값들을 본인 환경에 맞게 교체하세요.
- 운영 반영 시에는 평문 대신 환경변수(os.environ) 또는 Vault 등으로 대체 권장.
"""

# -----------------------------------------------------------------
# Source: Oracle 11G
# 주의) Oracle 11G는 python-oracledb의 Thin 모드(기본)를 지원하지 않습니다.
#       반드시 Thick 모드로 전환해야 하며, 이를 위해 Oracle Instant Client가
#       로컬 PC에 설치되어 있어야 합니다. (Toad를 쓰고 있다면 이미 Oracle
#       Client가 설치되어 있을 가능성이 높습니다 - ORACLE_HOME 경로 확인)
# -----------------------------------------------------------------
ORACLE_SRC_CONFIG = {
    "user": "LRM",                     # TODO: 소스 계정
    "password": "LRM",                 # TODO: 소스 비밀번호
    "dsn": "localhost:1521/ORCL11G",   # TODO: host:port/service_name
}

# Oracle 11G 접속을 위한 Instant Client 경로 (Thick 모드 초기화용)
ORACLE_CLIENT_LIB_DIR = r"C:\app\instantclient_19_30"   # TODO: 본인 PC의 Instant Client 경로


# -----------------------------------------------------------------
# Target: Oracle Express (Docker Container)
# 최신 Oracle XE(Docker 이미지 gvenzl/oracle-xe 등)는 Thin 모드로 접속 가능
# (Instant Client 불필요)
# -----------------------------------------------------------------
ORACLE_TGT_CONFIG = {
    "user": "LRM",                     # TODO: 타겟 계정
    "password": "LRM",                 # TODO: 타겟 비밀번호
    "dsn": "localhost:1522/XEPDB1",    # TODO: Docker 포트 매핑 및 서비스명
}


# -----------------------------------------------------------------
# Kafka 공통 설정
# -----------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"   # TODO: Docker 포트 매핑 확인

TOPIC_FULL = "emp.src.full"      # 배치(Full Extract) 토픽
TOPIC_STREAM = "emp.src.stream"  # 실시간 스트리밍 토픽
