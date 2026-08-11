#!/bin/bash
# =========================================================
# Kafka 토픽 생성 스크립트
# 사전 조건: apache/kafka Docker 컨테이너가 기동 중이어야 함
#            (컨테이너명은 본인 환경에 맞게 CONTAINER_NAME 수정)
# =========================================================

CONTAINER_NAME=kafka    # docker ps 로 확인한 실제 컨테이너 이름으로 교체

# 배치용 토픽 (Full Extract 결과 적재)
docker exec -it ${CONTAINER_NAME} /opt/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --topic emp.src.full \
    --partitions 3 \
    --replication-factor 1

# 실시간 스트리밍용 토픽 (증분 변경분 적재)
docker exec -it ${CONTAINER_NAME} /opt/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --topic emp.src.stream \
    --partitions 3 \
    --replication-factor 1

# 생성 결과 확인
docker exec -it ${CONTAINER_NAME} /opt/kafka/bin/kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092
