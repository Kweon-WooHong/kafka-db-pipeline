-- =========================================================
-- Target DB (Oracle Express - Docker Container) : EMP_TGT
-- Toad for Oracle 에서 TGT_USER 계정으로 접속 후 실행
-- =========================================================

CREATE TABLE EMP_TGT (
    EMP_ID       NUMBER(10)      NOT NULL,
    EMP_NAME     VARCHAR2(100),
    DEPT_CD      VARCHAR2(20),
    SALARY       NUMBER(12,2),
    HIRE_DATE    DATE,
    SRC_UPD_DT   DATE,                          -- 소스 UPD_DT (동기화 시점 비교용)
    LOADED_DT    DATE DEFAULT SYSDATE,          -- 타겟 적재 시각
    CONSTRAINT PK_EMP_TGT PRIMARY KEY (EMP_ID)
);
