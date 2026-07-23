-- [공통] LRM 작업 QUERY
-------------------------------------------------------------------------------------------------------------

-- (1) 테이블 목록
SELECT
       --'TRUNCATE TABLE ' || A.OWNER || '.' || A.TABLE_NAME || ';'
       --'UNION ALL SELECT ''' || A.TABLE_NAME || ''' AS TBL_ID, ''' || B.COMMENTS || ''' AS TBL_NAME, COUNT(*) AS CNT FROM ' || A.OWNER || '.' || A.TABLE_NAME AS TBL
       --A.TABLE_NAME || '(' || B.COMMENTS || ')' AS TBL
       --B.COMMENTS || ' (' || A.TABLE_NAME || ')' AS TBL
       --A.TABLE_NAME, B.COMMENTS --, A.AVG_ROW_LEN
       A.OWNER || '.' || A.TABLE_NAME || '  /* ' || B.COMMENTS || ' */' AS TBL
  FROM ALL_TABLES       A
     , ALL_TAB_COMMENTS B
 WHERE A.OWNER      = B.OWNER
   AND A.TABLE_NAME = B.TABLE_NAME
   AND A.OWNER      = 'LRM' --'[OWNER]'
   AND (:table_name IS NULL OR (A.TABLE_NAME LIKE '%' || :table_name || '%'))
   AND (:comments   IS NULL OR (B.COMMENTS   LIKE '%' || :comments   || '%'))
 ORDER BY A.TABLE_NAME
;


-- (2) 컬럼, 코멘트
SELECT
       --'     , #' || RPAD(LOWER(A.COLUMN_NAME) || '#', 20, ' ')|| ' AS '  || RPAD(A.COLUMN_NAME, 20, ' ') || ' /* ' || B.COMMENTS || ' */'
       --'   AND ' || RPAD(A.COLUMN_NAME, 30, ' ') || ' = ' || CASE WHEN A.DATA_TYPE = 'NUMBER' THEN '0000  ' ELSE '''AAAA''' END || ' /* ' || B.COMMENTS || ' */'
       --'   AND A.' || RPAD(A.COLUMN_NAME, 20, ' ') || ' = B.' || RPAD(A.COLUMN_NAME || '(+)', 20, ' ') || ' /* ' || B.COMMENTS || ' */'
       --
       --A.TABLE_NAME, A.COLUMN_ID, A.COLUMN_NAME, B.COMMENTS, A.DATA_TYPE || CASE WHEN A.DATA_TYPE = 'DATE' THEN '' WHEN A.DATA_TYPE = 'NUMBER' THEN '(' || CASE WHEN A.DATA_PRECISION > 0 AND A.DATA_SCALE > 0 THEN A.DATA_PRECISION||','||A.DATA_SCALE ELSE TO_CHAR(A.DATA_PRECISION) END || ')' ELSE '(' || TO_CHAR (A.DATA_LENGTH) || ')' END AS DATA_TYPE, A.NULLABLE
       --A.COLUMN_ID, A.COLUMN_NAME, B.COMMENTS, A.DATA_TYPE || CASE WHEN A.DATA_TYPE = 'DATE' THEN '' WHEN A.DATA_TYPE = 'NUMBER' THEN '(' || CASE WHEN A.DATA_PRECISION > 0 AND A.DATA_SCALE > 0 THEN A.DATA_PRECISION||','||A.DATA_SCALE ELSE TO_CHAR(A.DATA_PRECISION) END || ')' ELSE '(' || TO_CHAR (A.DATA_LENGTH) || ')' END AS DATA_TYPE, A.NULLABLE
       --A.COLUMN_NAME, B.COMMENTS
       --RPAD(A.COLUMN_ID, 4, ' ') || RPAD(A.COLUMN_NAME, 30, ' ') || RPAD(B.COMMENTS, 30, ' ') || RPAD(A.DATA_TYPE || CASE WHEN A.DATA_TYPE = 'DATE' THEN '' WHEN A.DATA_TYPE = 'NUMBER' THEN '(' || CASE WHEN A.DATA_PRECISION > 0 AND A.DATA_SCALE > 0 THEN A.DATA_PRECISION||','||A.DATA_SCALE ELSE TO_CHAR(A.DATA_PRECISION) END || ')' ELSE '(' || TO_CHAR (A.DATA_LENGTH) || ')' END, 16, ' ') || A.NULLABLE
       --
       --'     , ' || A.COLUMN_NAME
       '     , ' || RPAD(A.COLUMN_NAME, 30, ' ') || ' /* ' || B.COMMENTS || ' */'
  FROM ALL_TAB_COLUMNS  A
     , ALL_COL_COMMENTS B
 WHERE A.OWNER       = B.OWNER
   AND A.TABLE_NAME  = B.TABLE_NAME
   AND A.COLUMN_NAME = B.COLUMN_NAME
   AND A.OWNER       = 'LRM'
   AND A.TABLE_NAME  = 'LRC401P'
   --AND A.TABLE_NAME LIKE 'RND%'
   --AND B.COMMENTS LIKE '%금액'
 ORDER BY A.TABLE_NAME
        , A.COLUMN_ID
;


LRM.LRC400P  /* 산출_포지션마스터내역P */

;

SELECT STND_DATE                      /* 기준_일자 */
     , JOB_DAY_CLS_CODE               /* 작업_일_구분_코드 */
     , PSTN_ID                        /* 포지션_ID */
     , PSTN_SRC_CLS_CODE              /* 포지션_원천_구분_코드 */
     , PSTN_SRNO                      /* 포지션_일련번호 */
     , RMCOA_CODE                     /* RMCoA_코드 */
     , OCRN_DATE                      /* 발생_일자 */
     , MTRT_DATE                      /* 만기_일자 */
     , PSTN_CLS_CODE                  /* 포지션_구분_코드 */
     , SEL_BUY_CLS_CODE               /* 매도_매수_구분_코드 */
     , PAY_RCVN_CLS_CODE              /* 지급_수취_구분_코드 */
     , CRNC_CODE                      /* 통화_코드 */
     , CSFL_OCRN_AMT                  /* 현금흐름_발생_금액 */
     , PSTN_DVSN_RATE                 /* 포지션_분할_비율 */
     , BS_VRFC_STND_AMT               /* BS_대사_기준_금액 */
     , PROC_PGM_ID                    /* 처리_프로그램_ID */
     , PROC_DT                        /* 처리_일시 */
  FROM LRM.LRC401P  /* 산출_기초포지션내역P */
 WHERE 1 = 1
   AND STND_DATE = '20171231'
   AND JOB_DAY_CLS_CODE = 'M'
   AND PSTN_ID LIKE 'FBS%'
   AND ROWNUM <= 50
;

SELECT STND_DATE, JOB_DAY_CLS_CODE, COUNT(*)
  FROM LRM.LRC401P  /* 산출_기초포지션내역P */
 GROUP BY STND_DATE, JOB_DAY_CLS_CODE
 ORDER BY STND_DATE, JOB_DAY_CLS_CODE
;