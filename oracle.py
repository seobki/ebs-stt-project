from __future__ import annotations
import oracledb
import config
import sys
from typing import Dict, List, Optional
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

# 연도 "구간" 한 방 쿼리 (상한 배제)
QUERY_BY_RANGE = """
WITH dm AS (
  SELECT CONTENT_ID, MAX(SUBPROGNM) AS SUBPROGNM
  FROM EBSDAS.DMC_META
  GROUP BY CONTENT_ID
),
m_proxy AS (
  SELECT CONTENT_ID, STORAGE, PATH
  FROM EBSDAS.MEDIA
  WHERE "TYPE" = 'proxy'
),
thumb AS (
  SELECT CONTENT_ID, PATH AS THUMB_PATH
  FROM EBSDAS.MEDIA
  WHERE "TYPE" = 'thumb'
)
SELECT
  c.CONTENT_ID,
  c.TITLE,
  dm.SUBPROGNM,
  cci.ARCHIVE_ID,
  cci.SYS_CLIP_ID,
  cci.BRODYMD,
  m_proxy.STORAGE,
  m_proxy.PATH        AS PROXY_PATH,
  thumb.THUMB_PATH
FROM EBSDAS.CONTENT c
JOIN m_proxy                        ON m_proxy.CONTENT_ID = c.CONTENT_ID
LEFT JOIN EBSDAS.CONTENT_CODE_INFO cci ON cci.CONTENT_ID = c.CONTENT_ID
LEFT JOIN dm                         ON dm.CONTENT_ID = c.CONTENT_ID
LEFT JOIN thumb                      ON thumb.CONTENT_ID = c.CONTENT_ID
WHERE c.META_TABLE_ID = '81722'
  AND c.CREATED_TIME >= :from_ymd   -- 예: '20170101000000'
  AND c.CREATED_TIME <  :to_ymd     -- 예: '20260101000000'
"""

def connect_to_oracle() -> oracledb.Connection:
    try:
        conn = oracledb.connect(
            user=config.ORACLE_ID,
            password=config.ORACLE_PW,
            dsn=config.ORACLE_DSN,
        )
        logger.info("✅ Oracle 연결 성공")
        return conn
    except oracledb.DatabaseError as e:
        logger.error(f"❌ Oracle 연결 실패: {e}")
        sys.exit(1)

def _row_to_dict(cols: list[str], row: tuple) -> Dict:
    return dict(zip(cols, row))

def _ymd_range(start_year: int, end_year: int) -> tuple[str, str]:
    """[start_year, end_year]를 모두 포함하는 상한배제 경계 생성"""
    return (f"{start_year}0101000000", f"{end_year+1}0101000000")

def fetch_contents_by_year_range(start_year: int, end_year: int) -> List[Dict]:
    """
    주어진 연도 구간을 한 번의 쿼리로 전부 가져온 뒤 fetchall()로 리스트 반환.
    """
    from_ymd, to_ymd = _ymd_range(start_year, end_year)
    conn = connect_to_oracle()
    try:
        with conn.cursor() as cur:
            # fetchall 성능을 위해 넉넉히
            cur.arraysize = 2000
            cur.prefetchrows = 2000
            cur.execute(QUERY_BY_RANGE, {"from_ymd": from_ymd, "to_ymd": to_ymd})
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            data = [_row_to_dict(cols, r) for r in rows]
            logger.info(f"✅ 연도 구간 조회 완료: {start_year}~{end_year}, {len(data)}건")
            return data
    except Exception as e:
        logger.error(f"❌ 연도 구간 조회 실패({start_year}~{end_year}): {e}")
        return []
    finally:
        try:
            conn.close()
            logger.info("✅ Oracle 연결 종료")
        except Exception as e:
            logger.error(f"❌ Oracle 연결 종료 실패: {e}")
