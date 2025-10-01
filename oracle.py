import oracledb
import config
import sys
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

QUERY = """
    WITH dm AS (
    SELECT CONTENT_ID, MAX(SUBPROGNM) AS SUBPROGNM
    FROM EBSDAS.DMC_META
    GROUP BY CONTENT_ID
    ),
    thumb AS (
    SELECT CONTENT_ID, PATH AS THUMB_PATH, "TYPE"
    FROM EBSDAS.MEDIA 
    WHERE "TYPE" = 'thumb'
    )
    SELECT c.CONTENT_ID, c.TITLE, dm.SUBPROGNM, cci.ARCHIVE_ID,
        cci.SYS_CLIP_ID, cci.BRODYMD, m.PATH AS PROXY_PATH, thumb.THUMB_PATH
    FROM EBSDAS.CONTENT c
    LEFT JOIN EBSDAS.CONTENT_CODE_INFO cci ON c.CONTENT_ID = cci.CONTENT_ID
    LEFT JOIN EBSDAS.MEDIA m ON c.CONTENT_ID = m.CONTENT_ID and m."TYPE" = 'proxy'
    LEFT JOIN dm ON c.CONTENT_ID = dm.CONTENT_ID
    LEFT JOIN thumb ON c.CONTENT_ID = thumb.CONTENT_ID
    WHERE c.META_TABLE_ID = '81722' AND c.CONTENT_ID = :content_id
"""

def connect_to_oracle():
    
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

def fetch_content_by_id(content_id: int | str):
    
    conn = connect_to_oracle()
    
    try:
        # 숫자 컬럼이면 안전하게 int 캐스팅 권장
        bind_val = int(content_id)
        with conn.cursor() as cursor:
            # (선택) 바인드 타입 명시 — 캐스팅 비용/실수 예방
            # cursor.setinputsizes(content_id=oracledb.NUMBER)
            cursor.execute(QUERY, {"content_id": bind_val})
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "CONTENT_ID": row[0],
                "TITLE": row[1],
                "SUBPROGNM": row[2],
                "ARCHIVE_ID": row[3],
                "SYS_CLIP_ID": row[4],
                "BRODYMD": row[5],
                "PROXY_PATH": row[6],
                "THUMB_PATH": row[7],
            }
        logger.info(f"✅ 데이터 조회 성공: CONTENT_ID={content_id}")
        
    except Exception as e:
        logger.error(f"❌ 데이터 조회 실패: {e}")
        return None
    
    finally:
        if conn:
            try:
                conn.close()
                logger.info("✅ Oracle 연결 종료")
            except Exception as e:
                logger.error(f"❌ Oracle 연결 종료 실패: {e}") 
