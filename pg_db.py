# pg_db.py  (PostgreSQL backend)
from __future__ import annotations
from typing import List, Dict, Sequence, Tuple, Optional
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import config
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

# NAS 경로를 상대경로로 정규화 (원래 sqlite_db.py의 동작을 동일하게 유지)
NAS_PREFIX = "/mnt/nas_stt/"

def _strip_prefix(path: str) -> str:
    if path.startswith(NAS_PREFIX):
        return path[len(NAS_PREFIX):].lstrip("/")
    return path

def _hhmmss_to_ms(hhmmss: str) -> int:
    """'HH:MM:SS' 또는 'HH:MM:SS.sss' -> ms(int, 소수점 버림)"""
    s = (hhmmss or "").strip()
    if not s:
        return 0
    if "." in s:
        hms, _ = s.split(".", 1)
    else:
        hms = s
    parts = hms.split(":")
    if len(parts) == 2:
        h, m, sec = 0, int(parts[0]), int(parts[1])
    else:
        h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
    return (h * 3600 + m * 60 + sec) * 1000

# ---- DB 연결 유틸 -----------------------------------------------------------

def _pg_dsn() -> str:
    """
    config에 다음 중 하나가 있어야 합니다.
      - POSTGRES_DSN = "postgresql://user:pass@host:5432/dbname"
    또는 개별 항목:
      - PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE
    """
    if hasattr(config, "POSTGRES_DSN") and config.POSTGRES_DSN:
        return config.POSTGRES_DSN

    user = getattr(config, "PG_USER", "stt")
    pwd  = getattr(config, "PG_PASSWORD", "")
    host = getattr(config, "PG_HOST", "127.0.0.1")
    port = getattr(config, "PG_PORT", 5432)
    db   = getattr(config, "PG_DATABASE", "sttdb")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

def _connect():
    conn = psycopg2.connect(_pg_dsn())
    conn.autocommit = False
    return conn

# ---- 스키마 초기화 (idempotent) --------------------------------------------

_SCHEMA_SQL = """
-- ① 메타 테이블
CREATE TABLE IF NOT EXISTS public.stt_index (
    content_id   TEXT PRIMARY KEY,
    title        TEXT,
    subprognm    TEXT,
    archive_id   TEXT,
    sys_clip_id  TEXT,
    brodymd      TEXT,
    proxy_path   TEXT,
    wav_path     TEXT,
    json_path    TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_brodymd ON public.stt_index(brodymd);
CREATE INDEX IF NOT EXISTS idx_title   ON public.stt_index(title);

-- ② 세그먼트 테이블
CREATE TABLE IF NOT EXISTS public.stt_segment (
    content_id  TEXT NOT NULL,
    seg_no      INTEGER NOT NULL,
    start_ms    INTEGER NOT NULL,
    end_ms      INTEGER NOT NULL,
    text        TEXT NOT NULL,
    PRIMARY KEY (content_id, seg_no),
    FOREIGN KEY (content_id)
        REFERENCES public.stt_index(content_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_segment_text
  ON public.stt_segment(text);

CREATE INDEX IF NOT EXISTS idx_segment_start
  ON public.stt_segment(content_id, start_ms);

-- ③ updated_at 자동 갱신 트리거
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at ON public.stt_index;

CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON public.stt_index
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
"""

def init_db() -> None:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
        logger.info("✅ PostgreSQL 테이블/트리거 준비 완료")
    except Exception as e:
        logger.error(f"❌ PostgreSQL 스키마 준비 실패: {e}")
        raise

# ---- UPSERT: stt_index ------------------------------------------------------

UPSERT_STT_INDEX = """
INSERT INTO public.stt_index (
  content_id, title, subprognm, archive_id, sys_clip_id,
  brodymd, proxy_path, wav_path, json_path
) VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s, %s
)
ON CONFLICT (content_id) DO UPDATE SET
  title       = EXCLUDED.title,
  subprognm   = EXCLUDED.subprognm,
  archive_id  = EXCLUDED.archive_id,
  sys_clip_id = EXCLUDED.sys_clip_id,
  brodymd     = EXCLUDED.brodymd,
  proxy_path  = EXCLUDED.proxy_path,
  wav_path    = EXCLUDED.wav_path,
  json_path   = EXCLUDED.json_path;
-- updated_at은 트리거로 자동 갱신
"""

def upsert_record(results: dict, wav_path: str, json_path: str) -> None:
    """
    SQLite 버전과 동일한 시그니처 유지.
    Postgres 테이블은 thumb_path 컬럼이 없으므로 사용하지 않습니다.
    """
    brodymd = results.get("BRODYMD")
    if hasattr(brodymd, "strftime"):
        brodymd = brodymd.strftime("%Y%m%d")

    cid = str(results.get("CONTENT_ID", "")).strip()

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_STT_INDEX,
                    (
                        cid,
                        results.get("TITLE"),
                        results.get("SUBPROGNM"),
                        results.get("ARCHIVE_ID"),
                        results.get("SYS_CLIP_ID"),
                        brodymd,
                        results.get("PROXY_PATH"),
                        _strip_prefix(wav_path),
                        _strip_prefix(json_path),
                    ),
                )
            conn.commit()
        logger.info(f"✅ PostgreSQL 저장 완료: {cid}")
    except Exception as e:
        logger.error(f"❌ PostgreSQL 저장 실패: {cid}, error={e}")
        raise

# ---- UPSERT: stt_segment ----------------------------------------------------

UPSERT_SEGMENT = """
INSERT INTO public.stt_segment (content_id, seg_no, start_ms, end_ms, text)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (content_id, seg_no) DO UPDATE SET
  start_ms = EXCLUDED.start_ms,
  end_ms   = EXCLUDED.end_ms,
  text     = EXCLUDED.text;
"""

def upsert_segments(content_id: str, stt_segments: List[Dict]) -> None:
    """
    STT 결과(세그먼트 리스트)를 stt_segment에 벌크 upsert.
    seg_no는 0부터 증가. (같은 content_id 재처리 시 덮어씀)
    """
    rows: list[Tuple[str, int, int, int, str]] = []
    for i, seg in enumerate(stt_segments):
        start_ms = _hhmmss_to_ms(str(seg.get("start", "0")))
        end_ms   = _hhmmss_to_ms(str(seg.get("end", "0")))
        text     = str(seg.get("text", "")).strip()
        rows.append((content_id, i, start_ms, end_ms, text))

    if not rows:
        logger.debug(f"⚠️ upsert_segments: 빈 세그먼트 - content_id={content_id}")
        return

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_SEGMENT, rows, page_size=2000)
            conn.commit()
        logger.info(f"✅ 세그먼트 저장 완료: content_id={content_id}, count={len(rows)}")
    except Exception as e:
        logger.error(f"❌ 세그먼트 저장 실패: content_id={content_id}, error={e}")
        raise

_CHECK_EXISTS = "SELECT 1 FROM public.stt_index WHERE content_id = %s LIMIT 1;"
def has_record(content_id: str) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_CHECK_EXISTS, (content_id,))
            return cur.fetchone() is not None
