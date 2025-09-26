# sqlite_db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import config

DB_FILENAME = "stt_index.db"
NAS_PREFIX = "/mnt/nas_stt/"

def _strip_prefix(path: str) -> str:
    if path.startswith(NAS_PREFIX):
        return path[len(NAS_PREFIX):].lstrip("/")  # 앞에 / 중복 제거
    return path

def _hhmmss_to_ms(hhmmss: str) -> int:
    """'HH:MM:SS' 또는 'HH:MM:SS.sss' -> ms(int, 소수점 버림)"""
    s = hhmmss.strip()
    if not s:
        return 0
    if "." in s:
        hms, _ = s.split(".", 1)  # 소수점 이하 버림
    else:
        hms = s
    parts = hms.split(":")
    if len(parts) == 2:
        h, m, sec = 0, int(parts[0]), int(parts[1])
    else:
        h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
    return (h * 3600 + m * 60 + sec) * 1000

def _db_path() -> Path:
    # 이제 프로젝트 하위 db/ 를 사용
    return Path(config.BASE_STT_DB) / DB_FILENAME

def _connect() -> sqlite3.Connection:
    p = _db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p, timeout=30)
    # 로컬 디스크에서는 WAL 권장
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=15000;")  # 15초
    return conn

def init_db() -> None:
    with _connect() as conn:
        cur = conn.cursor()

        # ① 메타 테이블
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt_index (
                content_id   TEXT PRIMARY KEY,
                title        TEXT,
                subprognm    TEXT,
                archive_id   TEXT,
                sys_clip_id  TEXT,
                brodymd      TEXT,
                proxy_path   TEXT,
                wav_path     TEXT,
                json_path    TEXT,
                created_at   TEXT,
                updated_at   TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_brodymd ON stt_index(brodymd);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_title  ON stt_index(title);")

        # ② 세그먼트 테이블 (검색 대상)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt_segment (
                content_id  TEXT NOT NULL,
                seg_no      INTEGER NOT NULL,   -- 0..N
                start_ms    INTEGER NOT NULL,
                end_ms      INTEGER NOT NULL,
                text        TEXT NOT NULL,
                PRIMARY KEY (content_id, seg_no)
            );
        """)
        # 부분일치 속도용(간단 LIKE 검색 기준). 향후 PG 전환 시 pg_trgm/PGroonga 권장
        cur.execute("CREATE INDEX IF NOT EXISTS idx_segment_text ON stt_segment(text);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_segment_start ON stt_segment(content_id, start_ms);")

        conn.commit()
        print(f"✅ SQLite 테이블 준비 완료: {_db_path()}")

def upsert_record(results: dict, wav_path: str, json_path: str) -> None:
    brodymd = results.get("BRODYMD")
    if hasattr(brodymd, "strftime"):
        brodymd = brodymd.strftime("%Y%m%d")

    cid = str(results.get("CONTENT_ID", "")).strip()
    now = datetime.now().isoformat(timespec="seconds")
    
    
    try:    
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO stt_index (
                    content_id, title, subprognm, archive_id, sys_clip_id, brodymd,
                    proxy_path, thumb_path, wav_path, json_path, created_at, updated_at
                ) VALUES (
                    :content_id, :title, :subprognm, :archive_id, :sys_clip_id, :brodymd,
                    :proxy_path, :thumb_path, :wav_path, :json_path, :created_at, :updated_at
                )
                ON CONFLICT(content_id) DO UPDATE SET
                    title       = excluded.title,
                    subprognm   = excluded.subprognm,
                    archive_id  = excluded.archive_id,
                    sys_clip_id = excluded.sys_clip_id,
                    brodymd     = excluded.brodymd,
                    proxy_path  = excluded.proxy_path,
                    thumb_path  = excluded.thumb_path,
                    wav_path    = excluded.wav_path,
                    json_path   = excluded.json_path,
                    updated_at  = excluded.updated_at
            """, {
                "content_id": cid,
                "title":      results.get("TITLE"),
                "subprognm":  results.get("SUBPROGNM"),
                "archive_id": results.get("ARCHIVE_ID"),
                "sys_clip_id":results.get("SYS_CLIP_ID"),
                "brodymd":    brodymd,
                "proxy_path": results.get("PROXY_PATH"),
                "thumb_path": results.get("THUMB_PATH"),
                "wav_path":   _strip_prefix(wav_path),
                "json_path":  _strip_prefix(json_path),
                "created_at": now,
                "updated_at": now,
            })
            conn.commit()
            print(f"✅ SQLite 저장 완료: {cid}")
            
    except Exception as e:
        print(f"❌ SQLite 저장 실패: {e}")
        
def upsert_segments(content_id: str, stt_segments: List[Dict]) -> None:
    """
    STT 결과(세그먼트 리스트)를 stt_segment에 벌크 upsert.
    seg_no는 0부터 증가. (같은 content_id 재처리 시 덮어씀)
    """
    rows = []
    for i, seg in enumerate(stt_segments):
        start_ms = _hhmmss_to_ms(str(seg.get("start", "0")))
        end_ms   = _hhmmss_to_ms(str(seg.get("end", "0")))
        text     = str(seg.get("text", "")).strip()
        rows.append((content_id, i, start_ms, end_ms, text))

    if not rows:
        print(f"⚠️ upsert_segments: 빈 세그먼트 - content_id={content_id}")
        return
    
    try:
        with _connect() as conn:
            cur = conn.cursor()
            # ON CONFLICT로 seg_no 기준 덮어쓰기
            cur.executemany("""
                INSERT INTO stt_segment (content_id, seg_no, start_ms, end_ms, text)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(content_id, seg_no) DO UPDATE SET
                start_ms = excluded.start_ms,
                end_ms   = excluded.end_ms,
                text     = excluded.text
            """, rows)
            conn.commit()
            print(f"✅ 세그먼트 저장 완료: content_id={content_id}, count={len(rows)}")
    
    except Exception as e:
        print(f"❌ 세그먼트 저장 실패: content_id={content_id}, error={e}")
    
