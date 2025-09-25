# sqlite_db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from datetime import datetime
import config
import os

DB_FILENAME = "stt_index.db"
NAS_PREFIX = "/mnt/nas_stt/"

def _strip_prefix(path: str) -> str:
    if path.startswith(NAS_PREFIX):
        return path[len(NAS_PREFIX):].lstrip("/")  # 앞에 / 중복 제거
    return path

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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt_index (
                content_id   TEXT PRIMARY KEY,
                title        TEXT,
                subprognm    TEXT,
                archive_id   TEXT,
                sys_clip_id  TEXT,
                brodymd      TEXT,
                proxy_path   TEXT,
                thumb_path   TEXT,
                wav_path     TEXT,
                json_path    TEXT,
                created_at   TEXT,
                updated_at   TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_brodymd ON stt_index(brodymd);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_title ON stt_index(title);")
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
    
