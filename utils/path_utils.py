from pathlib import Path

def shard_filepath(base_dir: Path | str, content_id: str | int, ext: str) -> Path:
    """
    예: 25051248 -> base/25/05/25051248.ext
    """
    base = Path(base_dir)
    cid = str(content_id).zfill(8)  # 안전하게 8자리 보정
    level1, level2 = cid[:2], cid[2:4]
    return base / level1 / level2 / f"{cid}{ext}"

def ensure_parent_dir(path: Path | str) -> Path:
    # 지정된 경로의 부모 디렉토리 생성 (안전장치)
    
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


