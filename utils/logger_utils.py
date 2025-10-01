# utils/logger_utils.py
import logging
import sys
from pathlib import Path
from datetime import datetime
from config import BASE_STT_LOG  # "/mnt/nas_stt/log" 경로

def get_logger(
    name: str = "stt_app",
    log_level: int | str = logging.INFO
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # 이미 핸들러 있으면 재사용
        return logger

    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 콘솔 로그
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 오늘 날짜 기반 파일명 (예: 20251001.txt)
    Path(BASE_STT_LOG).mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    log_file = Path(BASE_STT_LOG) / f"{today_str}.txt"

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)  # 파일에는 DEBUG까지 상세히 기록
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False
    return logger
