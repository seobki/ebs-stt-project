# utils/logger_utils.py
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from config import BASE_STT_LOG  # 예: "/mnt/nas_stt/log"

class DateNamedDailyFileHandler(TimedRotatingFileHandler):
    """
    파일명을 날짜(YYYYMMDD.txt)만 쓰는 일별 로테이션 핸들러.
    - 자정마다 현재 파일을 닫고, 새 날짜 이름으로 파일을 열어 이어서 기록.
    - 이전 파일 이름은 그대로(예: 20251001.txt), rename/삭제 없음.
    - backupCount=0이면 삭제하지 않음(기본).
    """

    def __init__(self, log_dir: str, encoding: str = "utf-8", backupCount: int = 0):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 오늘 날짜 파일 경로 생성 (예: /mnt/nas_stt/log/20251001.txt)
        today_name = datetime.now().strftime("%Y%m%d") + ".txt"
        base_path = str(self.log_dir / today_name)

        # when="midnight": 자정 기준 롤오버 / interval=1: 1일마다
        super().__init__(filename=base_path,
                         when="midnight",
                         interval=1,
                         backupCount=backupCount,
                         encoding=encoding,
                         utc=False)

        # suffix는 사용하지 않습니다(파일명에 prefix를 붙이지 않기 위함).
        # TimedRotatingFileHandler의 emit() 로직은 rolloverAt을 보고 doRollover()를 호출하므로
        # 여기서는 doRollover에서 직접 새 날짜 파일을 열도록 오버라이드합니다.

    def doRollover(self):
        """
        기본 동작(기존 파일을 'basename.suffix'로 rename) 대신,
        - 현재 스트림을 닫고
        - baseFilename을 '새 날짜 파일 경로'로 바꾼 뒤
        - 새 파일로 다시 연다.
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # 현재 시각(로컬) 기준 새 날짜 파일명 계산
        current_time = int(time.time())
        time_tuple = time.localtime(current_time)
        new_name = time.strftime("%Y%m%d", time_tuple) + ".txt"
        self.baseFilename = str(self.log_dir / new_name)

        # 새 파일로 다시 오픈 (append 모드)
        self.mode = "a"
        self.stream = self._open()

        # 다음 롤오버 시점 갱신 (부모 클래스 로직과 동일하게)
        new_rollover_at = self.computeRollover(current_time)
        # 롤오버 시점이 현재/과거로 설정되는 가장자리 케이스 보정
        while new_rollover_at <= current_time:
            new_rollover_at = new_rollover_at + self.interval
        self.rolloverAt = new_rollover_at


def get_logger(
    name: str = "stt_app",
    log_level: int | str = logging.INFO,
    delete_old_days: int = 0,  # 0이면 삭제하지 않음
) -> logging.Logger:
    """
    공용 로거 생성:
    - 콘솔(stdout) + 날짜파일(YYYYMMDD.txt) 동시 기록
    - 자정 자동 롤오버, 파일명은 날짜만
    - delete_old_days=0 → 오래된 파일 자동 삭제 안함 (NAS에 계속 쌓임)
    """
    logger = logging.getLogger(name)
    if logger.handlers:  # 중복 생성 방지
        return logger

    # 문자열 레벨도 허용
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)

    logger.setLevel(logging.DEBUG)  # 내부 최저 레벨은 DEBUG

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] [%(module)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) 콘솔 핸들러 (운영에선 INFO 이상 권장)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 2) 날짜 파일 핸들러 (DEBUG까지 자세히 기록)
    fh = DateNamedDailyFileHandler(log_dir=BASE_STT_LOG, backupCount=delete_old_days)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False
    return logger
