from __future__ import annotations

import hashlib
import logging
import logging.handlers
import os
import re
import sqlite3
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

try:
    import pyodbc
except ImportError:
    pyodbc = None

load_dotenv(override=True)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED_IDENTIFIER = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
)

# ============================================================
# Configuration
# ============================================================

@dataclass(frozen=True)
class Config:
    db_driver: str
    db_server: str
    db_database: str
    db_user: Optional[str]
    db_password: Optional[str]
    db_trusted_connection: bool

    table_name: str
    id_column: str
    created_column: str
    qr_key_column: str
    
    date_column_map: dict[str, str]
    
    translate_columns: tuple[tuple[str, str], ...]
    ai_translate_update_column: Optional[str]
    work_state_table: str

    lm_url: str
    lm_api_key: Optional[str]
    lm_model: str
    connect_timeout_seconds: float
    request_timeout_seconds: float
    max_output_tokens: int
    llm_retry_count: int
    llm_retry_delay_seconds: float

    poll_interval_seconds: float
    batch_size: int
    failure_retry_seconds: int
    cache_db_file: Path
    log_dir: Path
    log_file_name: str
    log_backup_days: int
    use_db_applock: bool
    dry_run: bool


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else value.strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name, "1" if default else "0")
    return str(raw).lower() in {"1", "true", "yes", "y", "on"}


def _parse_pairs(raw: str) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(
                f"TRANSLATE_COLUMNS không hợp lệ: {part!r}. "
                "Định dạng đúng: source:target,source2:target2"
            )
        source, target = (x.strip() for x in part.split(":", 1))
        if not source or not target:
            raise ValueError(f"Cặp cột không hợp lệ: {part!r}")
        pair = (source, target)
        if pair not in seen:
            pairs.append(pair)
            seen.add(pair)

    if not pairs:
        raise ValueError("TRANSLATE_COLUMNS phải có ít nhất một cặp cột.")
    return tuple(pairs)

def _parse_date_column_map(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue

        if ":" not in part:
            raise ValueError(
                f"DATE_COLUMN_MAP không hợp lệ: {part!r}. "
                "Định dạng đúng: topic:date_column"
            )

        topic, date_column = (
            value.strip()
            for value in part.split(":", 1)
        )

        if not topic or not date_column:
            raise ValueError(
                f"DATE_COLUMN_MAP không hợp lệ: {part!r}"
            )

        if not _IDENTIFIER.fullmatch(topic):
            raise ValueError(
                f"Topic không an toàn trong DATE_COLUMN_MAP: {topic!r}"
            )

        if not _IDENTIFIER.fullmatch(date_column):
            raise ValueError(
                f"Tên cột ngày không an toàn: {date_column!r}"
            )

        result[topic.lower()] = date_column

    if not result:
        raise ValueError(
            "DATE_COLUMN_MAP phải có ít nhất một cấu hình."
        )

    return result

def load_config() -> Config:
    default_pairs = (
        "comment:comment_jp,"
        "countermeasure:countermeasure_jp,"
        "at_comment:at_comment_jp,"
        "hse_comment:hse_comment_jp,"
        "comment_jp:comment,"
        "countermeasure_jp:countermeasure,"
        "at_comment_jp:at_comment,"
        "hse_comment_jp:hse_comment"
    )
    default_date_column_map = (
        "comment:createdAt,"
        "countermeasure:createdAt,"
        "at_comment:at_date,"
        "hse_comment:hse_date"
    )
    cfg = Config(
        db_driver=_env("DB_DRIVER", "{ODBC Driver 17 for SQL Server}") or "",
        db_server=_env("DB_SERVER", "") or "",
        db_database=_env("DB_DATABASE", "") or "",
        db_user=_env("DB_USER"),
        db_password=_env("DB_PASSWORD"),
        db_trusted_connection=_env_bool("DB_TRUSTED_CONNECTION", False),

        table_name=_env("TABLE_NAME", "dbo.F2_Patrol_Report") or "",
        id_column=_env("ID_COLUMN", "id") or "",
        created_column=_env("CREATED_COLUMN", "createdAt") or "",
        qr_key_column=_env("QR_KEY_COLUMN", "qr_key") or "",
        
        date_column_map=_parse_date_column_map(_env("DATE_COLUMN_MAP",default_date_column_map,) or default_date_column_map),
        
        translate_columns=_parse_pairs(
            _env("TRANSLATE_COLUMNS", default_pairs) or default_pairs
        ),
        ai_translate_update_column=_env(
            "AI_TRANSLATE_UPDATE_COLUMN", "ai_translate_update_at"
        ),
        work_state_table=_env(
            "WORK_STATE_TABLE", "dbo.PatrolTranslateWorkState"
        ) or "",
    
        lm_url=(_env("LM_URL", "http://127.0.0.1:1234") or "").rstrip("/"),
        lm_api_key=_env("LM_API_KEY"),
        lm_model=_env("LM_MODEL", "openai/gpt-oss-20b") or "",
        connect_timeout_seconds=float(_env("CONNECT_TIMEOUT_SECONDS", "10") or 10),
        request_timeout_seconds=float(_env("REQUEST_TIMEOUT_SECONDS", "120") or 120),
        max_output_tokens=int(_env("MAX_OUTPUT_TOKENS", "500") or 500),
        llm_retry_count=max(0, int(_env("LLM_RETRY_COUNT", "2") or 2)),
        llm_retry_delay_seconds=max(
            0.1, float(_env("LLM_RETRY_DELAY_SECONDS", "2") or 2)
        ),

        poll_interval_seconds=max(
            2.0, float(_env("POLL_INTERVAL_SECONDS", "5") or 5)
        ),
        batch_size=max(1, int(_env("BATCH_SIZE", "20") or 20)),
        failure_retry_seconds=max(
            30, int(_env("FAILURE_RETRY_SECONDS", "300") or 300)
        ),
        cache_db_file=Path(
            _env("CACHE_DB_FILE", "./patrol_translate_cache.sqlite3") or ""
        ),
        log_dir=Path(_env("LOG_DIR", "./logs") or ""),
        log_file_name=_env("LOG_FILE_NAME", "patrol_translate.log") or "",
        log_backup_days=max(1, int(_env("LOG_BACKUP_DAYS", "14") or 14)),
        use_db_applock=_env_bool("USE_DB_APPLOCK", True),
        dry_run=_env_bool("DRY_RUN", False),
    )

    if not cfg.db_server:
        raise ValueError("Thiếu DB_SERVER trong file .env")
    if not cfg.db_database:
        raise ValueError("Thiếu DB_DATABASE trong file .env")
    if not cfg.db_trusted_connection and (not cfg.db_user or not cfg.db_password):
        raise ValueError(
            "Cần DB_USER + DB_PASSWORD hoặc DB_TRUSTED_CONNECTION=yes"
        )
    return cfg



# ============================================================
# Logging and validation helpers
# ============================================================

def setup_logging(cfg: Config) -> logging.Logger:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("patrol_translate")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(threadName)s - %(message)s"
    )

    file_handler = logging.handlers.TimedRotatingFileHandler(
        cfg.log_dir / cfg.log_file_name,
        when="midnight",
        backupCount=cfg.log_backup_days,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


log = logging.getLogger("patrol_translate")




def _safe_column(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"Tên cột không an toàn: {name!r}")
    return f"[{name}]"


def _safe_table(name: str) -> str:
    if not _QUALIFIED_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Tên bảng không an toàn: {name!r}")
    return ".".join(f"[{part}]" for part in name.split("."))


def normalize(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\u00a0", " ").strip()
    return text or None


def _is_japanese_column(name: str) -> bool:
    value = name.lower()
    return value.endswith(("_jp", "_ja", "_japanese")) or "japanese" in value


def _has_japanese(text: str) -> bool:
    return any(
        "\u3040" <= ch <= "\u30ff"
        or "\u3400" <= ch <= "\u4dbf"
        or "\u4e00" <= ch <= "\u9fff"
        for ch in text
    )


def _has_latin_letter(text: str) -> bool:
    """
    Nhận diện phần chữ Latin/tiếng Việt.

    Lưu ý: hàm này chỉ dùng kết hợp với kiểm tra "dòng không có tiếng Nhật"
    để tránh coi mã như PLC trong một câu tiếng Nhật là phần tiếng Việt.
    """
    return any(
        ch.isalpha() and not _has_japanese(ch)
        for ch in text
    )


def _strip_language_label(line: str) -> str:
    """
    Bỏ nhãn ngôn ngữ ở đầu dòng nhưng giữ nguyên nội dung thực tế.

    Ví dụ:
        VI: Đã hiểu.       -> Đã hiểu.
        JP: 了解しました。 -> 了解しました。
    """
    return re.sub(
        r"^\s*(?:vi|vn|vietnamese|tiếng\s*việt|"
        r"ja|jp|japanese|tiếng\s*nhật)\s*[:：\-]\s*",
        "",
        line,
        flags=re.IGNORECASE,
    ).strip()


def _extract_existing_target_text(
    raw_text: str,
    target_language: str,
) -> Optional[str]:
    """
    Lấy trực tiếp phần ngôn ngữ đích nếu raw đã chứa song ngữ theo từng dòng.

    Chỉ tái sử dụng khi đồng thời tồn tại:
    - ít nhất một dòng có ký tự Nhật;
    - ít nhất một dòng Latin/Việt hoàn toàn không chứa ký tự Nhật.

    Điều kiện này cố tình chặt để tránh trường hợp một câu Nhật có mã Latin
    như "PLCに異常があります" bị hiểu sai thành đã có sẵn bản tiếng Việt.

    Ví dụ:
        OK
        了解しました。

    target_language="vi" -> "OK"
    target_language="ja" -> "了解しました。"
    """
    source = normalize(raw_text)
    if not source:
        return None

    normalized = source.replace("\r\n", "\n").replace("\r", "\n")
    lines = [
        _strip_language_label(line)
        for line in normalized.split("\n")
        if _strip_language_label(line)
    ]

    if len(lines) < 2:
        return None

    japanese_lines = [
        line
        for line in lines
        if _has_japanese(line)
    ]

    vietnamese_lines = [
        line
        for line in lines
        if not _has_japanese(line) and _has_latin_letter(line)
    ]

    # Raw phải thực sự có cả hai phần ngôn ngữ.
    if not japanese_lines or not vietnamese_lines:
        return None

    selected = (
        japanese_lines
        if target_language == "ja"
        else vietnamese_lines
    )

    result = "\n".join(selected).strip()
    return result or None


def _clean_translation(text: str) -> Optional[str]:
    value = normalize(text)
    if not value:
        return None

    value = re.sub(r"^```(?:json|text)?\s*", "", value, flags=re.I)
    value = re.sub(r"\s*```$", "", value)
    value = re.sub(
        r"^(translation|bản dịch|dịch|japanese|vietnamese)\s*:\s*",
        "",
        value,
        flags=re.I,
    )
    value = value.strip().strip('"').strip()

    if not value:
        return None
    if set(value) <= {"?", "�", ".", " ", "\n", "\r", "\t"}:
        return None
    return value


# ============================================================
# Database
# ============================================================

class DbConnection:
    def __init__(self, cfg: Config):
        if pyodbc is None:
            raise RuntimeError("Chưa cài pyodbc. Chạy: pip install pyodbc")
        self.cfg = cfg
        self.conn = None

    def _connection_string(self) -> str:
        parts = [
            f"DRIVER={self.cfg.db_driver}",
            f"SERVER={self.cfg.db_server}",
            f"DATABASE={self.cfg.db_database}",
            "TrustServerCertificate=yes",
            "MARS_Connection=yes",
        ]
        if self.cfg.db_trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.extend([
                f"UID={self.cfg.db_user}",
                f"PWD={self.cfg.db_password}",
            ])
        return ";".join(parts)

    def connect(self) -> None:
        self.close()
        self.conn = pyodbc.connect(
            self._connection_string(),
            timeout=max(1, int(self.cfg.connect_timeout_seconds)),
            autocommit=False,
        )
        self.conn.timeout = max(1, int(self.cfg.request_timeout_seconds))

    def ensure_connected(self) -> None:
        if self.conn is None:
            self.connect()
            return
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except Exception:
            self.connect()

    def cursor(self):
        self.ensure_connected()
        return self.conn.cursor()

    def commit(self) -> None:
        if self.conn is not None:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn is not None:
            try:
                self.conn.rollback()
            except Exception:
                pass

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None


def ensure_work_state_table(db: DbConnection, cfg: Config) -> None:
    """
    Tạo bảng trạng thái nếu chưa có và tự migration TẠI CHỖ nếu bảng cũ
    thiếu cột (ALTER TABLE ADD + backfill), KHÔNG rename/xóa dữ liệu cũ.

    QUAN TRỌNG: cố tình KHÔNG dùng cách "rename bảng cũ thành *_Legacy rồi
    tạo bảng mới" vì nó xóa sổ lịch sử failure_count/next_retry_at đang có,
    khiến mọi bản ghi từng FAILED bị coi là mới và dịch lại từ đầu ngay
    sau khi migrate — gây tốn LLM call và có thể ghi đè translated_text
    đang đúng bằng kết quả dịch lại không cần thiết.
    """
    table = _safe_table(cfg.work_state_table)
    object_name = cfg.work_state_table.replace("'", "''")

    sql = f"""
    SET NOCOUNT ON;

    IF OBJECT_ID(N'{object_name}', N'U') IS NULL
    BEGIN
        CREATE TABLE {table} (
            record_id NVARCHAR(128) NOT NULL,
            source_column NVARCHAR(128) NOT NULL,
            target_column NVARCHAR(128) NOT NULL,
            source_hash CHAR(64) NOT NULL,
            status NVARCHAR(24) NOT NULL,
            failure_count INT NOT NULL
                CONSTRAINT DF_PatrolTranslateFailure DEFAULT 0,
            next_retry_at DATETIME2 NULL,
            last_error NVARCHAR(1800) NULL,
            updated_at DATETIME2 NOT NULL
                CONSTRAINT DF_PatrolTranslateUpdated
                DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_PatrolTranslateWorkState
                PRIMARY KEY (record_id, source_column, target_column)
        );
    END;

    IF COL_LENGTH(N'{object_name}', N'record_id') IS NULL
        ALTER TABLE {table} ADD record_id NVARCHAR(128) NULL;

    IF COL_LENGTH(N'{object_name}', N'source_column') IS NULL
        ALTER TABLE {table} ADD source_column NVARCHAR(128) NULL;

    IF COL_LENGTH(N'{object_name}', N'target_column') IS NULL
        ALTER TABLE {table} ADD target_column NVARCHAR(128) NULL;

    IF COL_LENGTH(N'{object_name}', N'source_hash') IS NULL
        ALTER TABLE {table} ADD source_hash CHAR(64) NULL;

    IF COL_LENGTH(N'{object_name}', N'status') IS NULL
        ALTER TABLE {table} ADD status NVARCHAR(24) NULL;

    IF COL_LENGTH(N'{object_name}', N'failure_count') IS NULL
        ALTER TABLE {table} ADD failure_count INT NULL;

    IF COL_LENGTH(N'{object_name}', N'next_retry_at') IS NULL
        ALTER TABLE {table} ADD next_retry_at DATETIME2 NULL;

    IF COL_LENGTH(N'{object_name}', N'last_error') IS NULL
        ALTER TABLE {table} ADD last_error NVARCHAR(1800) NULL;

    IF COL_LENGTH(N'{object_name}', N'updated_at') IS NULL
        ALTER TABLE {table} ADD updated_at DATETIME2 NULL;

    UPDATE {table}
       SET source_hash = REPLICATE('0', 64)
     WHERE source_hash IS NULL
        OR LTRIM(RTRIM(source_hash)) = '';

    UPDATE {table}
       SET status = N'PENDING'
     WHERE status IS NULL
        OR LTRIM(RTRIM(status)) = N'';

    UPDATE {table}
       SET failure_count = 0
     WHERE failure_count IS NULL;

    UPDATE {table}
       SET updated_at = SYSUTCDATETIME()
     WHERE updated_at IS NULL;
    """

    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()


def acquire_db_lock(db: DbConnection, cfg: Config) -> bool:
    if not cfg.use_db_applock:
        return True
    cursor = db.cursor()
    try:
        cursor.execute(
            """
            DECLARE @result INT;
            EXEC @result = sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Session',
                @LockTimeout = 0;
            SELECT @result;
            """,
            "PatrolTranslateService",
        )
        value = cursor.fetchone()[0]
        return int(value) >= 0
    finally:
        cursor.close()


def _canonical_topic(column_name: str) -> str:
    value = column_name.strip().lower()

    suffixes = (
        "_japanese",
        "_jp",
        "_ja",
        "_vietnamese",
        "_vi",
    )

    for suffix in suffixes:
        if value.endswith(suffix):
            value = value[:-len(suffix)]
            break

    return value

def _date_column_for_pair(
    cfg: Config,
    source_column: str,
    target_column: str,
) -> str:
    source_topic = _canonical_topic(source_column)
    target_topic = _canonical_topic(target_column)

    date_column = cfg.date_column_map.get(source_topic)

    if date_column:
        return date_column

    date_column = cfg.date_column_map.get(target_topic)

    if date_column:
        return date_column

    return cfg.created_column

def _date_condition(
    date_column: str,
    date_from: Optional[date],
    date_to: Optional[date],
) -> tuple[str, list[Any]]:
    if date_from is None or date_to is None:
        return "", []

    start = datetime.combine(
        date_from,
        datetime.min.time(),
    )

    end = datetime.combine(
        date_to + timedelta(days=1),
        datetime.min.time(),
    )

    safe_date_column = _safe_column(date_column)

    return (
        f" AND p.{safe_date_column} >= ?"
        f" AND p.{safe_date_column} < ?",
        [start, end],
    )


def fetch_records_for_review(
    db: DbConnection,
    cfg: Config,
    pairs: Iterable[tuple[str, str]],
    date_from: Optional[date],
    date_to: Optional[date],
    pending_only: bool = False,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    table = _safe_table(cfg.table_name)
    id_col = _safe_column(cfg.id_column)
    qr_col = _safe_column(cfg.qr_key_column)

    output: list[dict[str, Any]] = []

    for source, target in pairs:
        source_col = _safe_column(source)
        target_col = _safe_column(target)

        date_column = _date_column_for_pair(
            cfg,
            source,
            target,
        )
        date_col = _safe_column(date_column)

        date_sql, date_params = _date_condition(
            date_column,
            date_from,
            date_to,
        )

        pending_sql = (
            f"""
            AND NULLIF(
                LTRIM(
                    RTRIM(
                        CONVERT(
                            NVARCHAR(MAX),
                            p.{target_col}
                        )
                    )
                ),
                ''
            ) IS NULL
            """
            if pending_only
            else ""
        )

        sql = f"""
        SELECT TOP ({int(limit)})
            CONVERT(
                NVARCHAR(128),
                p.{id_col}
            ) AS record_id,

            p.{date_col} AS created_at,

            CONVERT(
                NVARCHAR(255),
                p.{qr_col}
            ) AS qr_key,

            CONVERT(
                NVARCHAR(MAX),
                p.{source_col}
            ) AS source_text,

            CONVERT(
                NVARCHAR(MAX),
                p.{target_col}
            ) AS target_text

        FROM {table} p

        WHERE NULLIF(
            LTRIM(
                RTRIM(
                    CONVERT(
                        NVARCHAR(MAX),
                        p.{source_col}
                    )
                )
            ),
            ''
        ) IS NOT NULL

          {pending_sql}
          {date_sql}

        ORDER BY
            p.{date_col} DESC,
            p.{id_col} DESC
        """

        cursor = db.cursor()

        try:
            cursor.execute(sql, date_params)

            for row in cursor.fetchall():
                output.append({
                    "record_id": row.record_id,
                    "created_at": row.created_at,
                    "date_column": date_column,
                    "qr_key": normalize(row.qr_key) or "",
                    "source_column": source,
                    "target_column": target,
                    "source": normalize(row.source_text),
                    "target": normalize(row.target_text),
                })
        finally:
            cursor.close()

    return output

def fetch_pending(
    db: DbConnection,
    cfg: Config,
    source_column: str,
    target_column: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    selected_ids: Optional[Iterable[Any]] = None,
    batch_size: Optional[int] = None,
) -> list[dict[str, Any]]:
    table = _safe_table(cfg.table_name)
    state_table = _safe_table(cfg.work_state_table)

    id_col = _safe_column(cfg.id_column)
    qr_col = _safe_column(cfg.qr_key_column)
    source_col = _safe_column(source_column)
    target_col = _safe_column(target_column)

    date_column = _date_column_for_pair(
        cfg,
        source_column,
        target_column,
    )
    date_col = _safe_column(date_column)

    date_sql, params = _date_condition(
        date_column,
        date_from,
        date_to,
    )

    id_sql = ""
    ids = [str(value) for value in (selected_ids or [])]

    if ids:
        placeholders = ",".join("?" for _ in ids)
        id_sql = (
            f" AND CONVERT(NVARCHAR(128), p.{id_col})"
            f" IN ({placeholders})"
        )
        params.extend(ids)

    top = int(batch_size or cfg.batch_size)

    sql = f"""
    SELECT TOP ({top})
        CONVERT(
            NVARCHAR(128),
            p.{id_col}
        ) AS record_id,

        p.{date_col} AS created_at,

        CONVERT(
            NVARCHAR(255),
            p.{qr_col}
        ) AS qr_key,

        CONVERT(
            NVARCHAR(MAX),
            p.{source_col}
        ) AS source_text,

        CONVERT(
            NVARCHAR(MAX),
            p.{target_col}
        ) AS target_text

    FROM {table} p

    LEFT JOIN {state_table} ws
      ON ws.record_id =
         CONVERT(NVARCHAR(128), p.{id_col})
     AND ws.source_column = ?
     AND ws.target_column = ?

    WHERE NULLIF(
        LTRIM(
            RTRIM(
                CONVERT(
                    NVARCHAR(MAX),
                    p.{source_col}
                )
            )
        ),
        ''
    ) IS NOT NULL

      AND NULLIF(
        LTRIM(
            RTRIM(
                CONVERT(
                    NVARCHAR(MAX),
                    p.{target_col}
                )
            )
        ),
        ''
      ) IS NULL

      AND (
          ws.record_id IS NULL

          OR ws.source_hash <> CONVERT(
              VARCHAR(64),
              HASHBYTES(
                  'SHA2_256',
                  CONVERT(
                      VARBINARY(MAX),
                      CONVERT(
                          NVARCHAR(MAX),
                          p.{source_col}
                      )
                  )
              ),
              2
          )

          OR ws.next_retry_at IS NULL
          OR ws.next_retry_at <= SYSUTCDATETIME()
      )

      {date_sql}
      {id_sql}

    ORDER BY
        p.{date_col},
        p.{id_col}
    """

    cursor = db.cursor()

    try:
        cursor.execute(
            sql,
            [
                source_column,
                target_column,
                *params,
            ],
        )

        rows = []

        for row in cursor.fetchall():
            rows.append({
                "record_id": row.record_id,
                "created_at": row.created_at,
                "date_column": date_column,
                "qr_key": normalize(row.qr_key) or "",
                "source_column": source_column,
                "target_column": target_column,
                "source": normalize(row.source_text),
                "target": normalize(row.target_text),
            })

        return rows

    finally:
        cursor.close()

def update_target(
    db: DbConnection,
    cfg: Config,
    record_id: Any,
    target_column: str,
    translated_text: str,
) -> bool:
    table = _safe_table(cfg.table_name)
    id_col = _safe_column(cfg.id_column)
    target_col = _safe_column(target_column)

    update_stamp = ""
    if cfg.ai_translate_update_column:
        update_stamp = (
            f", {_safe_column(cfg.ai_translate_update_column)} = SYSUTCDATETIME()"
        )

    sql = f"""
    UPDATE {table}
       SET {target_col} = ? {update_stamp}
     WHERE CONVERT(NVARCHAR(128), {id_col}) = ?
       AND NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {target_col}))), '') IS NULL
    """
    cursor = db.cursor()
    try:
        cursor.execute(sql, translated_text, str(record_id))
        changed = cursor.rowcount > 0
        db.commit()
        return changed
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()



def read_target_value(
    db: DbConnection,
    cfg: Config,
    record_id: Any,
    target_column: str,
) -> tuple[bool, Optional[str]]:
    """
    Đọc lại target ngay sau khi UPDATE trả rowcount=0.

    Trả về:
      - (False, None): không còn record trong DB.
      - (True, None): record còn tồn tại nhưng target vẫn trống.
      - (True, value): target đã có dữ liệu (có thể do tiến trình khác ghi).
    """
    table = _safe_table(cfg.table_name)
    id_col = _safe_column(cfg.id_column)
    target_col = _safe_column(target_column)

    sql = f"""
    SELECT TOP (1)
        CONVERT(NVARCHAR(MAX), {target_col}) AS target_text
    FROM {table}
    WHERE CONVERT(NVARCHAR(128), {id_col}) = ?
    """

    cursor = db.cursor()
    try:
        cursor.execute(sql, str(record_id))
        row = cursor.fetchone()
        if row is None:
            return False, None
        return True, normalize(row.target_text)
    finally:
        cursor.close()

def batch_update_target(
    db: DbConnection,
    cfg: Config,
    target_column: str,
    updates: Iterable[tuple[str, Any]],
) -> int:
    count = 0
    for translated_text, record_id in updates:
        if update_target(db, cfg, record_id, target_column, translated_text):
            count += 1
    return count


def _source_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clear_work_state(
    db: DbConnection,
    cfg: Config,
    record_id: Any,
    source_column: str,
    target_column: str,
) -> None:
    table = _safe_table(cfg.work_state_table)
    cursor = db.cursor()
    try:
        cursor.execute(
            f"""
            DELETE FROM {table}
             WHERE record_id = ?
               AND source_column = ?
               AND target_column = ?
            """,
            str(record_id),
            source_column,
            target_column,
        )
        db.commit()
    finally:
        cursor.close()


def defer_failed_record(
    db: DbConnection,
    cfg: Config,
    record_id: Any,
    source_column: str,
    target_column: str,
    source_text: str,
    error: str,
) -> None:
    table = _safe_table(cfg.work_state_table)
    next_retry = datetime.utcnow() + timedelta(seconds=cfg.failure_retry_seconds)
    cursor = db.cursor()
    try:
        cursor.execute(
            f"""
            MERGE {table} AS target
            USING (
                SELECT
                    ? AS record_id,
                    ? AS source_column,
                    ? AS target_column
            ) AS source
            ON target.record_id = source.record_id
               AND target.source_column = source.source_column
               AND target.target_column = source.target_column
            WHEN MATCHED THEN
                UPDATE SET
                    source_hash = ?,
                    status = 'FAILED',
                    failure_count = target.failure_count + 1,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (
                    record_id, source_column, target_column, source_hash,
                    status, failure_count, next_retry_at, last_error, updated_at
                )
                VALUES (?, ?, ?, ?, 'FAILED', 1, ?, ?, SYSUTCDATETIME());
            """,
            str(record_id), source_column, target_column,
            _source_hash(source_text), next_retry, error[:1800],
            str(record_id), source_column, target_column,
            _source_hash(source_text), next_retry, error[:1800],
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()


# ============================================================
# Cache and LLM client
# ============================================================

class TranslationCache:
    REQUIRED_COLUMNS = {
        "cache_key",
        "source_text",
        "target_language",
        "translated_text",
        "created_at",
    }

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(
            path,
            check_same_thread=False,
            timeout=30,
        )
        # WAL giúp giảm khóa DB khi service vừa đọc vừa ghi cache liên tục.
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._ensure_schema()

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _columns(self, table_name: str) -> set[str]:
        return {
            str(row[1])
            for row in self.conn.execute(
                f'PRAGMA table_info("{table_name}")'
            ).fetchall()
        }

    def _create_table(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS translations (
                cache_key TEXT PRIMARY KEY,
                source_text TEXT NOT NULL,
                target_language TEXT NOT NULL,
                translated_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_translations_language_created
            ON translations(target_language, created_at)
            """
        )
        self.conn.commit()

    def _ensure_schema(self) -> None:
        """
        Nếu cache cũ thiếu cột, giữ lại dưới tên legacy và tạo cache mới.
        (Cache dịch không mang tính trạng thái nghiệp vụ then chốt, nên
        backup-rename ở đây chấp nhận được và giữ code đơn giản.)
        """
        if not self._table_exists("translations"):
            self._create_table()
            return

        columns = self._columns("translations")
        if self.REQUIRED_COLUMNS.issubset(columns):
            self._create_table()
            return

        suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        legacy_name = f"translations_legacy_{suffix}"
        counter = 1

        while self._table_exists(legacy_name):
            legacy_name = f"translations_legacy_{suffix}_{counter}"
            counter += 1

        self.conn.execute(
            f'ALTER TABLE "translations" RENAME TO "{legacy_name}"'
        )
        self.conn.commit()
        self._create_table()

        log.warning(
            "Cache SQLite cũ đã được đổi tên thành %s; "
            "đã tạo bảng translations mới.",
            legacy_name,
        )

    @staticmethod
    def _key(source_text: str, target_language: str) -> str:
        raw = f"{target_language}\n{source_text}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def get(self, source_text: str, target_language: str) -> Optional[str]:
        try:
            row = self.conn.execute(
                """
                SELECT translated_text
                FROM translations
                WHERE cache_key = ?
                """,
                (self._key(source_text, target_language),),
            ).fetchone()
            return normalize(row[0]) if row else None
        except sqlite3.DatabaseError as exc:
            log.warning("Không đọc được cache, bỏ qua cache: %s", exc)
            return None

    def put(
        self,
        source_text: str,
        target_language: str,
        translated_text: str,
    ) -> None:
        try:
            self.conn.execute(
                """
                INSERT INTO translations (
                    cache_key,
                    source_text,
                    target_language,
                    translated_text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    source_text = excluded.source_text,
                    target_language = excluded.target_language,
                    translated_text = excluded.translated_text,
                    created_at = excluded.created_at
                """,
                (
                    self._key(source_text, target_language),
                    source_text,
                    target_language,
                    translated_text,
                    datetime.utcnow().isoformat(timespec="seconds"),
                ),
            )
            self.conn.commit()
        except sqlite3.DatabaseError as exc:
            log.warning("Không ghi được cache, tiếp tục không dùng cache: %s", exc)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class LlmClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()

        retry = Retry(
            total=cfg.llm_retry_count,
            connect=cfg.llm_retry_count,
            read=0,
            status=cfg.llm_retry_count,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.cfg.lm_api_key:
            headers["Authorization"] = f"Bearer {self.cfg.lm_api_key}"
        return headers

    def _prompt(self, source_text: str, target_language: str) -> list[dict[str, str]]:
        if target_language == "ja":
            system = (
                "Bạn là biên dịch viên HSE nhà máy. "
                "Dịch chính xác nội dung tiếng Việt sang tiếng Nhật tự nhiên, "
                "ngắn gọn, giữ nguyên tên máy, mã, số liệu và ký hiệu. "
                "Chỉ trả về bản dịch, không giải thích, không suy luận. "
                "Nếu văn bản là mã, viết tắt, số hiệu hoặc không cần dịch, "
                "hãy trả về chính văn bản gốc nguyên vẹn — TUYỆT ĐỐI KHÔNG "
                "được để trống câu trả lời."
            )
        else:
            system = (
                "Bạn là biên dịch viên HSE nhà máy. "
                "Dịch chính xác nội dung tiếng Nhật sang tiếng Việt tự nhiên, "
                "ngắn gọn, giữ nguyên tên máy, mã, số liệu và ký hiệu. "
                "Chỉ trả về bản dịch, không giải thích, không suy luận. "
                "Nếu văn bản là mã, viết tắt, số hiệu hoặc không cần dịch, "
                "hãy trả về chính văn bản gốc nguyên vẹn — TUYỆT ĐỐI KHÔNG "
                "được để trống câu trả lời."
            )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": source_text},
        ]

    def translate(self, source_text: str, target_language: str) -> str:
        """
        Gọi LLM và trích xuất nội dung dịch.

        Một số server OpenAI-compatible (đặc biệt các model dạng reasoning
        như gpt-oss) có thể trả `content` dưới dạng list thay vì string,
        hoặc đặt nội dung thực tế trong `reasoning_content` / `reasoning`
        thay vì `content`. Vì vậy cần thử qua nhiều field theo thứ tự ưu
        tiên thay vì chỉ đọc `message.content` như một chuỗi đơn thuần.
        """
        payload = {
            "model": self.cfg.lm_model,
            "messages": self._prompt(source_text, target_language),
            "temperature": 0.1,
            "max_tokens": self.cfg.max_output_tokens,
            "stream": False,
        }

        last_error: Optional[Exception] = None
        attempts = self.cfg.llm_retry_count + 1
        for attempt in range(1, attempts + 1):
            started = time.monotonic()
            try:
                response = self.session.post(
                    f"{self.cfg.lm_url}/v1/chat/completions",
                    headers=self._headers(),
                    json=payload,
                    timeout=(
                        self.cfg.connect_timeout_seconds,
                        self.cfg.request_timeout_seconds,
                    ),
                )
                elapsed = time.monotonic() - started
                if response.status_code >= 400:
                    raise RuntimeError(
                        f"LLM HTTP {response.status_code}: "
                        f"{response.text[:500]}"
                    )

                body = response.json()
                log.info(body)
                choices = body.get("choices")
                if not isinstance(choices, list) or not choices:
                    raise RuntimeError("LLM không trả về choices[0].")

                choice = choices[0] if isinstance(choices[0], dict) else {}
                message = choice.get("message") or {}

                raw_content = message.get("content")

                # Một số OpenAI-compatible server trả content dạng list.
                if isinstance(raw_content, list):
                    parts: list[str] = []
                    for item in raw_content:
                        if isinstance(item, str):
                            parts.append(item)
                        elif isinstance(item, dict):
                            value = (
                                item.get("text")
                                or item.get("content")
                                or item.get("value")
                            )
                            if value:
                                parts.append(str(value))
                    raw_content = "\n".join(parts)

                # CHỦ Ý KHÔNG fallback sang message["reasoning"] /
                # message["reasoning_content"]. Với các model reasoning
                # (vd. gpt-oss-20b), trường này chứa chuỗi suy luận nội bộ
                # bằng tiếng Anh (vd. 'User says "ok". Probably no
                # translation needed...') chứ KHÔNG phải bản dịch — nếu
                # dùng làm fallback sẽ có nguy cơ ghi thẳng đoạn suy luận
                # đó vào cột dịch trong DB. Chỉ chấp nhận các trường thực
                # sự chứa nội dung trả lời cuối cùng.
                candidates = [
                    raw_content,
                    choice.get("text"),
                    body.get("output_text"),
                    body.get("response"),
                ]

                content = None
                for candidate in candidates:
                    if candidate is None:
                        continue
                    cleaned = _clean_translation(str(candidate))
                    if cleaned:
                        content = cleaned
                        break

                if not content:
                    preview = str(body)[:800]
                    raise RuntimeError(
                        "LLM không trả về nội dung dịch hợp lệ "
                        "(content rỗng hoặc chỉ chứa suy luận nội bộ, "
                        "không phải bản dịch). "
                        f"Response preview: {preview}"
                    )

                log.info("LLM translated in %.2fs", elapsed)
                return content
            except Exception as exc:
                last_error = exc
                log.warning(
                    "LLM attempt %s/%s failed: %s",
                    attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(self.cfg.llm_retry_delay_seconds * attempt)

        raise RuntimeError(f"Dịch thất bại: {last_error}")

    def close(self) -> None:
        self.session.close()


_KEEP_AS_IS_TOKENS = {
    "OK",
    "OKE",
    "NG",
    "N/A",
    "NA",
    "PLC",
    "USB",
    "HSE",
    "QC",
    "QA",
    "AI",
    "QR",
    "QR CODE",
    "MCCB",
    "MCB",
    "DB",
}

_KEEP_AS_IS_PATTERN = re.compile(r"^[A-Za-z0-9_./+\-#() ]+$")


def _is_keep_as_is_text(text: str) -> bool:
    """
    Nhận diện văn bản là mã kỹ thuật/viết tắt/ký hiệu không cần dịch:
    - Khớp danh sách token cố định (OK, PLC, HSE, ...)
    - Chuỗi rất ngắn (<=5 ký tự) chỉ gồm chữ/số/ký hiệu kỹ thuật
    - Chuỗi toàn chữ hoa/số/ký hiệu kỹ thuật (mã máy, số hiệu)

    Dùng để bỏ qua việc gọi LLM hoàn toàn cho các trường hợp rõ ràng,
    tránh vừa tốn chi phí gọi model vừa rủi ro model trả lời sai định dạng.
    """
    clean = text.strip()
    if not clean:
        return False

    upper = clean.upper()
    if upper in _KEEP_AS_IS_TOKENS:
        return True

    if len(clean) <= 5 and _KEEP_AS_IS_PATTERN.fullmatch(clean):
        return True

    if re.fullmatch(r"[A-Z0-9_./+\-#() ]+", clean):
        return True

    return False


def _can_keep_unchanged(source: str, translated: str) -> bool:
    """
    Kiểm tra sau khi đã có kết quả dịch: nếu source và translated giống hệt
    nhau (không phân biệt hoa/thường) VÀ source thuộc dạng không cần dịch,
    thì chấp nhận kết quả "giữ nguyên" thay vì coi là lỗi.
    """
    if source.strip().casefold() != translated.strip().casefold():
        return False
    return _is_keep_as_is_text(source)


def translate_text(
    source_text: Any,
    target_column: str,
    client: LlmClient,
    cache: TranslationCache,
) -> str:
    source = normalize(source_text)
    if not source:
        raise ValueError("Nội dung nguồn trống.")

    target_language = "ja" if _is_japanese_column(target_column) else "vi"

    # ========================================================
    # 1. Raw đã có sẵn cả phần Việt và phần Nhật
    #    -> lấy đúng phần theo ngôn ngữ của cột đích
    #    -> tuyệt đối không gọi LLM
    #
    # Ví dụ raw:
    #     OK
    #     了解しました。
    #
    # target=comment    -> OK
    # target=comment_jp -> 了解しました。
    # ========================================================
    existing_target = _extract_existing_target_text(
        source,
        target_language,
    )

    if existing_target:
        log.info(
            "Reuse bilingual raw without LLM: target=%s result=%r",
            target_language,
            existing_target[:300],
        )
        cache.put(
            source,
            target_language,
            existing_target,
        )
        return existing_target

    # ========================================================
    # 2. Mã kỹ thuật / viết tắt / token ngắn
    #    -> giữ nguyên, không gọi LLM
    # ========================================================
    if _is_keep_as_is_text(source):
        cache.put(source, target_language, source)
        return source

    # ========================================================
    # 3. Cache chỉ được kiểm tra sau bước tách raw song ngữ.
    #    Nhờ vậy cache cũ không thể ép service dùng lại một bản dịch LLM
    #    khi raw hiện tại đã có sẵn đúng ngôn ngữ đích.
    # ========================================================
    cached = cache.get(source, target_language)
    if cached:
        return cached

    # ========================================================
    # 4. Raw chưa có phần ngôn ngữ đích -> mới gọi LLM
    # ========================================================
    translated = client.translate(source, target_language)

    if (
        target_language == "ja"
        and not _has_japanese(translated)
        and not _can_keep_unchanged(source, translated)
    ):
        raise RuntimeError(
            "Bản dịch tiếng Nhật không chứa ký tự tiếng Nhật."
        )

    if (
        target_language == "vi"
        and _has_japanese(translated)
    ):
        raise RuntimeError(
            "Bản dịch tiếng Việt vẫn còn chứa nội dung tiếng Nhật."
        )

    if (
        target_language == "vi"
        and translated.strip().casefold() == source.strip().casefold()
        and not _can_keep_unchanged(source, translated)
    ):
        raise RuntimeError(
            "LLM trả lại nguyên văn cho nội dung cần dịch."
        )

    cache.put(source, target_language, translated)
    return translated


# ============================================================
# Processing engine
# ============================================================

@dataclass
class ProcessResult:
    scanned: int = 0
    translated: int = 0
    skipped: int = 0
    failed: int = 0
    stopped: bool = False


def process_pairs(
    cfg: Config,
    pairs: Iterable[tuple[str, str]],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    selected_ids: Optional[Iterable[Any]] = None,
    stop_event: Any = None,
    progress_callback: Any = None,
) -> ProcessResult:
    db: Optional[DbConnection] = None
    cache: Optional[TranslationCache] = None
    client: Optional[LlmClient] = None
    result = ProcessResult()

    try:
        db = DbConnection(cfg)
        db.connect()
        ensure_work_state_table(db, cfg)

        cache = TranslationCache(cfg.cache_db_file)
        client = LlmClient(cfg)

        for source_column, target_column in pairs:
            # Với chế độ "chỉ dịch dòng đã chọn", giữ một tập ID còn lại.
            # Sau mỗi batch, loại các ID đã quét khỏi tập này để không bỏ sót
            # khi số dòng chọn lớn hơn BATCH_SIZE và cũng tránh vòng lặp vô
            # hạn ở DRY_RUN (target không được ghi DB).
            remaining_selected_ids: Optional[set[str]] = (
                {str(value) for value in selected_ids}
                if selected_ids is not None
                else None
            )

            while True:
                if stop_event is not None and stop_event.is_set():
                    result.stopped = True
                    return result

                if remaining_selected_ids is not None and not remaining_selected_ids:
                    break

                ids_for_query = (
                    remaining_selected_ids
                    if remaining_selected_ids is not None
                    else None
                )

                rows = fetch_pending(
                    db=db,
                    cfg=cfg,
                    source_column=source_column,
                    target_column=target_column,
                    date_from=date_from,
                    date_to=date_to,
                    selected_ids=ids_for_query,
                    batch_size=cfg.batch_size,
                )
                if not rows:
                    break

                if remaining_selected_ids is not None:
                    remaining_selected_ids.difference_update(
                        str(row["record_id"]) for row in rows
                    )

                for row in rows:
                    if stop_event is not None and stop_event.is_set():
                        result.stopped = True
                        return result

                    result.scanned += 1
                    record_id = row["record_id"]
                    source_text = row["source"] or ""

                    if progress_callback:
                        progress_callback("start", row, result)

                    try:
                        translated = translate_text(
                            source_text,
                            target_column,
                            client,
                            cache,
                        )

                        if cfg.dry_run:
                            changed = True
                        else:
                            changed = update_target(
                                db, cfg, record_id, target_column, translated
                            )

                        if changed:
                            clear_work_state(
                                db, cfg, record_id, source_column, target_column
                            )
                            result.translated += 1
                            if progress_callback:
                                progress_callback(
                                    "success",
                                    {**row, "translated": translated},
                                    result,
                                )
                        else:
                            # rowcount=0 chưa đủ để kết luận target đã có dữ liệu.
                            # Đọc lại DB để phân biệt chính xác:
                            #   1) Target đã được process khác ghi -> skipped hợp lệ.
                            #   2) Record bị xóa -> lỗi.
                            #   3) Record vẫn còn nhưng target vẫn trống -> lỗi UPDATE.
                            record_exists, current_target = read_target_value(
                                db,
                                cfg,
                                record_id,
                                target_column,
                            )

                            if current_target:
                                clear_work_state(
                                    db, cfg, record_id, source_column, target_column
                                )
                                result.skipped += 1
                                if progress_callback:
                                    progress_callback(
                                        "skipped",
                                        {
                                            **row,
                                            "target": current_target,
                                            "skip_reason": "TARGET_ALREADY_EXISTS",
                                            "note": "Target đã có dữ liệu trong DB",
                                        },
                                        result,
                                    )
                            elif not record_exists:
                                raise RuntimeError(
                                    f"Không thể UPDATE vì record id={record_id} "
                                    "không còn tồn tại trong DB."
                                )
                            else:
                                raise RuntimeError(
                                    f"UPDATE target thất bại cho id={record_id}, "
                                    f"cột={target_column}; target vẫn đang trống."
                                )
                    except Exception as exc:
                        result.failed += 1
                        try:
                            defer_failed_record(
                                db,
                                cfg,
                                record_id,
                                source_column,
                                target_column,
                                source_text,
                                str(exc),
                            )
                        except Exception:
                            log.exception("Không lưu được trạng thái lỗi.")
                        if progress_callback:
                            progress_callback(
                                "failed",
                                {**row, "error": str(exc)},
                                result,
                            )

        return result
    finally:
        if client is not None:
            client.close()
        if cache is not None:
            cache.close()
        if db is not None:
            db.close()


def run_service() -> None:
    global log
    cfg = load_config()
    log = setup_logging(cfg)

    db = DbConnection(cfg)
    try:
        db.connect()
        ensure_work_state_table(db, cfg)
        if not acquire_db_lock(db, cfg):
            raise RuntimeError(
                "Một service khác đang chạy và giữ DB application lock."
            )
        log.info("Patrol translate service started.")
        log.info("Pairs: %s", cfg.translate_columns)

        while True:
            try:
                result = process_pairs(cfg, cfg.translate_columns)
                log.info(
                    "Cycle complete: scanned=%s translated=%s skipped=%s failed=%s",
                    result.scanned,
                    result.translated,
                    result.skipped,
                    result.failed,
                )
            except KeyboardInterrupt:
                break
            except Exception:
                log.exception("Service cycle failed.")
            time.sleep(cfg.poll_interval_seconds)
    finally:
        db.close()
        log.info("Patrol translate service stopped.")


if __name__ == "__main__":
    run_service()