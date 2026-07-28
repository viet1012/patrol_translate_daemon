"""
patrol_translate_service.py  (v2)

Background service that watches the F2_Patrol_Report table and, for every
new/edited record whose `comment` column is NOT Japanese, calls a local LLM
(LM Studio / any OpenAI-compatible /v1/chat/completions endpoint) to
translate it into Japanese, then writes the translation back into the
`comment` column.

Translation logic (prompts, output cleanup, validation, retry policy) is
UNCHANGED from v1 and mirrors the Java `PatrolCommentService` exactly.

v2 architecture changes:
  1. DB layer      - one persistent pyodbc connection, reused across polls,
                      with automatic ping + reconnect instead of open/close
                      every cycle.
  2. Checkpoint     - watermark is (COALESCE(edit_date, created_at), id),
                      so a row that gets edited after being scanned once
                      will be picked up again.
  3. Translation    - a local SQLite cache (hash of normalized text + source
     cache             language -> translation) avoids calling the LLM again
                      for text it has already translated.
  4. Batch update   - translations for a whole poll batch are written with
                      one executemany() + a single commit(), not per row.
  5. Logging        - TimedRotatingFileHandler, rotates at midnight, keeps
                      14 days of logs, plus console output.
  6. Single instance - guarded two ways: (a) a local file lock so a second
                      process on the same machine refuses to start, and
                      (b) sp_getapplock on the persistent DB connection so
                      a second process on a *different* machine pointing at
                      the same DB also refuses to start.
  7. HTTP client    - requests.Session with HTTPAdapter + urllib3 Retry for
                      429/500/502/503/504, with backoff.

--------------------------------------------------------------------
Configuration (environment variables). Required: DB_SERVER, DB_DATABASE.

  DB_DRIVER              default "{ODBC Driver 17 for SQL Server}"
  DB_SERVER
  DB_DATABASE
  DB_USER / DB_PASSWORD  (omit both if DB_TRUSTED_CONNECTION=yes)
  DB_TRUSTED_CONNECTION  "yes" for Windows auth

  TABLE_NAME             default "F2_Patrol_Report"
  ID_COLUMN              default "id"
  CREATED_COLUMN         default "created_at"
  EDITED_COLUMN          default "edit_date"
  SOURCE_COLUMN          default "comment"

  LM_URL                 default "http://192.168.122.16:1234"
  LM_API_KEY             default ""
  LM_MODEL               default "openai/gpt-oss-20b"

  POLL_INTERVAL_SECONDS  default 5
  BATCH_SIZE             default 20
  STATE_FILE             default "./patrol_translate_state.json"
  CACHE_DB_FILE          default "./patrol_translate_cache.sqlite3"
  LOCK_FILE              default "./patrol_translate.lock"
  LOG_DIR                default "./logs"
  LOG_FILE_NAME          default "patrol_translate.log"
  LOG_BACKUP_DAYS        default 14
  USE_DB_APPLOCK         default "1"  (also try sp_getapplock, best-effort)
  HTTP_MAX_RETRIES       default 3
  HTTP_BACKOFF_FACTOR    default 0.5
  DRY_RUN                "1" to log without writing to DB

Requires: pyodbc, requests
--------------------------------------------------------------------
"""

from __future__ import annotations

import hashlib
import json
import logging
import logging.handlers
import os
import re
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter

from dotenv import load_dotenv
# Đọc file .env
load_dotenv(override=True)

try:
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

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
    edited_column: str
    source_column: str
    japanese_column: str
    ai_translate_update_column: str

    lm_url: str
    lm_api_key: Optional[str]
    lm_model: str

    poll_interval_seconds: float
    batch_size: int
    state_file: Path
    cache_db_file: Path
    lock_file: Path
    log_dir: Path
    log_file_name: str
    log_backup_days: int
    use_db_applock: bool
    http_max_retries: int
    http_backoff_factor: float
    dry_run: bool

    connect_timeout_seconds: float = 10
    request_timeout_seconds: float = 90
    max_output_tokens: int = 300
    max_retry: int = 1  # retry policy for a plain timeout (kept from v1)


def load_config() -> Config:
    def env(name: str, default: Optional[str] = None) -> Optional[str]:
        value = os.environ.get(name)
        return value if value not in (None, "") else default

    return Config(
        db_driver=env("DB_DRIVER", "{ODBC Driver 17 for SQL Server}"),
        db_server=env("DB_SERVER", ""),
        db_database=env("DB_DATABASE", ""),
        db_user=env("DB_USER"),
        db_password=env("DB_PASSWORD"),
        db_trusted_connection=(env("DB_TRUSTED_CONNECTION", "no") or "no").lower() in ("1", "yes", "true"),
        table_name=env("TABLE_NAME", "F2_Patrol_Report"),
        id_column=env("ID_COLUMN", "id"),
        created_column=env("CREATED_COLUMN", "created_at"),
        edited_column=env("EDITED_COLUMN", "edit_date"),
        source_column=env("SOURCE_COLUMN", "comment"),
        japanese_column=env("JAPANESE_COLUMN", "comment_japanese"),
        ai_translate_update_column=env("AI_TRANSLATE_UPDATE_COLUMN", "ai_translate_update_at"),
        lm_url=(env("LM_URL", "http://192.168.122.16:1234") or "").rstrip("/"),
        lm_api_key=env("LM_API_KEY"),
        lm_model=env("LM_MODEL", "openai/gpt-oss-20b"),
        poll_interval_seconds=float(env("POLL_INTERVAL_SECONDS", "5")),
        batch_size=int(env("BATCH_SIZE", "20")),
        state_file=Path(env("STATE_FILE", "./patrol_translate_state.json")),
        cache_db_file=Path(env("CACHE_DB_FILE", "./patrol_translate_cache.sqlite3")),
        lock_file=Path(env("LOCK_FILE", "./patrol_translate.lock")),
        log_dir=Path(env("LOG_DIR", "./logs")),
        log_file_name=env("LOG_FILE_NAME", "patrol_translate.log"),
        log_backup_days=int(env("LOG_BACKUP_DAYS", "14")),
        use_db_applock=(env("USE_DB_APPLOCK", "1") or "1") in ("1", "true", "True"),
        http_max_retries=int(env("HTTP_MAX_RETRIES", "3")),
        http_backoff_factor=float(env("HTTP_BACKOFF_FACTOR", "0.5")),
        dry_run=(env("DRY_RUN", "0") or "0") in ("1", "true", "True"),
    )


# --------------------------------------------------------------------------
# Logging (#5 - TimedRotatingFileHandler)
# --------------------------------------------------------------------------

def setup_logging(cfg: Config) -> logging.Logger:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = cfg.log_dir / cfg.log_file_name

    logger = logging.getLogger("patrol_translate")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(log_path),
        when="midnight",
        backupCount=cfg.log_backup_days,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


log = logging.getLogger("patrol_translate")  # configured by setup_logging() in run()


def abbreviate(value: Optional[str], max_length: int) -> Optional[str]:
    if value is None:
        return None
    return value if len(value) <= max_length else value[:max_length] + "..."


# --------------------------------------------------------------------------
# Language helpers (UNCHANGED from v1 / PatrolCommentService)
# --------------------------------------------------------------------------

def normalize(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.replace("\u00A0", " ").strip()
    return normalized if normalized else None


def is_hiragana(cp: int) -> bool:
    return 0x3040 <= cp <= 0x309F


def is_katakana(cp: int) -> bool:
    return 0x30A0 <= cp <= 0x30FF


def is_cjk(cp: int) -> bool:
    return 0x4E00 <= cp <= 0x9FFF


def contains_japanese(value: Optional[str]) -> bool:
    if not value:
        return False
    return any(is_hiragana(ord(c)) or is_katakana(ord(c)) or is_cjk(ord(c)) for c in value)


def is_mostly_japanese(value: Optional[str]) -> bool:
    if not value:
        return False
    letters = [c for c in value if c.isalpha()]
    if not letters:
        return False
    japanese_letters = [c for c in letters if is_hiragana(ord(c)) or is_katakana(ord(c)) or is_cjk(ord(c))]
    return (len(japanese_letters) * 100 // len(letters)) >= 60


def contains_latin_letter(value: Optional[str]) -> bool:
    if not value:
        return False
    return any(
        c.isalpha() and not (is_hiragana(ord(c)) or is_katakana(ord(c)) or is_cjk(ord(c)))
        for c in value
    )


def should_translate(text: Optional[str]) -> bool:
    value = normalize(text)
    if value is None:
        return False
    return any(c.isalpha() for c in value)


def is_valid_translation(original: str, translated: Optional[str], source_is_japanese: bool) -> bool:
    source = normalize(original)
    target = normalize(translated)
    if source is None or target is None:
        return False
    if source.casefold() == target.casefold():
        return False
    if source_is_japanese:
        return contains_latin_letter(target) and not is_mostly_japanese(target)
    return contains_japanese(target)


_LABEL_PREFIX_RE = re.compile(
    r"^(translation|translated text|japanese|vietnamese|bản dịch|dịch)\s*:\s*",
    re.IGNORECASE,
)


def clean_model_output(value: str) -> Optional[str]:
    result = value.replace("```text", "").replace("```json", "").replace("```", "").strip()
    result = _LABEL_PREFIX_RE.sub("", result).strip()

    if len(result) >= 2:
        if (result.startswith('"') and result.endswith('"')) or (
            result.startswith("'") and result.endswith("'")
        ):
            result = result[1:-1].strip()

    return normalize(result)


# --------------------------------------------------------------------------
# Prompts (UNCHANGED from v1)
# --------------------------------------------------------------------------

def vietnamese_to_japanese_prompt() -> str:
    return """You are a professional Vietnamese-to-Japanese translator
for factory safety patrols, 5S audits, and manufacturing reports.

Mandatory rules:

1. Translate the input into natural, professional Japanese
   used in Japanese manufacturing factories.

2. The input can be Vietnamese, English, or mixed Vietnamese-English.

3. Vietnamese without proper diacritics must be interpreted
   and corrected according to the safety context before translation.

4. Correct obvious Vietnamese spelling mistakes based on context.
   Example: "do nga" in a safety report means "do nga" (fell over).

5. Preserve factory abbreviations and identifiers such as:
   MTC, PLC, HSE, QA, QR, 5S, machine codes, area codes,
   equipment codes, model names, and part numbers.

6. Preserve technical meanings related to safety, machinery,
   production, tools, work areas, risk levels, falling,
   overturning, slipping, collision, electric shock,
   fire, leakage, and mechanical hazards.

7. Never return the original Vietnamese or English sentence unchanged.

8. Return only the final Japanese translation.

9. Do not return JSON, Markdown, explanations, labels,
   language names, quotes, or the original text.
"""


def japanese_to_vietnamese_prompt() -> str:
    return """You are a professional Japanese-to-Vietnamese translator
for factory safety patrols, 5S audits, and manufacturing reports.

Mandatory rules:

1. Translate the Japanese input into natural Vietnamese
   with correct diacritics.

2. Preserve factory abbreviations and identifiers such as:
   MTC, PLC, HSE, QA, QR, 5S, machine codes, area codes,
   equipment codes, model names, and part numbers.

3. Preserve the technical meaning used in safety,
   machinery, manufacturing, production, and 5S reports.

4. Never return the original Japanese sentence unchanged.

5. Return only the final Vietnamese translation.

6. Do not return JSON, Markdown, explanations, labels,
   language names, quotes, or the original text.
"""


# --------------------------------------------------------------------------
# Translation cache (#3 - SQLite)
# --------------------------------------------------------------------------

class TranslationCache:
    """SQLite-backed cache of source-text -> translated-text, keyed by a
    hash of (source language flag + normalized source text). Avoids paying
    for an LLM call when the exact same comment text has been seen before
    (common with template phrases in safety patrol reports)."""

    def __init__(self, db_file: Path):
        self.db_file = db_file
        self.conn = sqlite3.connect(str(db_file))
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS translations (
                source_hash TEXT PRIMARY KEY,
                source_lang TEXT NOT NULL,
                source_text TEXT NOT NULL,
                target_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    @staticmethod
    def _key(source_text: str, source_is_japanese: bool) -> str:
        lang_flag = "ja" if source_is_japanese else "vi"
        payload = f"{lang_flag}:{source_text}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def get(self, source_text: str, source_is_japanese: bool) -> Optional[str]:
        key = self._key(source_text, source_is_japanese)
        row = self.conn.execute(
            "SELECT target_text FROM translations WHERE source_hash = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def put(self, source_text: str, source_is_japanese: bool, target_text: str) -> None:
        key = self._key(source_text, source_is_japanese)
        self.conn.execute(
            """
            INSERT INTO translations (source_hash, source_lang, source_text, target_text, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_hash) DO UPDATE SET
                target_text = excluded.target_text,
                created_at = excluded.created_at
            """,
            (key, "ja" if source_is_japanese else "vi", source_text, target_text, datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# LLM client (#7 - Session + HTTPAdapter + Retry)
# --------------------------------------------------------------------------

class LlmClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()

        retry = Retry(
            total=cfg.http_max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["POST"]),
            backoff_factor=cfg.http_backoff_factor,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _build_payload(self, text: str, source_is_japanese: bool) -> dict:
        system_prompt = japanese_to_vietnamese_prompt() if source_is_japanese else vietnamese_to_japanese_prompt()
        return {
            "model": self.cfg.lm_model,
            "temperature": 0.1,
            "max_tokens": self.cfg.max_output_tokens,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
        }

    def _call_once(self, text: str, source_is_japanese: bool) -> Optional[str]:
        payload = self._build_payload(text, source_is_japanese)
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.cfg.lm_api_key:
            headers["Authorization"] = f"Bearer {self.cfg.lm_api_key}"

        url = f"{self.cfg.lm_url}/v1/chat/completions"
        started = time.monotonic()

        # Session-level Retry already handles 429/5xx with backoff; this call
        # only needs to additionally handle connect/read timeouts (below).
        response = self.session.post(
            url,
            json=payload,
            headers=headers,
            timeout=(self.cfg.connect_timeout_seconds, self.cfg.request_timeout_seconds),
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)

        if response.status_code == 200:
            result = self._extract_content(response.text)
            log.info(
                "[TRANSLATE] Completed in %d ms. inputLength=%d, outputLength=%d",
                elapsed_ms, len(text), len(result) if result else 0,
            )
            return result

        if response.status_code == 429:
            log.warning("[TRANSLATE] LLM rate limited after retries. status=429, elapsedMs=%d", elapsed_ms)
            return None

        log.warning(
            "[TRANSLATE] LLM returned error. status=%d, elapsedMs=%d, body=%s",
            response.status_code, elapsed_ms, abbreviate(response.text, 600),
        )
        return None

    def _extract_content(self, response_body: str) -> Optional[str]:
        if not response_body or not response_body.strip():
            return None
        try:
            root = json.loads(response_body)
        except json.JSONDecodeError:
            log.warning("[TRANSLATE] Could not parse LLM response body as JSON.")
            return None

        try:
            content = root["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            log.warning(
                "[TRANSLATE] Response missing choices[0].message.content. body=%s",
                abbreviate(response_body, 600),
            )
            return None

        content = normalize(content)
        if content is None:
            return None
        return clean_model_output(content)

    def translate_with_retry(self, text: str, source_is_japanese: bool) -> Optional[str]:
        """Retry policy for connect/read TIMEOUTS specifically (kept identical
        to v1 / Java MAX_RETRY=1). 429/5xx are already retried by the Session's
        urllib3.Retry adapter above."""
        attempt = 0
        while True:
            try:
                return self._call_once(text, source_is_japanese)
            except requests.exceptions.Timeout:
                if attempt >= self.cfg.max_retry:
                    log.error("[TRANSLATE] Timeout, giving up. text=%s", abbreviate(text, 160))
                    return None
                attempt += 1
                log.warning(
                    "[TRANSLATE] Timeout. Retry %d/%d. text=%s",
                    attempt, self.cfg.max_retry, abbreviate(text, 160),
                )
                time.sleep(0.7)
            except requests.exceptions.RequestException as exc:
                log.error("[TRANSLATE] LLM call failed. text=%s, error=%s", abbreviate(text, 160), exc)
                return None


def translate_default(client: LlmClient, cache: TranslationCache, input_text: str) -> str:
    original = normalize(input_text)
    if original is None or not should_translate(original):
        return input_text

    # Đã có tiếng Nhật: xem như đã dịch, tuyệt đối giữ nguyên comment.
    if contains_japanese(original):
        log.info("[TRANSLATE] Skip: comment already contains Japanese.")
        return original

    # Chỉ dịch Việt/Latin -> Nhật
    cached = cache.get(original, False)
    if cached is not None and is_valid_translation(original, cached, False):
        return cached

    translated = normalize(client.translate_with_retry(original, False))

    # Lỗi LLM / kết quả không phải tiếng Nhật: không ghi đè raw comment.
    if not is_valid_translation(original, translated, False):
        log.warning("[TRANSLATE] Invalid translation; keeping original comment.")
        return original

    cache.put(original, False, translated)

    # Giữ tiếng Việt và thêm tiếng Nhật bên dưới.
    return translated


# --------------------------------------------------------------------------
# Checkpoint / watermark state (#2)
# --------------------------------------------------------------------------

@dataclass
class Watermark:
    last_ts: Optional[datetime]
    last_id: int


def load_watermark(state_file: Path) -> Watermark:
    if not state_file.exists():
        return Watermark(last_ts=None, last_id=0)
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        ts_raw = data.get("last_ts")
        last_ts = datetime.fromisoformat(ts_raw) if ts_raw else None
        return Watermark(last_ts=last_ts, last_id=int(data.get("last_id", 0)))
    except (json.JSONDecodeError, ValueError, OSError):
        log.warning("[STATE] Could not read state file, starting from beginning.")
        return Watermark(last_ts=None, last_id=0)


def save_watermark(state_file: Path, watermark: Watermark) -> None:
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({
            "last_ts": watermark.last_ts.isoformat() if watermark.last_ts else None,
            "last_id": watermark.last_id,
        }),
        encoding="utf-8",
    )
    tmp.replace(state_file)


# --------------------------------------------------------------------------
# Single-instance guard (#6)
# --------------------------------------------------------------------------

class FileLockGuard:
    """Prevent a second service instance from running on the same host."""

    def __init__(self, lock_file: Path):
        self.lock_file = lock_file
        self._fh = None

    def acquire(self) -> bool:
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.lock_file, "a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0, os.SEEK_END)
                if self._fh.tell() == 0:
                    self._fh.write(b"\0")
                    self._fh.flush()
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)

            self._fh.seek(0)
            self._fh.write(f"{os.getpid()}\n".encode("ascii"))
            self._fh.truncate()
            self._fh.flush()
            return True
        except (BlockingIOError, OSError):
            self._fh.close()
            self._fh = None
            return False

    def release(self) -> None:
        if self._fh is not None:
            try:
                if os.name == "nt":
                    import msvcrt
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._fh, fcntl.LOCK_UN)
            except Exception:  # noqa: BLE001
                pass
            self._fh.close()
            self._fh = None


def acquire_db_applock(conn, resource_name: str) -> bool:
    """Best-effort SQL Server sp_getapplock so a second process on a
    different host, pointed at the same database, also refuses to start.
    Held for the lifetime of `conn` (session-scoped); no-ops gracefully on
    non-SQL-Server or if the connection isn't available."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DECLARE @result int; "
            "EXEC @result = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
            "@LockOwner = 'Session', @LockTimeout = 0; "
            "SELECT @result;",
            resource_name,
        )
        row = cursor.fetchone()
        conn.commit()
        result_code = row[0] if row else -999
        return result_code >= 0
    except Exception as exc:  # noqa: BLE001
        log.warning("[LOCK] sp_getapplock not available/failed (%s). Relying on file lock only.", exc)
        return True  # don't block startup if the DB doesn't support applock


# --------------------------------------------------------------------------
# DB layer (#1 - persistent connection with reconnect)
# --------------------------------------------------------------------------

def build_connection_string(cfg: Config) -> str:
    parts = [f"DRIVER={cfg.db_driver}", f"SERVER={cfg.db_server}", f"DATABASE={cfg.db_database}"]
    if cfg.db_trusted_connection:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={cfg.db_user}")
        parts.append(f"PWD={cfg.db_password}")
    return ";".join(parts) + ";"


class DbConnection:
    """Wraps a single persistent pyodbc connection, reused across polls.
    Automatically reconnects (and re-acquires the applock) if the
    connection drops."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.conn = None

    def connect(self) -> None:
        if pyodbc is None:
            raise RuntimeError("pyodbc is not installed. Run: pip install pyodbc")
        conn_str = build_connection_string(self.cfg)
        self.conn = pyodbc.connect(conn_str, timeout=int(self.cfg.connect_timeout_seconds), autocommit=False)
        if self.cfg.use_db_applock:
            resource = f"patrol_translate_service:{self.cfg.table_name}"
            if not acquire_db_applock(self.conn, resource):
                raise RuntimeError(
                    "Another instance already holds the DB application lock "
                    f"('{resource}'). Refusing to start."
                )

    def ensure_connected(self) -> None:
        if self.conn is None:
            self.connect()
            return
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:  # noqa: BLE001
            log.warning("[DB] Connection appears dead, reconnecting...")
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001
                pass
            self.connect()

    def cursor(self):
        return self.conn.cursor()

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001
                pass
            self.conn = None


def fetch_new_records(db: DbConnection, cfg: Config, watermark: Watermark):
    # comment_japanese is the durable work queue: a blank value means this
    # source comment still needs a Japanese translation.  This also catches
    # older records that predate the local watermark.
    watermark_expr = f"[{cfg.created_column}]"
    sql = (
        f"SELECT TOP ({cfg.batch_size}) [{cfg.id_column}], [{cfg.source_column}], "
        f"[{cfg.japanese_column}], {watermark_expr} AS watermark_ts "
        f"FROM [{cfg.table_name}] "
        f"WHERE [{cfg.source_column}] IS NOT NULL "
        f"AND ([{cfg.japanese_column}] IS NULL OR LTRIM(RTRIM([{cfg.japanese_column}])) = '') "
        f"ORDER BY {watermark_expr} DESC, [{cfg.id_column}] DESC"
    )
    cursor = db.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def batch_update_comments(db: DbConnection, cfg: Config, updates: list[tuple[str, int]]) -> None:
    """Write Japanese translations without changing the raw source comment."""
    if not updates:
        return
    sql = (
        f"UPDATE [{cfg.table_name}] "
        f"SET [{cfg.japanese_column}] = ?, "
        f"[{cfg.ai_translate_update_column}] = GETDATE() "
        f"WHERE [{cfg.id_column}] = ?"
    )
    cursor = db.cursor()
    cursor.fast_executemany = True
    cursor.executemany(sql, updates)
    db.commit()


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

_shutdown_requested = False


def _handle_shutdown(signum, frame):  # noqa: ARG001
    global _shutdown_requested
    log.info("[SERVICE] Shutdown signal received (%s). Finishing current cycle...", signum)
    _shutdown_requested = True


def process_batch(
    client: LlmClient,
    cache: TranslationCache,
    db: DbConnection,
    cfg: Config,
    watermark: Watermark,
) -> Watermark:
    rows = fetch_new_records(db, cfg, watermark)

    if not rows:
        return watermark

    pending_updates: list[tuple[str, int]] = []

    for record_id, comment, japanese_comment, watermark_ts in rows:
        # Keep the greatest watermark so the checkpoint never moves back.
        if watermark_ts is not None and (
            watermark.last_ts is None
            or watermark_ts > watermark.last_ts
            or (watermark_ts == watermark.last_ts and record_id > watermark.last_id)
        ):
            watermark = Watermark(last_ts=watermark_ts, last_id=record_id)

        original = normalize(comment)
        if original is None:
            continue

        # Defensive check in case another process filled the target between
        # the SELECT and this loop.
        if contains_japanese(normalize(japanese_comment)):
            log.info("[SKIP] id=%s: %s already contains Japanese.", record_id, cfg.japanese_column)
            continue

        if not should_translate(original):
            log.info("[SKIP] id=%s: not translatable text: [%s]", record_id, abbreviate(original, 120))
            continue

        if contains_japanese(original):
            japanese_lines = [
                line.strip() for line in original.splitlines()
                if contains_japanese(line)
            ]
            existing_japanese = "\n".join(japanese_lines).strip()
            if existing_japanese:
                log.info(
                    "[MIGRATE] id=%s: copying existing Japanese to %s.",
                    record_id,
                    cfg.japanese_column,
                )
                pending_updates.append((existing_japanese, record_id))
            else:
                log.info("[SKIP] id=%s: source comment already contains Japanese.", record_id)
            continue

        log.info("[TRANSLATE] id=%s: source=[%s]", record_id, abbreviate(original, 160))
        translated = translate_default(client, cache, original)

        if translated == original:
            log.warning("[TRANSLATE] id=%s: translation unchanged/invalid, skipping.", record_id)
            continue

        log.info("[TRANSLATE] id=%s: result=[%s]", record_id, abbreviate(translated, 160))
        pending_updates.append((translated, record_id))

    if pending_updates:
        if cfg.dry_run:
            for translated, record_id in pending_updates:
                log.info("[DRY-RUN] id=%s: would update %s -> [%s]", record_id, cfg.japanese_column, abbreviate(translated, 160))
        else:
            batch_update_comments(db, cfg, pending_updates)
            log.info("[DB] Batch updated %d record(s) in one commit.", len(pending_updates))

    return watermark


def run() -> None:
    cfg = load_config()
    setup_logging(cfg)

    if not cfg.db_server or not cfg.db_database:
        log.error("DB_SERVER and DB_DATABASE must be set. Exiting.")
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    # #6 single instance: local file lock first (cheap, fast fail)
    file_lock = FileLockGuard(cfg.lock_file)
    if not file_lock.acquire():
        log.error("[LOCK] Another instance is already running on this host (lock file: %s). Exiting.", cfg.lock_file)
        sys.exit(1)

    client = LlmClient(cfg)
    cache = TranslationCache(cfg.cache_db_file)
    db = DbConnection(cfg)
    watermark = load_watermark(cfg.state_file)

    try:
        db.connect()  # also acquires sp_getapplock if enabled
    except Exception:
        log.exception("[SERVICE] Could not start (DB connect / app-lock failed).")
        file_lock.release()
        sys.exit(1)

    log.info(
        "[SERVICE] Starting. table=%s, column=%s, watermark=%s/%s, pollInterval=%ss, dryRun=%s",
        cfg.table_name, cfg.source_column, watermark.last_ts, watermark.last_id,
        cfg.poll_interval_seconds, cfg.dry_run,
    )

    try:
        while not _shutdown_requested:
            try:
                db.ensure_connected()
                watermark = process_batch(client, cache, db, cfg, watermark)
                save_watermark(cfg.state_file, watermark)
            except Exception:  # noqa: BLE001
                log.exception("[SERVICE] Unexpected error during poll cycle.")

            for _ in range(int(cfg.poll_interval_seconds * 10)):
                if _shutdown_requested:
                    break
                time.sleep(0.1)
    finally:
        db.close()
        cache.close()
        file_lock.release()
        log.info("[SERVICE] Stopped.")


if __name__ == "__main__":
    run()
    raise SystemExit(0)
    TEST_ID = 2927  # đổi 123 thành id cần dịch

    cfg = load_config()
    setup_logging(cfg)

    db = DbConnection(cfg)
    cache = TranslationCache(cfg.cache_db_file)
    client = LlmClient(cfg)

    try:
        db.connect()

        row = db.cursor().execute(
            f"SELECT [{cfg.source_column}] "
            f"FROM [{cfg.table_name}] "
            f"WHERE [{cfg.id_column}] = ?",
            TEST_ID,
        ).fetchone()

        if row is None:
            print(f"Không tìm thấy id={TEST_ID}")
        else:
            original = normalize(row[0])
            print("Gốc:", original)

            translated = translate_default(client, cache, original)
            print("Tiếng Nhật:", translated)

            # Test an toàn: DRY_RUN=1 chỉ in kết quả, không ghi DB
            if cfg.dry_run:
                print("DRY_RUN=1: chưa cập nhật database.")
            elif translated != original:
                batch_update_comments(db, cfg, [(translated, TEST_ID)])
                print(f"Đã cập nhật comment cho id={TEST_ID}")

    finally:
        db.close()
        cache.close()
