"""
patrol_translate_service.py  (v3)

Background service that watches the F2_Patrol_Report table and, for every
new/edited record whose SOURCE column is NOT Japanese, calls a local LLM
(LM Studio / any OpenAI-compatible /v1/chat/completions endpoint) to
translate it into Japanese, then writes the translation into a separate
TARGET column (source stays untouched).

Translation logic (prompts, output cleanup, validation, retry policy) is
UNCHANGED from v1/v2 and mirrors the Java `PatrolCommentService` exactly.

--------------------------------------------------------------------
v3 change: MULTIPLE column pairs in one service.

Instead of a single hard-coded (comment -> comment_japanese) pair, the
service now reads a list of "source:target" pairs from TRANSLATE_COLUMNS
and processes all of them every poll cycle, e.g.:

    TRANSLATE_COLUMNS=comment:comment_jp,countermeasure:countermeasure_jp

Each pair is queue-based: a row is considered "pending" for a given pair
whenever target IS NULL/empty and source IS NOT NULL. This naturally
covers old rows (no watermark needed) and edited rows (if you ever clear
the target column, it goes back into the queue).

Everything else (persistent DB connection + reconnect, SQLite translation
cache, batched UPDATE with executemany() + one commit, daily-rotating
logs, single-instance guard via file lock + sp_getapplock, requests
Session with HTTPAdapter/Retry) is unchanged from v2.
--------------------------------------------------------------------
Configuration (environment variables). Required: DB_SERVER, DB_DATABASE.

  DB_DRIVER              default "{ODBC Driver 17 for SQL Server}"
  DB_SERVER
  DB_DATABASE
  DB_USER / DB_PASSWORD  (omit both if DB_TRUSTED_CONNECTION=yes)
  DB_TRUSTED_CONNECTION  "yes" for Windows auth

  TABLE_NAME                  default "F2_Patrol_Report"
  ID_COLUMN                   default "id"
  CREATED_COLUMN              default "created_at"
  TRANSLATE_COLUMNS           default "comment:comment_jp,countermeasure:countermeasure_jp"
                               comma-separated "source:target" pairs, e.g.
                               "comment:comment_jp,countermeasure:countermeasure_jp"
  AI_TRANSLATE_UPDATE_COLUMN  default "ai_translate_update_at"
                               (shared timestamp column, stamped on every
                               successful write to any target column)

  LM_URL                 default "http://192.168.122.16:1234"
  LM_API_KEY              default ""
  LM_MODEL               default "openai/gpt-oss-20b"
  LM_STUDIO_AUTO_RELOAD  default "1". Reload the model when a successful
                         completion response has missing/empty content.
  LM_STUDIO_RELOAD_COOLDOWN_SECONDS  default 60
  LM_STUDIO_RELOAD_UNLOAD_FIRST       default "1"

  POLL_INTERVAL_SECONDS  default 5
  BATCH_SIZE              default 20     (rows fetched per pair, per poll)
  STATE_FILE              default "./patrol_translate_state.json"
  CACHE_DB_FILE           default "./patrol_translate_cache.sqlite3"
  LOCK_FILE               default "./patrol_translate.lock"
  LOG_DIR                 default "./logs"
  LOG_FILE_NAME           default "patrol_translate.log"
  LOG_BACKUP_DAYS         default 14
  USE_DB_APPLOCK          default "1"  (also try sp_getapplock, best-effort)
  HTTP_MAX_RETRIES        default 3
  HTTP_BACKOFF_FACTOR     default 0.5
  DRY_RUN                 "1" to log without writing to DB

Requires: pyodbc, requests, python-dotenv
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter

from dotenv import load_dotenv
from llm_studio_recovery import LmStudioRecovery, is_corrupted_content
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
    translate_columns: tuple  # tuple of (source_column, target_column) pairs
    ai_translate_update_column: str

    lm_url: str
    lm_api_key: Optional[str]
    lm_model: str
    lm_studio_auto_reload: bool
    lm_studio_reload_cooldown_seconds: float
    lm_studio_reload_unload_first: bool
    lm_studio_instance_id: Optional[str]
    lm_studio_failure_threshold: int
    lm_studio_reload_lock_file: Path
    llm_failure_retry_seconds: float
    llm_failure_retry_max_seconds: float

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


def _parse_translate_columns(raw: str) -> tuple:
    """Parse "src1:tgt1,src2:tgt2" into (("src1","tgt1"), ("src2","tgt2"))."""
    pairs = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" not in chunk:
            raise ValueError(
                f"Invalid TRANSLATE_COLUMNS entry '{chunk}'. Expected 'source:target'."
            )
        source, target = chunk.split(":", 1)
        source = source.strip()
        target = target.strip()
        if not source or not target:
            raise ValueError(
                f"Invalid TRANSLATE_COLUMNS entry '{chunk}'. Expected 'source:target'."
            )
        pairs.append((source, target))
    if not pairs:
        raise ValueError("TRANSLATE_COLUMNS must contain at least one 'source:target' pair.")
    return tuple(pairs)


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
        translate_columns=_parse_translate_columns(
            env("TRANSLATE_COLUMNS", "comment:comment_japanese")
        ),
        ai_translate_update_column=env("AI_TRANSLATE_UPDATE_COLUMN", "ai_translate_update_at"),
        lm_url=(env("LM_URL", "http://192.168.122.16:1234") or "").rstrip("/"),
        lm_api_key=env("LM_API_KEY"),
        lm_model=env("LM_MODEL", "openai/gpt-oss-20b"),
        lm_studio_auto_reload=(env("LM_STUDIO_AUTO_RELOAD", "1") or "1").lower() in ("1", "yes", "true"),
        lm_studio_reload_cooldown_seconds=float(env("LM_STUDIO_RELOAD_COOLDOWN_SECONDS", "60")),
        lm_studio_reload_unload_first=(env("LM_STUDIO_RELOAD_UNLOAD_FIRST", "1") or "1").lower() in ("1", "yes", "true"),
        lm_studio_instance_id=env("LM_STUDIO_INSTANCE_ID") or env("LM_MODEL"),
        lm_studio_failure_threshold=max(1, int(env("LM_STUDIO_FAILURE_THRESHOLD", "3"))),
        lm_studio_reload_lock_file=Path(env("LM_STUDIO_RELOAD_LOCK_FILE", "./lm_studio_reload.lock")),
        llm_failure_retry_seconds=float(env("LLM_FAILURE_RETRY_SECONDS", "300")),
        llm_failure_retry_max_seconds=float(env("LLM_FAILURE_RETRY_MAX_SECONDS", "3600")),
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
# Logging (TimedRotatingFileHandler)
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
# Language helpers (UNCHANGED / PatrolCommentService)
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
# Prompts (UNCHANGED)
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
# Translation cache (SQLite)
# --------------------------------------------------------------------------

class TranslationCache:
    """SQLite-backed cache of source-text -> translated-text, keyed by a
    hash of (source language flag + normalized source text). Shared across
    all column pairs on purpose: the same phrase appearing in `comment` and
    `countermeasure` only needs to be translated once."""

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
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS translation_failures (
                source_hash TEXT PRIMARY KEY,
                source_lang TEXT NOT NULL,
                failure_count INTEGER NOT NULL,
                retry_after TEXT NOT NULL,
                last_error TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
        self.conn.execute("DELETE FROM translation_failures WHERE source_hash = ?", (key,))
        self.conn.commit()

    def remaining_failure_delay(self, source_text: str, source_is_japanese: bool) -> int:
        """Return seconds until retry is allowed, or zero when it is allowed."""
        key = self._key(source_text, source_is_japanese)
        row = self.conn.execute(
            "SELECT retry_after FROM translation_failures WHERE source_hash = ?", (key,)
        ).fetchone()
        if not row:
            return 0
        try:
            retry_after = datetime.fromisoformat(row[0])
        except (TypeError, ValueError):
            self.conn.execute("DELETE FROM translation_failures WHERE source_hash = ?", (key,))
            self.conn.commit()
            return 0
        remaining = (retry_after - datetime.utcnow()).total_seconds()
        return max(0, int(remaining) + (1 if remaining > 0 else 0))

    def record_failure(
        self,
        source_text: str,
        source_is_japanese: bool,
        *,
        base_delay_seconds: float,
        max_delay_seconds: float,
        reason: str,
    ) -> int:
        """Persist exponential retry delay and return the delay in seconds."""
        if base_delay_seconds <= 0:
            return 0
        key = self._key(source_text, source_is_japanese)
        row = self.conn.execute(
            "SELECT failure_count FROM translation_failures WHERE source_hash = ?", (key,)
        ).fetchone()
        failure_count = (int(row[0]) if row else 0) + 1
        delay = base_delay_seconds * (2 ** min(failure_count - 1, 20))
        delay = int(min(delay, max(max_delay_seconds, base_delay_seconds)))
        now = datetime.utcnow()
        retry_after = now + timedelta(seconds=delay)
        self.conn.execute(
            """
            INSERT INTO translation_failures
                (source_hash, source_lang, failure_count, retry_after, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_hash) DO UPDATE SET
                failure_count = excluded.failure_count,
                retry_after = excluded.retry_after,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (
                key,
                "ja" if source_is_japanese else "vi",
                failure_count,
                retry_after.isoformat(),
                reason[:300],
                now.isoformat(),
            ),
        )
        self.conn.commit()
        return delay

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# LLM client (Session + HTTPAdapter + Retry)
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
        self.recovery = LmStudioRecovery(
            base_url=cfg.lm_url,
            model_key=cfg.lm_model,
            api_key=cfg.lm_api_key,
            enabled=cfg.lm_studio_auto_reload,
            cooldown_seconds=cfg.lm_studio_reload_cooldown_seconds,
            timeout_seconds=cfg.request_timeout_seconds,
            unload_first=cfg.lm_studio_reload_unload_first,
            logger=log,
            session=self.session,
            instance_id=cfg.lm_studio_instance_id,
            failure_threshold=cfg.lm_studio_failure_threshold,
            lock_file=cfg.lm_studio_reload_lock_file,
        )

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

        response = self.session.post(
            url,
            json=payload,
            headers=headers,
            timeout=(self.cfg.connect_timeout_seconds, self.cfg.request_timeout_seconds),
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)

        if response.status_code == 200:
            result = self._extract_content(response.text)
            recovery_reason = None
            if result is None:
                recovery_reason = "missing or empty choices[0].message.content"
            elif is_corrupted_content(result):
                log.warning("[TRANSLATE] LLM returned corrupted content made only of '?' or replacement characters.")
                result = None
                recovery_reason = "corrupted content made only of '?' or replacement characters"
            log.info(
                "[TRANSLATE] Completed in %d ms. inputLength=%d, outputLength=%d",
                elapsed_ms, len(text), len(result) if result else 0,
            )
            if recovery_reason:
                self.recovery.record_invalid_completion(recovery_reason)
            else:
                self.recovery.record_success()
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

        if content is not None and not isinstance(content, str):
            log.warning("[TRANSLATE] LLM content has unexpected type: %s", type(content).__name__)
            return None

        content = normalize(content)
        if content is None:
            return None
        return clean_model_output(content)

    def translate_with_retry(self, text: str, source_is_japanese: bool) -> Optional[str]:
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
    return translate_text(client, cache, input_text, source_is_japanese=False)


def translate_text(
    client: LlmClient,
    cache: TranslationCache,
    input_text: str,
    source_is_japanese: bool,
) -> str:
    original = normalize(input_text)
    if original is None or not should_translate(original):
        return input_text

    # Đã có tiếng Nhật: xem như đã dịch, tuyệt đối giữ nguyên comment.
    if source_is_japanese:
        if not contains_japanese(original):
            log.info("[TRANSLATE] Skip: expected Japanese source text.")
            return original
    elif contains_japanese(original):
        log.info("[TRANSLATE] Skip: text already contains Japanese.")
        return original

    # Chỉ dịch Việt/Latin -> Nhật
    cached = cache.get(original, source_is_japanese)
    if cached is not None and is_valid_translation(original, cached, source_is_japanese):
        return cached

    remaining_delay = cache.remaining_failure_delay(original, source_is_japanese)
    if remaining_delay:
        log.warning(
            "[TRANSLATE] Skipping previously failed text for another %ds. text=%s",
            remaining_delay,
            abbreviate(original, 160),
        )
        return original

    translated = normalize(client.translate_with_retry(original, source_is_japanese))

    # Lỗi LLM / kết quả không phải tiếng Nhật: không ghi đè raw text.
    if not is_valid_translation(original, translated, source_is_japanese):
        retry_delay = cache.record_failure(
            original,
            source_is_japanese,
            base_delay_seconds=client.cfg.llm_failure_retry_seconds,
            max_delay_seconds=client.cfg.llm_failure_retry_max_seconds,
            reason="LLM returned no valid translation",
        )
        log.warning("[TRANSLATE] Invalid translation; keeping original text.")
        if retry_delay:
            log.warning("[TRANSLATE] Next retry for this text will wait %ds.", retry_delay)
        return original

    cache.put(original, source_is_japanese, translated)
    return translated


# --------------------------------------------------------------------------
# Checkpoint / watermark state (kept for observability/logging only; the
# actual work queue is driven by "target IS NULL/empty", see fetch_pending())
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
# Single-instance guard
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
        return True


# --------------------------------------------------------------------------
# DB layer (persistent connection with reconnect)
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


def fetch_pending(db: DbConnection, cfg: Config, source_column: str, target_column: str):
    """Rows where `source_column` has text but `target_column` is still
    empty -- i.e. still pending translation for THIS column pair."""
    watermark_expr = f"[{cfg.created_column}]"
    sql = (
        f"SELECT TOP ({cfg.batch_size}) [{cfg.id_column}], [{source_column}], "
        f"[{target_column}], {watermark_expr} AS watermark_ts "
        f"FROM [{cfg.table_name}] "
        f"WHERE [{source_column}] IS NOT NULL "
        f"AND ([{target_column}] IS NULL OR LTRIM(RTRIM([{target_column}])) = '') "
        f"ORDER BY {watermark_expr} DESC, [{cfg.id_column}] DESC"
    )
    cursor = db.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def batch_update_target(db: DbConnection, cfg: Config, target_column: str, updates: list[tuple[str, int]]) -> None:
    """Write translations into `target_column` without touching the source
    column. updates: list of (translated_text, id)."""
    if not updates:
        return
    sql = (
        f"UPDATE [{cfg.table_name}] "
        f"SET [{target_column}] = ?, "
        f"[{cfg.ai_translate_update_column}] = GETDATE() "
        f"WHERE [{cfg.id_column}] = ?"
    )
    cursor = db.cursor()
    # Avoid pyodbc allocating one fixed string buffer from the first value in
    # a batch.  Japanese translations naturally vary in length.
    cursor.fast_executemany = False
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


def process_pair(
    client: LlmClient,
    cache: TranslationCache,
    db: DbConnection,
    cfg: Config,
    source_column: str,
    target_column: str,
    watermark: Watermark,
) -> Watermark:
    rows = fetch_pending(db, cfg, source_column, target_column)
    source_is_japanese = source_column.casefold().endswith(("_jp", "_japanese"))

    if not rows:
        return watermark

    pending_updates: list[tuple[str, int]] = []

    for record_id, source_value, target_value, watermark_ts in rows:
        if watermark_ts is not None and (
            watermark.last_ts is None
            or watermark_ts > watermark.last_ts
            or (watermark_ts == watermark.last_ts and record_id > watermark.last_id)
        ):
            watermark = Watermark(last_ts=watermark_ts, last_id=record_id)

        original = normalize(source_value)
        if original is None:
            continue

        # Defensive check in case another process filled the target between
        # the SELECT and this loop.
        if contains_japanese(normalize(target_value)):
            log.info("[SKIP] id=%s: %s already contains Japanese.", record_id, target_column)
            continue

        if not should_translate(original):
            log.info(
                "[SKIP] id=%s (%s): not translatable text: [%s]",
                record_id, source_column, abbreviate(original, 120),
            )
            continue

        if contains_japanese(original) and not source_is_japanese:
            japanese_lines = [
                line.strip() for line in original.splitlines()
                if contains_japanese(line)
            ]
            existing_japanese = "\n".join(japanese_lines).strip()
            if existing_japanese:
                log.info(
                    "[MIGRATE] id=%s: copying existing Japanese from %s to %s.",
                    record_id, source_column, target_column,
                )
                pending_updates.append((existing_japanese, record_id))
            else:
                log.info(
                    "[SKIP] id=%s: %s already contains Japanese.", record_id, source_column,
                )
            continue

        log.info("[TRANSLATE] id=%s (%s): source=[%s]", record_id, source_column, abbreviate(original, 160))
        translated = translate_text(client, cache, original, source_is_japanese)

        if translated == original:
            log.warning("[TRANSLATE] id=%s (%s): translation unchanged/invalid, skipping.", record_id, source_column)
            continue

        log.info("[TRANSLATE] id=%s (%s): result=[%s]", record_id, source_column, abbreviate(translated, 160))
        pending_updates.append((translated, record_id))

    if pending_updates:
        if cfg.dry_run:
            for translated, record_id in pending_updates:
                log.info(
                    "[DRY-RUN] id=%s: would update %s -> [%s]",
                    record_id, target_column, abbreviate(translated, 160),
                )
        else:
            batch_update_target(db, cfg, target_column, pending_updates)
            log.info(
                "[DB] Batch updated %d record(s) for %s in one commit.",
                len(pending_updates), target_column,
            )

    return watermark


def process_batch(
    client: LlmClient,
    cache: TranslationCache,
    db: DbConnection,
    cfg: Config,
    watermark: Watermark,
) -> Watermark:
    """Runs one poll cycle across ALL configured (source, target) pairs."""
    for source_column, target_column in cfg.translate_columns:
        watermark = process_pair(client, cache, db, cfg, source_column, target_column, watermark)
    return watermark


def run() -> None:
    cfg = load_config()
    setup_logging(cfg)

    if not cfg.db_server or not cfg.db_database:
        log.error("DB_SERVER and DB_DATABASE must be set. Exiting.")
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    file_lock = FileLockGuard(cfg.lock_file)
    if not file_lock.acquire():
        log.error("[LOCK] Another instance is already running on this host (lock file: %s). Exiting.", cfg.lock_file)
        sys.exit(1)

    client = LlmClient(cfg)
    cache = TranslationCache(cfg.cache_db_file)
    db = DbConnection(cfg)
    watermark = load_watermark(cfg.state_file)

    try:
        db.connect()
    except Exception:
        log.exception("[SERVICE] Could not start (DB connect / app-lock failed).")
        file_lock.release()
        sys.exit(1)

    log.info(
        "[SERVICE] Starting. table=%s, pairs=%s, watermark=%s/%s, pollInterval=%ss, dryRun=%s",
        cfg.table_name, cfg.translate_columns, watermark.last_ts, watermark.last_id,
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
