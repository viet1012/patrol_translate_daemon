"""Standalone background monitor for LM Studio model recovery.

Run this process separately from ``patrol_translate_service.py``. It sends a
small completion probe at an interval and reloads the configured model when
the response is unavailable, malformed, or has empty content.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from llm_studio_recovery import LmStudioRecovery, is_corrupted_content


load_dotenv(override=True)


def _enabled(value: Optional[str], default: str) -> bool:
    return (value or default).lower() in ("1", "yes", "true")


@dataclass(frozen=True)
class MonitorConfig:
    lm_url: str
    lm_model: str
    lm_api_key: Optional[str]
    interval_seconds: float
    timeout_seconds: float
    enabled: bool
    cooldown_seconds: float
    unload_first: bool
    instance_id: Optional[str]
    failure_threshold: int
    reload_lock_file: Path

    @classmethod
    def from_environment(cls) -> "MonitorConfig":
        return cls(
            lm_url=os.getenv("LM_URL", "http://127.0.0.1:1234").rstrip("/"),
            lm_model=os.getenv("LM_MODEL", ""),
            lm_api_key=os.getenv("LM_API_KEY") or None,
            interval_seconds=float(os.getenv("LM_STUDIO_MONITOR_INTERVAL_SECONDS", "30")),
            timeout_seconds=float(os.getenv("LM_STUDIO_MONITOR_TIMEOUT_SECONDS", "30")),
            enabled=_enabled(os.getenv("LM_STUDIO_MONITOR_ENABLED"), "1"),
            cooldown_seconds=float(os.getenv("LM_STUDIO_RELOAD_COOLDOWN_SECONDS", "60")),
            unload_first=_enabled(os.getenv("LM_STUDIO_RELOAD_UNLOAD_FIRST"), "1"),
            instance_id=os.getenv("LM_STUDIO_INSTANCE_ID") or os.getenv("LM_MODEL"),
            failure_threshold=max(1, int(os.getenv("LM_STUDIO_FAILURE_THRESHOLD", "3"))),
            reload_lock_file=Path(os.getenv("LM_STUDIO_RELOAD_LOCK_FILE", "./lm_studio_reload.lock")),
        )


class LmStudioRecoveryDaemon:
    """Runs a lightweight content probe and delegates reloads to recovery."""

    def __init__(self, cfg: MonitorConfig, logger: logging.Logger) -> None:
        if not cfg.lm_model:
            raise ValueError("LM_MODEL is required for the LM Studio recovery monitor.")
        if cfg.interval_seconds <= 0:
            raise ValueError("LM_STUDIO_MONITOR_INTERVAL_SECONDS must be greater than zero.")

        self.cfg = cfg
        self.log = logger
        self.session = requests.Session()
        self.recovery = LmStudioRecovery(
            base_url=cfg.lm_url,
            model_key=cfg.lm_model,
            api_key=cfg.lm_api_key,
            enabled=cfg.enabled,
            cooldown_seconds=cfg.cooldown_seconds,
            timeout_seconds=cfg.timeout_seconds,
            unload_first=cfg.unload_first,
            logger=logger,
            session=self.session,
            instance_id=cfg.instance_id,
            failure_threshold=cfg.failure_threshold,
            lock_file=cfg.reload_lock_file,
        )

    def check_once(self) -> bool:
        """Return True only when LM Studio produces non-empty probe content."""
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.cfg.lm_api_key:
            headers["Authorization"] = f"Bearer {self.cfg.lm_api_key}"

        payload = {
            "model": self.cfg.lm_model,
            "temperature": 0,
            "max_tokens": 8,
            "stream": False,
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}],
        }
        try:
            response = self.session.post(
                f"{self.cfg.lm_url}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=self.cfg.timeout_seconds,
            )
        except requests.RequestException as exc:
            self.log.warning("[LM-STUDIO-MONITOR] Probe request failed: %s", exc)
            return False

        if not response.ok:
            self.log.warning("[LM-STUDIO-MONITOR] Probe returned HTTP %d.", response.status_code)
            return False

        try:
            content = json.loads(response.text)["choices"][0]["message"]["content"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            content = None

        if not isinstance(content, str) or not content.strip() or is_corrupted_content(content):
            self.log.warning("[LM-STUDIO-MONITOR] Probe returned empty or corrupted content.")
            self.recovery.record_invalid_completion("probe returned empty or corrupted content")
            return False

        self.recovery.record_success()
        self.log.debug("[LM-STUDIO-MONITOR] Probe succeeded.")
        return True

    def run(self, stop_event: threading.Event) -> None:
        self.log.info(
            "[LM-STUDIO-MONITOR] Started. model=%s interval=%ss",
            self.cfg.lm_model,
            self.cfg.interval_seconds,
        )
        while not stop_event.is_set():
            self.check_once()
            stop_event.wait(self.cfg.interval_seconds)
        self.log.info("[LM-STUDIO-MONITOR] Stopped.")


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("lm_studio_recovery_monitor")
    stop_event = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    cfg = MonitorConfig.from_environment()
    if not cfg.enabled:
        logger.info("[LM-STUDIO-MONITOR] Disabled by LM_STUDIO_MONITOR_ENABLED.")
        return
    LmStudioRecoveryDaemon(cfg, logger).run(stop_event)


if __name__ == "__main__":
    main()
