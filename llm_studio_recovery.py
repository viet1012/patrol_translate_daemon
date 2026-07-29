"""Production-safe recovery helper for LM Studio."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

import requests


def is_corrupted_content(value: Optional[str]) -> bool:
    """
    Chỉ xem là corrupted khi output sau khi bỏ khoảng trắng
    hoàn toàn gồm dấu '?' hoặc Unicode replacement character.
    """
    if not isinstance(value, str):
        return False

    compact = "".join(value.split())

    return (
        bool(compact)
        and set(compact).issubset({"?", "\ufffd"})
    )


class _ProcessFileLock:
    """
    File lock liên process.

    Windows dùng msvcrt.
    Linux/macOS dùng fcntl.
    """

    def __init__(
        self,
        lock_file: Path,
        logger: logging.Logger,
    ) -> None:
        self.lock_file = lock_file
        self.log = logger
        self._file = None

    def acquire(self) -> bool:
        self.lock_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._file = open(
            self.lock_file,
            "a+b",
        )

        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0, os.SEEK_END)

                if self._file.tell() == 0:
                    self._file.write(b"\0")
                    self._file.flush()

                self._file.seek(0)

                msvcrt.locking(
                    self._file.fileno(),
                    msvcrt.LK_NBLCK,
                    1,
                )

            else:
                import fcntl

                fcntl.flock(
                    self._file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )

            return True

        except (BlockingIOError, OSError):
            self.release()
            return False

    def release(self) -> None:
        if self._file is None:
            return

        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)

                msvcrt.locking(
                    self._file.fileno(),
                    msvcrt.LK_UNLCK,
                    1,
                )

            else:
                import fcntl

                fcntl.flock(
                    self._file.fileno(),
                    fcntl.LOCK_UN,
                )

        except Exception:
            pass

        try:
            self._file.close()
        except Exception:
            pass

        self._file = None

    def read_reload_timestamp(self) -> float:
        """Read the last reload wall-clock time while this lock is held."""
        if self._file is None:
            return 0.0
        try:
            self._file.seek(0)
            raw = self._file.read().decode("ascii", errors="ignore").strip("\0 \t\r\n")
            return float(raw) if raw else 0.0
        except (OSError, ValueError):
            return 0.0

    def write_reload_timestamp(self, timestamp: float) -> None:
        """Persist a reload time so the cooldown is shared across processes."""
        if self._file is None:
            return
        self._file.seek(0)
        self._file.write(f"{timestamp:.6f}".encode("ascii"))
        self._file.truncate()
        self._file.flush()

    def __enter__(self) -> "_ProcessFileLock":
        if not self.acquire():
            raise RuntimeError(
                "LM Studio reload lock is already held"
            )

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.release()


class LmStudioRecovery:
    """
    Phục hồi LM Studio khi model liên tục trả output hỏng.

    Class này không tự reload vì HTTP 400/429/500.
    Caller chỉ gọi record_invalid_completion() khi:
    - content rỗng;
    - content toàn dấu '?';
    - content toàn Unicode replacement character;
    - response completion không có content hợp lệ.
    """

    def __init__(
        self,
        *,
        base_url: str,
        model_key: str,
        api_key: Optional[str],
        enabled: bool,
        cooldown_seconds: float,
        timeout_seconds: float,
        unload_first: bool,
        logger: logging.Logger,
        session: Optional[requests.Session] = None,
        instance_id: Optional[str] = None,
        failure_threshold: int = 3,
        lock_file: str | Path = "./lm_studio_reload.lock",
    ) -> None:
        self.base_url = base_url.rstrip("/")

        # Dùng cho /models/load
        self.model_key = model_key

        # Dùng cho /models/unload.
        # Có thể khác model_key.
        self.instance_id = instance_id

        self.api_key = api_key
        self.enabled = enabled

        self.cooldown_seconds = max(
            0.0,
            cooldown_seconds,
        )

        self.timeout_seconds = max(
            1.0,
            timeout_seconds,
        )

        self.unload_first = unload_first
        self.failure_threshold = max(
            1,
            failure_threshold,
        )

        self.log = logger
        self.session = session or requests.Session()

        self.lock_file = Path(lock_file)

        self._thread_lock = threading.Lock()
        self._consecutive_failures = 0
        self._last_reload_at = 0.0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def record_success(self) -> None:
        """
        Gọi sau một completion hợp lệ.

        Reset chuỗi lỗi liên tiếp.
        """
        with self._thread_lock:
            if self._consecutive_failures > 0:
                self.log.info(
                    "[LM-STUDIO] Valid completion received; "
                    "resetting consecutive failure count from %d to 0.",
                    self._consecutive_failures,
                )

            self._consecutive_failures = 0

    def record_invalid_completion(
        self,
        reason: str,
    ) -> bool:
        """
        Gọi khi completion content bị hỏng.

        Chỉ reload khi số lỗi liên tiếp đạt threshold.
        """
        if not self.enabled:
            return False

        with self._thread_lock:
            self._consecutive_failures += 1
            failure_count = self._consecutive_failures

        self.log.warning(
            "[LM-STUDIO] Invalid completion detected. "
            "reason=%s, consecutiveFailures=%d/%d",
            reason,
            failure_count,
            self.failure_threshold,
        )

        if failure_count < self.failure_threshold:
            return False

        reloaded = self._attempt_reload(reason)

        if reloaded:
            with self._thread_lock:
                self._consecutive_failures = 0

        return reloaded

    def _attempt_reload(
        self,
        reason: str,
    ) -> bool:
        now = time.monotonic()

        with self._thread_lock:
            remaining = (
                self.cooldown_seconds
                - (now - self._last_reload_at)
            )

            if remaining > 0:
                self.log.warning(
                    "[LM-STUDIO] Reload skipped because cooldown "
                    "is active for %.0f more seconds.",
                    remaining,
                )
                return False

        process_lock = _ProcessFileLock(
            self.lock_file,
            self.log,
        )

        if not process_lock.acquire():
            self.log.warning(
                "[LM-STUDIO] Another process is already "
                "reloading LM Studio; skipping duplicate reload."
            )
            return False

        try:
            # Kiểm tra lại cooldown sau khi lấy được file lock,
            # vì process khác có thể vừa reload xong.
            now = time.monotonic()
            wall_now = time.time()
            shared_last_reload_at = process_lock.read_reload_timestamp()
            shared_remaining = self.cooldown_seconds - (wall_now - shared_last_reload_at)

            if shared_remaining > 0:
                self.log.warning(
                    "[LM-STUDIO] Reload skipped because another process reloaded "
                    "%.0f seconds ago.",
                    self.cooldown_seconds - shared_remaining,
                )
                return False

            with self._thread_lock:
                remaining = (
                    self.cooldown_seconds
                    - (now - self._last_reload_at)
                )

                if remaining > 0:
                    self.log.warning(
                        "[LM-STUDIO] Reload skipped after acquiring "
                        "lock because cooldown is active for %.0f seconds.",
                        remaining,
                    )
                    return False

                # Set trước network call để tránh reload loop.
                self._last_reload_at = now

            process_lock.write_reload_timestamp(wall_now)
            return self._reload_model(reason)

        finally:
            process_lock.release()

    def _reload_model(
        self,
        reason: str,
    ) -> bool:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.api_key:
            headers["Authorization"] = (
                f"Bearer {self.api_key}"
            )

        try:
            if self.unload_first:
                self._unload_model(headers)

            loaded = self.session.post(
                f"{self.base_url}/api/v1/models/load",
                json={
                    "model": self.model_key,
                },
                headers=headers,
                timeout=self.timeout_seconds,
            )

            if not loaded.ok:
                self.log.error(
                    "[LM-STUDIO] Model load failed. "
                    "model=%s, status=%d, body=%s",
                    self.model_key,
                    loaded.status_code,
                    loaded.text[:600],
                )
                return False

            self.log.warning(
                "[LM-STUDIO] Model '%s' reloaded successfully "
                "after repeated invalid completions. reason=%s",
                self.model_key,
                reason,
            )

            return True

        except requests.RequestException as exc:
            self.log.error(
                "[LM-STUDIO] Reload request failed. error=%s",
                exc,
            )
            return False

    def _unload_model(
        self,
        headers: dict[str, str],
    ) -> None:
        if not self.instance_id:
            self.log.warning(
                "[LM-STUDIO] unload_first=true but instance_id "
                "is not configured; skipping unload."
            )
            return

        try:
            unloaded = self.session.post(
                f"{self.base_url}/api/v1/models/unload",
                json={
                    "instance_id": self.instance_id,
                },
                headers=headers,
                timeout=self.timeout_seconds,
            )

            if not unloaded.ok:
                self.log.warning(
                    "[LM-STUDIO] Model unload failed; "
                    "continuing with load. "
                    "instanceId=%s, status=%d, body=%s",
                    self.instance_id,
                    unloaded.status_code,
                    unloaded.text[:300],
                )
                return

            self.log.info(
                "[LM-STUDIO] Model instance unloaded. "
                "instanceId=%s",
                self.instance_id,
            )

        except requests.RequestException as exc:
            self.log.warning(
                "[LM-STUDIO] Model unload request failed; "
                "continuing with load. error=%s",
                exc,
            )
