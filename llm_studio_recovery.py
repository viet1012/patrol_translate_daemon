"""
Production-safe recovery helper for LM Studio.

Chức năng:
- Theo dõi lỗi liên tiếp từ LM Studio.
- Reload model khi đạt ngưỡng lỗi.
- Chống reload trùng giữa nhiều thread.
- Chống reload trùng giữa nhiều process.
- Có cooldown sau khi reload thành công.
- Có delay ngắn sau khi reload thất bại để tránh spam API.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

import requests


# ============================================================================
# Output validation
# ============================================================================

def is_corrupted_content(value: Optional[str]) -> bool:
    """
    Xác định output bị hỏng.

    Chỉ xem là corrupted khi nội dung sau khi bỏ khoảng trắng
    hoàn toàn chỉ gồm:
    - dấu hỏi: ?
    - Unicode replacement character: �
    """
    if not isinstance(value, str):
        return False

    compact = "".join(value.split())

    return bool(compact) and set(compact).issubset({"?", "\ufffd"})


# ============================================================================
# Cross-process file lock
# ============================================================================

class _ProcessFileLock:
    """
    File lock dùng giữa nhiều process.

    Windows:
        msvcrt.locking

    Linux/macOS:
        fcntl.flock

    File lock đồng thời lưu timestamp của lần reload thành công gần nhất.
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
        """
        Thử lấy exclusive lock.

        Trả về:
            True: lấy lock thành công.
            False: process khác đang giữ lock.
        """
        self.lock_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        try:
            # Không dùng a+b vì append mode luôn ghi ở cuối file,
            # ngay cả khi đã gọi seek(0).
            if self.lock_file.exists():
                self._file = open(self.lock_file, "r+b")
            else:
                self._file = open(self.lock_file, "w+b")

            if os.name == "nt":
                self._acquire_windows_lock()
            else:
                self._acquire_unix_lock()

            return True

        except (BlockingIOError, OSError):
            self.release()
            return False

    def _acquire_windows_lock(self) -> None:
        import msvcrt

        if self._file is None:
            raise RuntimeError("Lock file is not open")

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

    def _acquire_unix_lock(self) -> None:
        import fcntl

        if self._file is None:
            raise RuntimeError("Lock file is not open")

        fcntl.flock(
            self._file.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )

    def release(self) -> None:
        """Giải phóng lock và đóng file."""
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
            # Không làm crash service chỉ vì unlock lỗi.
            pass

        try:
            self._file.close()
        except Exception:
            pass

        self._file = None

    def read_reload_timestamp(self) -> float:
        """
        Đọc timestamp lần reload thành công gần nhất.

        Phải gọi khi đang giữ lock.
        """
        if self._file is None:
            return 0.0

        try:
            self._file.seek(0)

            raw = (
                self._file
                .read()
                .decode("ascii", errors="ignore")
                .strip("\0 \t\r\n")
            )

            return float(raw) if raw else 0.0

        except (OSError, ValueError):
            return 0.0

    def write_reload_timestamp(self, timestamp: float) -> None:
        """
        Lưu timestamp lần reload thành công.

        Phải gọi khi đang giữ lock.
        """
        if self._file is None:
            return

        self._file.seek(0)
        self._file.write(f"{timestamp:.6f}".encode("ascii"))
        self._file.truncate()
        self._file.flush()

        try:
            os.fsync(self._file.fileno())
        except OSError:
            # fsync có thể không được hỗ trợ trên một số filesystem.
            pass

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


# ============================================================================
# LM Studio recovery
# ============================================================================

class LmStudioRecovery:
    """
    Theo dõi lỗi liên tiếp và tự reload model LM Studio.

    Các lỗi nên báo vào class này:
    - Completion không có content.
    - Content rỗng.
    - Content bị hỏng toàn dấu '?' hoặc ký tự '�'.
    - Request timeout.
    - Connection error.
    - HTTP 500, 502, 503, 504.

    Các lỗi không nên reload:
    - HTTP 400: payload/config sai.
    - HTTP 401/403: API key hoặc permission sai.
    - HTTP 404: endpoint/model sai.
    - HTTP 422: request validation sai.
    - HTTP 429: rate limit.
    """

    # Những HTTP status có khả năng là lỗi tạm thời của server/model.
    RELOADABLE_HTTP_STATUSES = {
        500,
        502,
        503,
        504,
    }

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
        failed_reload_retry_seconds: float = 10.0,
        model_ready_wait_seconds: float = 2.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model_key = model_key
        self.instance_id = instance_id

        self.api_key = api_key
        self.enabled = enabled

        self.cooldown_seconds = max(
            0.0,
            float(cooldown_seconds),
        )

        self.timeout_seconds = max(
            1.0,
            float(timeout_seconds),
        )

        self.failed_reload_retry_seconds = max(
            1.0,
            float(failed_reload_retry_seconds),
        )

        self.model_ready_wait_seconds = max(
            0.0,
            float(model_ready_wait_seconds),
        )

        self.unload_first = unload_first

        self.failure_threshold = max(
            1,
            int(failure_threshold),
        )

        self.log = logger
        self.session = session or requests.Session()
        self.lock_file = Path(lock_file)

        self._thread_lock = threading.RLock()

        self._consecutive_failures = 0

        # monotonic dùng nội bộ process.
        self._last_successful_reload_at = 0.0
        self._last_failed_reload_at = 0.0

        # Ngăn nhiều thread trong cùng process cùng reload.
        self._reload_in_progress = False

    # ----------------------------------------------------------------------
    # Public state
    # ----------------------------------------------------------------------

    @property
    def consecutive_failures(self) -> int:
        with self._thread_lock:
            return self._consecutive_failures

    @property
    def reload_in_progress(self) -> bool:
        with self._thread_lock:
            return self._reload_in_progress

    # ----------------------------------------------------------------------
    # Success handling
    # ----------------------------------------------------------------------

    def record_success(self) -> None:
        """
        Gọi sau khi nhận được completion hợp lệ.

        Reset chuỗi lỗi liên tiếp.
        """
        with self._thread_lock:
            previous = self._consecutive_failures
            self._consecutive_failures = 0

        if previous > 0:
            self.log.info(
                "[LM-STUDIO] Valid completion received; "
                "resetting consecutive failure count from %d to 0.",
                previous,
            )

    # ----------------------------------------------------------------------
    # Failure handling
    # ----------------------------------------------------------------------

    def record_invalid_completion(
        self,
        reason: str,
    ) -> bool:
        """
        Giữ tương thích với code service hiện tại.

        Dùng khi:
        - choices[0].message.content không tồn tại;
        - content rỗng;
        - content bị corrupted;
        - output không hợp lệ.

        Trả về True nếu reload thành công.
        """
        return self.record_failure(
            reason=reason,
            category="invalid-completion",
            reload_allowed=True,
        )

    def record_transport_failure(
        self,
        reason: str,
    ) -> bool:
        """
        Dùng khi timeout hoặc connection error.

        Ví dụ:
            recovery.record_transport_failure("request timeout")
            recovery.record_transport_failure("connection refused")
        """
        return self.record_failure(
            reason=reason,
            category="transport",
            reload_allowed=True,
        )

    def record_http_failure(
        self,
        status_code: int,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Xử lý HTTP failure.

        Chỉ HTTP 5xx được định nghĩa trong RELOADABLE_HTTP_STATUSES
        mới được tính vào chuỗi lỗi reload.
        """
        message = reason or f"HTTP {status_code}"

        if status_code not in self.RELOADABLE_HTTP_STATUSES:
            self.log.warning(
                "[LM-STUDIO] HTTP failure is not reloadable. "
                "status=%d, reason=%s",
                status_code,
                message,
            )
            return False

        return self.record_failure(
            reason=message,
            category=f"http-{status_code}",
            reload_allowed=True,
        )

    def record_failure(
        self,
        *,
        reason: str,
        category: str,
        reload_allowed: bool,
    ) -> bool:
        """
        Ghi nhận một lỗi LM Studio.

        Reload chỉ xảy ra khi:
        - recovery được bật;
        - lỗi được phép reload;
        - số lỗi liên tiếp đạt threshold;
        - không bị cooldown;
        - không có thread/process khác đang reload.
        """
        if not reload_allowed:
            return False

        if not self.enabled:
            self.log.debug(
                "[LM-STUDIO] Recovery disabled. "
                "category=%s, reason=%s",
                category,
                reason,
            )
            return False

        with self._thread_lock:
            self._consecutive_failures += 1
            failure_count = self._consecutive_failures

        self.log.warning(
            "[LM-STUDIO] Failure detected. "
            "category=%s, reason=%s, consecutiveFailures=%d/%d",
            category,
            reason,
            failure_count,
            self.failure_threshold,
        )

        if failure_count < self.failure_threshold:
            return False

        reloaded = self._attempt_reload(
            reason=f"{category}: {reason}",
        )

        if reloaded:
            with self._thread_lock:
                self._consecutive_failures = 0

        return reloaded

    # ----------------------------------------------------------------------
    # Reload coordination
    # ----------------------------------------------------------------------

    def _attempt_reload(
        self,
        reason: str,
    ) -> bool:
        """
        Thử reload model.

        Kiểm tra:
        1. Thread khác trong cùng process đang reload hay không.
        2. Cooldown nội bộ process.
        3. Delay sau lần reload thất bại.
        4. File lock giữa nhiều process.
        5. Cooldown được chia sẻ giữa các process.
        """
        now_monotonic = time.monotonic()

        with self._thread_lock:
            if self._reload_in_progress:
                self.log.warning(
                    "[LM-STUDIO] Reload skipped because another "
                    "thread is already reloading the model."
                )
                return False

            successful_remaining = (
                self.cooldown_seconds
                - (
                    now_monotonic
                    - self._last_successful_reload_at
                )
            )

            if (
                self._last_successful_reload_at > 0
                and successful_remaining > 0
            ):
                self.log.warning(
                    "[LM-STUDIO] Reload skipped because successful "
                    "reload cooldown is active for %.0f more seconds.",
                    successful_remaining,
                )
                return False

            failed_remaining = (
                self.failed_reload_retry_seconds
                - (
                    now_monotonic
                    - self._last_failed_reload_at
                )
            )

            if (
                self._last_failed_reload_at > 0
                and failed_remaining > 0
            ):
                self.log.warning(
                    "[LM-STUDIO] Reload skipped because the previous "
                    "reload attempt failed. Retry allowed in %.0f seconds.",
                    failed_remaining,
                )
                return False

            self._reload_in_progress = True

        process_lock = _ProcessFileLock(
            self.lock_file,
            self.log,
        )

        try:
            if not process_lock.acquire():
                self.log.warning(
                    "[LM-STUDIO] Another process is already "
                    "reloading the model; skipping duplicate reload."
                )
                return False

            # Kiểm tra cooldown chia sẻ giữa các process.
            wall_now = time.time()
            shared_last_reload_at = (
                process_lock.read_reload_timestamp()
            )

            if shared_last_reload_at > 0:
                shared_remaining = (
                    self.cooldown_seconds
                    - (
                        wall_now
                        - shared_last_reload_at
                    )
                )

                if shared_remaining > 0:
                    self.log.warning(
                        "[LM-STUDIO] Reload skipped because another "
                        "process reloaded the model recently. "
                        "Cooldown remaining: %.0f seconds.",
                        shared_remaining,
                    )

                    # Đồng bộ cooldown local.
                    with self._thread_lock:
                        self._last_successful_reload_at = (
                            time.monotonic()
                        )

                    return False

            self.log.warning(
                "[LM-STUDIO] Starting model recovery. "
                "model=%s, instanceId=%s, reason=%s",
                self.model_key,
                self.instance_id or "-",
                reason,
            )

            reloaded = self._reload_model(reason)

            if reloaded:
                completed_monotonic = time.monotonic()
                completed_wall_time = time.time()

                with self._thread_lock:
                    self._last_successful_reload_at = (
                        completed_monotonic
                    )
                    self._last_failed_reload_at = 0.0

                # Chỉ ghi shared cooldown sau khi reload thành công.
                process_lock.write_reload_timestamp(
                    completed_wall_time
                )

                if self.model_ready_wait_seconds > 0:
                    self.log.info(
                        "[LM-STUDIO] Waiting %.1f seconds "
                        "for the model to become ready.",
                        self.model_ready_wait_seconds,
                    )
                    time.sleep(
                        self.model_ready_wait_seconds
                    )

                return True

            with self._thread_lock:
                self._last_failed_reload_at = time.monotonic()

            return False

        except Exception:
            with self._thread_lock:
                self._last_failed_reload_at = time.monotonic()

            self.log.exception(
                "[LM-STUDIO] Unexpected error during model recovery."
            )
            return False

        finally:
            process_lock.release()

            with self._thread_lock:
                self._reload_in_progress = False

    # ----------------------------------------------------------------------
    # LM Studio model API
    # ----------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.api_key:
            headers["Authorization"] = (
                f"Bearer {self.api_key}"
            )

        return headers

    def _reload_model(
        self,
        reason: str,
    ) -> bool:
        """
        Reload model bằng LM Studio API.

        Flow:
            unload instance hiện tại nếu được cấu hình;
            load model lại.
        """
        headers = self._headers()

        try:
            if self.unload_first:
                self._unload_model(headers)

            loaded = self.session.post(
                f"{self.base_url}/api/v1/models/load",
                json={
                    "model": self.model_key,
                },
                headers=headers,
                timeout=(
                    min(10.0, self.timeout_seconds),
                    self.timeout_seconds,
                ),
            )

            if not loaded.ok:
                self.log.error(
                    "[LM-STUDIO] Model load failed. "
                    "model=%s, status=%d, body=%s",
                    self.model_key,
                    loaded.status_code,
                    self._short_body(loaded.text, 600),
                )
                return False

            self.log.warning(
                "[LM-STUDIO] Model '%s' reloaded successfully. "
                "reason=%s",
                self.model_key,
                reason,
            )

            return True

        except requests.Timeout as exc:
            self.log.error(
                "[LM-STUDIO] Model reload timed out. "
                "model=%s, error=%s",
                self.model_key,
                exc,
            )
            return False

        except requests.ConnectionError as exc:
            self.log.error(
                "[LM-STUDIO] Could not connect to LM Studio "
                "during reload. model=%s, error=%s",
                self.model_key,
                exc,
            )
            return False

        except requests.RequestException as exc:
            self.log.error(
                "[LM-STUDIO] Reload request failed. "
                "model=%s, error=%s",
                self.model_key,
                exc,
            )
            return False

    def _unload_model(
        self,
        headers: dict[str, str],
    ) -> bool:
        """
        Unload model instance.

        Nếu unload lỗi, vẫn tiếp tục thực hiện load.
        """
        if not self.instance_id:
            self.log.warning(
                "[LM-STUDIO] unload_first=true but instance_id "
                "is not configured; skipping unload."
            )
            return False

        try:
            unloaded = self.session.post(
                f"{self.base_url}/api/v1/models/unload",
                json={
                    "instance_id": self.instance_id,
                },
                headers=headers,
                timeout=(
                    min(10.0, self.timeout_seconds),
                    self.timeout_seconds,
                ),
            )

            if not unloaded.ok:
                self.log.warning(
                    "[LM-STUDIO] Model unload failed; "
                    "continuing with load. "
                    "instanceId=%s, status=%d, body=%s",
                    self.instance_id,
                    unloaded.status_code,
                    self._short_body(unloaded.text, 300),
                )
                return False

            self.log.info(
                "[LM-STUDIO] Model instance unloaded successfully. "
                "instanceId=%s",
                self.instance_id,
            )

            return True

        except requests.Timeout as exc:
            self.log.warning(
                "[LM-STUDIO] Model unload timed out; "
                "continuing with load. error=%s",
                exc,
            )
            return False

        except requests.RequestException as exc:
            self.log.warning(
                "[LM-STUDIO] Model unload request failed; "
                "continuing with load. error=%s",
                exc,
            )
            return False

    @staticmethod
    def _short_body(
        value: Optional[str],
        limit: int,
    ) -> str:
        if not value:
            return ""

        text = value.strip()

        if len(text) <= limit:
            return text

        return text[:limit] + "..."