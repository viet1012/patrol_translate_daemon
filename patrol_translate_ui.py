from __future__ import annotations

import calendar
import queue
import threading
from dataclasses import replace
from datetime import date, datetime, timedelta
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any, Optional

from patrol_translate_service import (
    Config,
    DbConnection,
    ProcessResult,
    ensure_work_state_table,
    fetch_records_for_review,
    load_config,
    normalize,
    process_pairs,
    setup_logging,
)


def short_text(value: Any, limit: int = 95) -> str:
    text = normalize(value) or ""
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ============================================================
# Date range presets
# ============================================================
# Gộp toàn bộ lựa chọn phạm vi ngày về MỘT nguồn trạng thái duy nhất
# (dropdown) thay vì nhiều nút bấm rời rạc ("Tuần này", "7 ngày gần
# nhất", radio "Toàn bộ DB"...) dễ khiến người dùng không rõ phạm vi
# nào đang thực sự được áp dụng.

_DATE_PRESET_KEYS: tuple[str, ...] = (
    "all",
    "today",
    "yesterday",
    "last_7_days",
    "last_14_days",
    "last_30_days",
    "this_week",
    "last_week",
    "this_month",
    "last_month",
    "custom",
)

_DATE_PRESET_LABELS: dict[str, str] = {
    "all": "Toàn bộ dữ liệu",
    "today": "Hôm nay",
    "yesterday": "Hôm qua",
    "last_7_days": "7 ngày gần nhất",
    "last_14_days": "14 ngày gần nhất",
    "last_30_days": "30 ngày gần nhất",
    "this_week": "Tuần này",
    "last_week": "Tuần trước",
    "this_month": "Tháng này",
    "last_month": "Tháng trước",
    "custom": "Tùy chỉnh...",
}


def _compute_preset_range(key: str) -> tuple[Optional[date], Optional[date]]:
    """
    Trả về (date_from, date_to) cho một preset. (None, None) nghĩa là
    không lọc theo ngày (toàn bộ DB). Với "custom" hàm này không được
    gọi — ngày do người dùng tự nhập.
    """
    today = date.today()

    if key == "all":
        return None, None
    if key == "today":
        return today, today
    if key == "yesterday":
        y = today - timedelta(days=1)
        return y, y
    if key == "last_7_days":
        return today - timedelta(days=6), today
    if key == "last_14_days":
        return today - timedelta(days=13), today
    if key == "last_30_days":
        return today - timedelta(days=29), today
    if key == "this_week":
        monday = today - timedelta(days=today.weekday())
        return monday, monday + timedelta(days=6)
    if key == "last_week":
        this_monday = today - timedelta(days=today.weekday())
        last_monday = this_monday - timedelta(days=7)
        return last_monday, last_monday + timedelta(days=6)
    if key == "this_month":
        first_day = today.replace(day=1)
        last_day = today.replace(
            day=calendar.monthrange(today.year, today.month)[1]
        )
        return first_day, last_day
    if key == "last_month":
        first_of_this_month = today.replace(day=1)
        last_day_prev = first_of_this_month - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        return first_day_prev, last_day_prev

    raise ValueError(f"Preset không hợp lệ: {key!r}")


class PatrolTranslateUi:
    def __init__(self, root: tk.Tk, cfg: Config):
        self.root = root
        self.cfg = replace(cfg, use_db_applock=False)
        self.events: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.records: dict[str, dict[str, Any]] = {}
        self.worker_running = False
        self.loading_running = False
        self.load_generation = 0
        self.auto_after_id: Optional[str] = None

        self.root.title("Patrol Translate - Stable")
        self.root.geometry("1450x860")
        self.root.minsize(1120, 720)

        # --- Phạm vi ngày: một nguồn trạng thái duy nhất ---
        default_key = "last_7_days"
        self.date_preset_key_var = tk.StringVar(value=default_key)
        self.date_preset_display_var = tk.StringVar(
            value=_DATE_PRESET_LABELS[default_key]
        )
        date_from, date_to = _compute_preset_range(default_key)
        self.from_date_var = tk.StringVar(
            value=(date_from or date.today()).isoformat()
        )
        self.to_date_var = tk.StringVar(
            value=(date_to or date.today()).isoformat()
        )
        self.range_summary_var = tk.StringVar(value="")

        self.mode_var = tk.StringVar(value="jp")
        self.single_group_var = tk.StringVar(value="comment_countermeasure")
        self.pending_only_var = tk.BooleanVar(value=False)
        self.auto_var = tk.BooleanVar(value=False)
        self.auto_interval_var = tk.StringVar(
            value=str(max(5, int(self.cfg.poll_interval_seconds)))
        )

        self.status_var = tk.StringVar(value="Sẵn sàng")
        self.summary_var = tk.StringVar(value="Chưa tải dữ liệu")
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_text_var = tk.StringVar(value="0 / 0")

        self._configure_style()
        self._build()

        # Cập nhật nhãn tóm tắt phạm vi mỗi khi 2 ô ngày đổi giá trị
        # (kể cả khi gõ tay ở chế độ Tùy chỉnh).
        self.from_date_var.trace_add("write", lambda *_: self._update_range_summary())
        self.to_date_var.trace_add("write", lambda *_: self._update_range_summary())
        self._update_range_summary()

        self.root.after(150, self._drain_events)
        self.load_records(show_error=False)

    # --------------------------------------------------------
    # UI
    # --------------------------------------------------------

    def _configure_style(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Title.TLabel", font=("Segoe UI", 20, "bold"))
        style.configure("Sub.TLabel", font=("Segoe UI", 10), foreground="#4b5563")
        style.configure("Primary.TButton", font=("Segoe UI", 11, "bold"), padding=(18, 11))
        style.configure("Danger.TButton", font=("Segoe UI", 10, "bold"), padding=(14, 9))
        style.configure("Treeview", font=("Segoe UI", 9), rowheight=31)
        style.configure("Treeview.Heading", font=("Segoe UI", 9, "bold"))
        style.configure("Status.TLabel", font=("Segoe UI", 10, "bold"))
        style.configure("RangeSummary.TLabel", font=("Segoe UI", 9, "italic"), foreground="#2563eb")

    def _build(self) -> None:
        root_frame = ttk.Frame(self.root, padding=16)
        root_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            root_frame,
            text="Patrol Translate",
            style="Title.TLabel",
        ).pack(anchor=tk.W)
        ttk.Label(
            root_frame,
            text=(
                "Dịch Việt ↔ Nhật ổn định, hỗ trợ khoảng ngày, "
                "dòng đã chọn, tự động và qr_key."
            ),
            style="Sub.TLabel",
        ).pack(anchor=tk.W, pady=(0, 12))

        # ---------------- Phạm vi dữ liệu ----------------
        scope_frame = ttk.LabelFrame(root_frame, text="Phạm vi dữ liệu", padding=12)
        scope_frame.pack(fill=tk.X)

        scope_row = ttk.Frame(scope_frame)
        scope_row.pack(fill=tk.X)

        ttk.Label(scope_row, text="Khoảng ngày:").pack(side=tk.LEFT)
        self.date_preset_combo = ttk.Combobox(
            scope_row,
            textvariable=self.date_preset_display_var,
            state="readonly",
            width=20,
            values=tuple(_DATE_PRESET_LABELS[k] for k in _DATE_PRESET_KEYS),
        )
        self.date_preset_combo.pack(side=tk.LEFT, padx=(8, 16))
        self.date_preset_combo.bind(
            "<<ComboboxSelected>>", self._on_preset_changed
        )

        ttk.Label(scope_row, text="Từ").pack(side=tk.LEFT)
        self.from_entry = ttk.Entry(
            scope_row, textvariable=self.from_date_var, width=12
        )
        self.from_entry.pack(side=tk.LEFT, padx=(6, 12))

        ttk.Label(scope_row, text="Đến").pack(side=tk.LEFT)
        self.to_entry = ttk.Entry(
            scope_row, textvariable=self.to_date_var, width=12
        )
        self.to_entry.pack(side=tk.LEFT, padx=(6, 0))

        ttk.Checkbutton(
            scope_row,
            text="Chỉ hiện chưa dịch",
            variable=self.pending_only_var,
            command=lambda: self.load_records(show_error=True),
        ).pack(side=tk.RIGHT)

        summary_row = ttk.Frame(scope_frame)
        summary_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(
            summary_row,
            textvariable=self.range_summary_var,
            style="RangeSummary.TLabel",
        ).pack(side=tk.LEFT)

        # ---------------- Chế độ dịch ----------------
        mode_frame = ttk.LabelFrame(root_frame, text="Chế độ dịch", padding=12)
        mode_frame.pack(fill=tk.X, pady=(10, 0))

        mode_row = ttk.Frame(mode_frame)
        mode_row.pack(fill=tk.X)

        ttk.Label(mode_row, text="Chế độ:").pack(side=tk.LEFT)
        ttk.Radiobutton(
            mode_row, text="Full tiếng Nhật (4 cặp)",
            variable=self.mode_var, value="jp",
            command=self._mode_changed,
        ).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Radiobutton(
            mode_row, text="Full tiếng Việt (4 cặp)",
            variable=self.mode_var, value="vi",
            command=self._mode_changed,
        ).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Radiobutton(
            mode_row, text="Một nhóm cột",
            variable=self.mode_var, value="single",
            command=self._mode_changed,
        ).pack(side=tk.LEFT, padx=(10, 0))

        self.group_combo = ttk.Combobox(
            mode_row,
            textvariable=self.single_group_var,
            state="readonly",
            width=34,
            values=(
                "comment_countermeasure",
                "after_comment",
                "hse_comment",
            ),
        )
        self.group_combo.pack(side=tk.LEFT, padx=(8, 0))
        self.group_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: self.load_records(True),
        )

        # Hiển thị tên dễ hiểu thay vì key kỹ thuật
        self.group_combo_display = {
            "comment_countermeasure": "Comment + Countermeasure",
            "after_comment": "After Comment",
            "hse_comment": "HSE Comment",
        }
        self.group_combo.configure(
            values=tuple(self.group_combo_display.values())
        )
        self.group_combo.set(
            self.group_combo_display[self.single_group_var.get()]
        )

        self.single_direction_var = tk.StringVar(value="jp")
        self.single_jp_radio = ttk.Radiobutton(
            mode_row,
            text="→ Nhật",
            variable=self.single_direction_var,
            value="jp",
            command=self._single_direction_changed,
        )
        self.single_jp_radio.pack(side=tk.LEFT, padx=(8, 0))
        self.single_vi_radio = ttk.Radiobutton(
            mode_row,
            text="→ Việt",
            variable=self.single_direction_var,
            value="vi",
            command=self._single_direction_changed,
        )
        self.single_vi_radio.pack(side=tk.LEFT, padx=(4, 0))

        # ---------------- Thao tác ----------------
        action_frame = ttk.LabelFrame(root_frame, text="Thao tác", padding=12)
        action_frame.pack(fill=tk.X, pady=(10, 0))

        action_row = ttk.Frame(action_frame)
        action_row.pack(fill=tk.X)

        self.load_btn = ttk.Button(
            action_row, text="Tải danh sách", command=lambda: self.load_records(True)
        )
        self.load_btn.pack(side=tk.LEFT)

        self.translate_all_btn = ttk.Button(
            action_row,
            text="Dịch ngay",
            style="Primary.TButton",
            command=self.translate_now,
        )
        self.translate_all_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.translate_selected_btn = ttk.Button(
            action_row,
            text="Chỉ dịch dòng đã chọn",
            command=self.translate_selected,
        )
        self.translate_selected_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.stop_btn = ttk.Button(
            action_row,
            text="Dừng",
            style="Danger.TButton",
            command=self.stop_translation,
            state=tk.DISABLED,
        )
        self.stop_btn.pack(side=tk.LEFT, padx=(8, 0))

        ttk.Separator(action_row, orient=tk.VERTICAL).pack(
            side=tk.LEFT, fill=tk.Y, padx=14
        )

        ttk.Checkbutton(
            action_row,
            text="Tự động dịch",
            variable=self.auto_var,
            command=self._toggle_auto,
        ).pack(side=tk.LEFT)

        ttk.Label(action_row, text="Mỗi").pack(side=tk.LEFT, padx=(8, 4))
        ttk.Entry(action_row, textvariable=self.auto_interval_var, width=6).pack(side=tk.LEFT)
        ttk.Label(action_row, text="giây").pack(side=tk.LEFT, padx=(4, 0))

        progress_frame = ttk.Frame(root_frame)
        progress_frame.pack(fill=tk.X, pady=(12, 8))
        ttk.Progressbar(
            progress_frame,
            variable=self.progress_var,
            maximum=100,
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(
            progress_frame,
            textvariable=self.progress_text_var,
            width=14,
            anchor=tk.E,
        ).pack(side=tk.LEFT, padx=(8, 0))

        table_frame = ttk.Frame(root_frame)
        table_frame.pack(fill=tk.BOTH, expand=True)

        columns = (
            "no", "id", "qr_key", "created", "pair",
            "source", "target", "status", "note",
        )
        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            selectmode="extended",
        )

        headings = {
            "no": "STT",
            "id": "ID",
            "qr_key": "QR KEY",
            "created": "Ngày tạo",
            "pair": "Cặp cột",
            "source": "Nội dung nguồn",
            "target": "Bản dịch",
            "status": "Trạng thái",
            "note": "Ghi chú",
        }
        widths = {
            "no": 55, "id": 80, "qr_key": 115, "created": 135,
            "pair": 220, "source": 300, "target": 300,
            "status": 100, "note": 180,
        }

        for name in columns:
            self.tree.heading(name, text=headings[name])
            self.tree.column(name, width=widths[name], minwidth=45, anchor=tk.W)
        self.tree.column("no", anchor=tk.CENTER)
        self.tree.column("status", anchor=tk.CENTER)

        yscroll = ttk.Scrollbar(
            table_frame, orient=tk.VERTICAL, command=self.tree.yview
        )
        xscroll = ttk.Scrollbar(
            table_frame, orient=tk.HORIZONTAL, command=self.tree.xview
        )
        self.tree.configure(
            yscrollcommand=yscroll.set,
            xscrollcommand=xscroll.set,
        )

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        self.tree.tag_configure("pending", background="#fff7ed")
        self.tree.tag_configure("working", background="#eff6ff")
        self.tree.tag_configure("done", background="#ecfdf5")
        self.tree.tag_configure("error", background="#fef2f2")
        self.tree.tag_configure("translated", background="#f8fafc")

        footer = ttk.Frame(root_frame)
        footer.pack(fill=tk.X, pady=(8, 0))
        ttk.Label(
            footer, textvariable=self.status_var, style="Status.TLabel"
        ).pack(side=tk.LEFT)
        ttk.Label(
            footer, textvariable=self.summary_var
        ).pack(side=tk.RIGHT)

        self._apply_preset_state(self.date_preset_key_var.get())
        self._mode_changed(load=False)

    # --------------------------------------------------------
    # Date range preset handling
    # --------------------------------------------------------

    def _on_preset_changed(self, _event: Any = None) -> None:
        display = self.date_preset_display_var.get()
        key = next(
            (k for k, label in _DATE_PRESET_LABELS.items() if label == display),
            "custom",
        )
        self.date_preset_key_var.set(key)
        self._apply_preset_state(key)

        if key != "custom":
            self.load_records(show_error=True)

    def _apply_preset_state(self, key: str) -> None:
        if key == "custom":
            self.from_entry.configure(state=tk.NORMAL)
            self.to_entry.configure(state=tk.NORMAL)
            return

        date_from, date_to = _compute_preset_range(key)
        if key == "all":
            # Không lọc theo ngày — vẫn hiện khoảng ngày rộng nhất có thể
            # để ô nhập không bị trống khó hiểu, nhưng khóa lại vì không
            # dùng để lọc.
            self.from_entry.configure(state=tk.NORMAL)
            self.to_entry.configure(state=tk.NORMAL)
            self.from_entry.configure(state="readonly")
            self.to_entry.configure(state="readonly")
            self.from_date_var.set("(không lọc)")
            self.to_date_var.set("(không lọc)")
            return

        assert date_from is not None and date_to is not None
        self.from_entry.configure(state=tk.NORMAL)
        self.to_entry.configure(state=tk.NORMAL)
        self.from_date_var.set(date_from.isoformat())
        self.to_date_var.set(date_to.isoformat())
        self.from_entry.configure(state="readonly")
        self.to_entry.configure(state="readonly")

    def _update_range_summary(self) -> None:
        key = self.date_preset_key_var.get()
        if key == "all":
            self.range_summary_var.set("→ Áp dụng: toàn bộ dữ liệu, không lọc theo ngày")
            return
        try:
            date_from = self._parse_date(self.from_date_var.get(), "Từ ngày")
            date_to = self._parse_date(self.to_date_var.get(), "Đến ngày")
        except ValueError:
            self.range_summary_var.set("→ Ngày nhập chưa hợp lệ (YYYY-MM-DD)")
            return
        if date_from > date_to:
            self.range_summary_var.set("→ Từ ngày đang lớn hơn Đến ngày")
            return
        days = (date_to - date_from).days + 1
        self.range_summary_var.set(
            f"→ Áp dụng: {date_from.strftime('%d/%m/%Y')} – "
            f"{date_to.strftime('%d/%m/%Y')} ({days} ngày)"
        )

    # --------------------------------------------------------
    # Selection and filters
    # --------------------------------------------------------

    @staticmethod
    def _pair_label(pair: tuple[str, str]) -> str:
        return f"{pair[0]} → {pair[1]}"

    def _selected_group_key(self) -> str:
        current = self.group_combo.get().strip()
        for key, display in self.group_combo_display.items():
            if current == display:
                return key
        raise ValueError("Nhóm cột đang chọn không hợp lệ.")

    def _find_pair(self, source: str, target: str) -> tuple[str, str]:
        expected = (source, target)
        if expected not in self.cfg.translate_columns:
            raise ValueError(
                f"Thiếu cặp {source} → {target} trong TRANSLATE_COLUMNS."
            )
        return expected

    def _selected_group_pairs(self, direction: str) -> tuple[tuple[str, str], ...]:
        group = self._selected_group_key()

        if direction == "jp":
            groups = {
                "comment_countermeasure": (
                    self._find_pair("comment", "comment_jp"),
                    self._find_pair("countermeasure", "countermeasure_jp"),
                ),
                "after_comment": (
                    self._find_pair("at_comment", "at_comment_jp"),
                ),
                "hse_comment": (
                    self._find_pair("hse_comment", "hse_comment_jp"),
                ),
            }
        else:
            groups = {
                "comment_countermeasure": (
                    self._find_pair("comment_jp", "comment"),
                    self._find_pair("countermeasure_jp", "countermeasure"),
                ),
                "after_comment": (
                    self._find_pair("at_comment_jp", "at_comment"),
                ),
                "hse_comment": (
                    self._find_pair("hse_comment_jp", "hse_comment"),
                ),
            }

        return groups[group]

    @staticmethod
    def _is_japanese_column(name: str) -> bool:
        value = name.lower()
        return value.endswith(("_jp", "_ja", "_japanese")) or "japanese" in value

    def _selected_pairs(self) -> tuple[tuple[str, str], ...]:
        mode = self.mode_var.get()
        if mode == "single":
            # Nhóm Comment + Countermeasure sẽ trả về 2 cặp cùng lúc.
            # Chiều dịch lấy theo lựa chọn gần nhất: mặc định là Việt → Nhật.
            direction = self.single_direction_var.get()
            return self._selected_group_pairs(direction)

        if mode == "jp":
            pairs = tuple(
                pair for pair in self.cfg.translate_columns
                if self._is_japanese_column(pair[1])
            )
        else:
            pairs = tuple(
                pair for pair in self.cfg.translate_columns
                if self._is_japanese_column(pair[0])
                and not self._is_japanese_column(pair[1])
            )

        if not pairs:
            raise ValueError(
                "Không tìm thấy cặp cột phù hợp trong TRANSLATE_COLUMNS."
            )
        return pairs

    @staticmethod
    def _parse_date(value: str, label: str) -> date:
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"{label} phải có định dạng YYYY-MM-DD.") from exc

    def _selected_range(self) -> tuple[Optional[date], Optional[date]]:
        if self.date_preset_key_var.get() == "all":
            return None, None
        date_from = self._parse_date(self.from_date_var.get(), "Từ ngày")
        date_to = self._parse_date(self.to_date_var.get(), "Đến ngày")
        if date_from > date_to:
            raise ValueError("Từ ngày không được lớn hơn Đến ngày.")
        return date_from, date_to

    def _single_direction_changed(self) -> None:
        if self.mode_var.get() == "single":
            self.load_records(show_error=True)

    def _mode_changed(self, load: bool = True) -> None:
        mode = self.mode_var.get()
        self.group_combo.configure(
            state="readonly" if mode == "single" else "disabled"
        )

        single_state = tk.NORMAL if mode == "single" else tk.DISABLED
        self.single_jp_radio.configure(state=single_state)
        self.single_vi_radio.configure(state=single_state)

        # Full Nhật/Việt cũng đồng bộ chiều cho lần chuyển sang Một nhóm.
        if mode == "jp":
            self.single_direction_var.set("jp")
        elif mode == "vi":
            self.single_direction_var.set("vi")

        if load:
            self.load_records(show_error=True)

    # --------------------------------------------------------
    # Records
    # --------------------------------------------------------

    @staticmethod
    def _item_id(record: dict[str, Any]) -> str:
        return (
            f"{record['record_id']}::"
            f"{record['source_column']}::"
            f"{record['target_column']}"
        )

    def _status_of(self, record: dict[str, Any]) -> str:
        return record.get("runtime_status") or (
            "ĐÃ DỊCH" if record.get("target") else "CHỜ DỊCH"
        )

    def _tag_of(self, status: str) -> str:
        return {
            "CHỜ DỊCH": "pending",
            "ĐANG DỊCH": "working",
            "ĐÃ DỊCH": "done",
            "LỖI": "error",
            "BỎ QUA": "translated",
            "ĐÃ CÓ SẴN": "translated",
            "DỪNG": "translated",
        }.get(status, "translated")

    def _render_record(self, item_id: str) -> None:
        record = self.records[item_id]
        created = record.get("created_at")
        created_text = (
            created.strftime("%Y-%m-%d %H:%M")
            if isinstance(created, datetime)
            else str(created or "")
        )
        status = self._status_of(record)

        values = (
            record.get("row_no", ""),
            record["record_id"],
            record.get("qr_key", ""),
            created_text,
            self._pair_label((
                record["source_column"],
                record["target_column"],
            )),
            short_text(record.get("source")),
            short_text(record.get("target")) or "(Chưa có)",
            status,
            short_text(record.get("note"), 80),
        )

        if self.tree.exists(item_id):
            self.tree.item(item_id, values=values, tags=(self._tag_of(status),))
        else:
            self.tree.insert(
                "", tk.END, iid=item_id,
                values=values, tags=(self._tag_of(status),)
            )

    def load_records(self, show_error: bool = True) -> None:
        if self.worker_running:
            if show_error:
                messagebox.showinfo(
                    "Đang dịch",
                    "Không thể tải lại danh sách khi tác vụ dịch đang chạy.",
                )
            return

        if self.loading_running:
            if show_error:
                messagebox.showinfo(
                    "Đang tải",
                    "Danh sách đang được tải. Vui lòng chờ hoàn tất.",
                )
            return

        try:
            pairs = self._selected_pairs()
            date_from, date_to = self._selected_range()
        except Exception as exc:
            if show_error:
                messagebox.showerror("Bộ lọc không hợp lệ", str(exc))
            return

        self.loading_running = True
        self.load_generation += 1
        generation = self.load_generation

        self.status_var.set("Đang tải dữ liệu...")
        self._set_busy(False)

        def worker() -> None:
            db = None
            try:
                db = DbConnection(self.cfg)
                db.connect()
                ensure_work_state_table(db, self.cfg)
                rows = fetch_records_for_review(
                    db,
                    self.cfg,
                    pairs,
                    date_from,
                    date_to,
                    pending_only=self.pending_only_var.get(),
                )
                self.events.put(("records", (generation, rows)))
            except Exception as exc:
                self.events.put((
                    "load_error",
                    (generation, f"Không tải được dữ liệu:\n{exc}"),
                ))
            finally:
                if db is not None:
                    db.close()
                self.events.put(("load_finished", generation))

        threading.Thread(
            target=worker,
            name="LoadRecords",
            daemon=True,
        ).start()

    # --------------------------------------------------------
    # Translation
    # --------------------------------------------------------

    def translate_now(self) -> None:
        self._start_translation(selected_ids=None)

    def translate_selected(self) -> None:
        selected = list(self.tree.selection())
        if not selected:
            messagebox.showwarning(
                "Chưa chọn dòng",
                "Hãy chọn ít nhất một dòng trong bảng.",
            )
            return

        groups: dict[tuple[str, str], set[str]] = {}
        for item_id in selected:
            row = self.records.get(item_id)
            if not row:
                continue
            pair = (row["source_column"], row["target_column"])
            groups.setdefault(pair, set()).add(str(row["record_id"]))

        if not groups:
            messagebox.showwarning("Không có dữ liệu", "Dòng đã chọn không hợp lệ.")
            return

        self._start_translation(selected_groups=groups)

    def _start_translation(
        self,
        selected_ids: Optional[list[str]] = None,
        selected_groups: Optional[dict[tuple[str, str], set[str]]] = None,
    ) -> None:
        if self.worker_running:
            messagebox.showinfo("Đang chạy", "Một tác vụ dịch đang chạy.")
            return

        if self.loading_running:
            messagebox.showinfo(
                "Đang tải dữ liệu",
                "Vui lòng chờ tải danh sách hoàn tất rồi mới bắt đầu dịch.",
            )
            return

        try:
            date_from, date_to = self._selected_range()
            pairs = self._selected_pairs()
        except Exception as exc:
            messagebox.showerror("Không thể bắt đầu", str(exc))
            return

        if selected_groups is not None:
            pairs = tuple(selected_groups.keys())

        self.worker_running = True
        self.stop_event.clear()
        self._set_busy(True)
        self.progress_var.set(0)
        self.progress_text_var.set("0 dòng")
        self.status_var.set("Đang dịch...")

        def callback(event_name: str, row: dict[str, Any], result: ProcessResult) -> None:
            self.events.put(("progress", (event_name, row, result)))

        def worker() -> None:
            total = ProcessResult()
            try:
                if selected_groups is None:
                    total = process_pairs(
                        cfg=self.cfg,
                        pairs=pairs,
                        date_from=date_from,
                        date_to=date_to,
                        selected_ids=selected_ids,
                        stop_event=self.stop_event,
                        progress_callback=callback,
                    )
                else:
                    for pair, ids in selected_groups.items():
                        if self.stop_event.is_set():
                            total.stopped = True
                            break
                        partial = process_pairs(
                            cfg=self.cfg,
                            pairs=(pair,),
                            date_from=date_from,
                            date_to=date_to,
                            selected_ids=ids,
                            stop_event=self.stop_event,
                            progress_callback=callback,
                        )
                        total.scanned += partial.scanned
                        total.translated += partial.translated
                        total.skipped += partial.skipped
                        total.failed += partial.failed
                        total.stopped = total.stopped or partial.stopped

                self.events.put(("translation_finished", total))
            except Exception as exc:
                self.events.put(("error", f"Tác vụ dịch bị lỗi:\n{exc}"))
                self.events.put(("translation_finished", total))

        threading.Thread(
            target=worker,
            name="TranslateWorker",
            daemon=True,
        ).start()

    def stop_translation(self) -> None:
        # Nút Dừng đồng thời tắt Auto để tác vụ không tự khởi động lại
        # ngay sau khi worker hiện tại vừa dừng xong.
        if self.auto_var.get():
            self.auto_var.set(False)
        if self.auto_after_id is not None:
            self.root.after_cancel(self.auto_after_id)
            self.auto_after_id = None

        if self.worker_running:
            self.stop_event.set()
            self.status_var.set("Đang yêu cầu dừng... Tự động dịch đã tắt.")
        else:
            self.status_var.set("Tự động dịch đã tắt.")

    def _set_busy(self, busy: bool) -> None:
        controls_disabled = busy or self.loading_running
        state = tk.DISABLED if controls_disabled else tk.NORMAL
        self.load_btn.configure(state=state)
        self.translate_all_btn.configure(state=state)
        self.translate_selected_btn.configure(state=state)
        self.stop_btn.configure(state=tk.NORMAL if busy else tk.DISABLED)

    # --------------------------------------------------------
    # Auto mode
    # --------------------------------------------------------

    def _toggle_auto(self) -> None:
        if self.auto_var.get():
            try:
                interval = int(self.auto_interval_var.get().strip())
                if interval < 5:
                    raise ValueError
            except ValueError:
                self.auto_var.set(False)
                messagebox.showerror(
                    "Khoảng quét không hợp lệ",
                    "Khoảng tự động phải là số nguyên từ 5 giây trở lên.",
                )
                return
            self.status_var.set(f"Tự động dịch đang bật, mỗi {interval} giây.")
            self._schedule_auto(100)
        else:
            if self.auto_after_id is not None:
                self.root.after_cancel(self.auto_after_id)
                self.auto_after_id = None
            self.status_var.set("Tự động dịch đã tắt.")

    def _schedule_auto(self, delay_ms: Optional[int] = None) -> None:
        if not self.auto_var.get():
            return
        interval = max(5, int(self.auto_interval_var.get().strip()))
        self.auto_after_id = self.root.after(
            delay_ms if delay_ms is not None else interval * 1000,
            self._auto_tick,
        )

    def _auto_tick(self) -> None:
        self.auto_after_id = None
        if not self.auto_var.get():
            return
        if not self.worker_running and not self.loading_running:
            self.translate_now()
        self._schedule_auto()

    # --------------------------------------------------------
    # Event queue
    # --------------------------------------------------------

    def _drain_events(self) -> None:
        try:
            while True:
                event_name, payload = self.events.get_nowait()

                if event_name == "records":
                    generation, rows = payload
                    # Bỏ qua kết quả từ một yêu cầu load cũ nếu sau đó đã có
                    # yêu cầu load mới hơn. Không cho dữ liệu cũ ghi đè UI.
                    if generation != self.load_generation:
                        continue

                    self.tree.delete(*self.tree.get_children())
                    self.records.clear()
                    pending = 0
                    for index, row in enumerate(rows, start=1):
                        row["row_no"] = index
                        row["runtime_status"] = None
                        row["note"] = ""
                        item_id = self._item_id(row)
                        self.records[item_id] = row
                        self._render_record(item_id)
                        if not row.get("target"):
                            pending += 1
                    self.summary_var.set(
                        f"{len(rows)} dòng/cặp • {pending} đang chờ dịch"
                    )
                    self.status_var.set("Đã tải danh sách.")

                elif event_name == "load_finished":
                    generation = payload
                    if generation == self.load_generation:
                        self.loading_running = False
                        self._set_busy(self.worker_running)

                elif event_name == "load_error":
                    generation, message = payload
                    if generation == self.load_generation:
                        messagebox.showerror("Lỗi", str(message))
                        self.status_var.set("Không tải được dữ liệu.")

                elif event_name == "progress":
                    action, row, result = payload
                    item_id = self._item_id(row)
                    record = self.records.get(item_id)
                    if record is None:
                        record = {
                            **row,
                            "row_no": len(self.records) + 1,
                            "target": None,
                            "note": "",
                        }
                        self.records[item_id] = record

                    if action == "start":
                        record["runtime_status"] = "ĐANG DỊCH"
                        record["note"] = ""
                    elif action == "success":
                        record["runtime_status"] = "ĐÃ DỊCH"
                        record["target"] = row.get("translated")
                        record["note"] = "Đã ghi DB"
                    elif action == "failed":
                        record["runtime_status"] = "LỖI"
                        record["note"] = row.get("error", "")
                    elif action == "skipped":
                        # Service đã đọc lại DB và trả target thực tế.
                        # Vì vậy đây không phải "bỏ qua chưa rõ lý do",
                        # mà là target đã có sẵn do tiến trình khác ghi.
                        record["runtime_status"] = "ĐÃ CÓ SẴN"
                        record["target"] = row.get("target") or record.get("target")
                        record["note"] = row.get("note") or "Target đã có dữ liệu trong DB"

                    self._render_record(item_id)
                    self.progress_text_var.set(
                        f"Đã quét {result.scanned} • OK {result.translated} • Lỗi {result.failed}"
                    )

                elif event_name == "translation_finished":
                    result: ProcessResult = payload
                    self.worker_running = False
                    self._set_busy(False)
                    if result.stopped:
                        self.status_var.set(
                            f"Đã dừng. Thành công {result.translated}, lỗi {result.failed}."
                        )
                    else:
                        self.status_var.set(
                            f"Hoàn tất: dịch {result.translated}, "
                            f"bỏ qua {result.skipped}, lỗi {result.failed}."
                        )
                    self.progress_var.set(100)
                    self.load_records(show_error=False)

                elif event_name == "error":
                    messagebox.showerror("Lỗi", str(payload))
                    self.status_var.set("Có lỗi xảy ra.")

        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._drain_events)


def main() -> None:
    try:
        cfg = load_config()
        setup_logging(cfg)
    except Exception as exc:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Lỗi cấu hình", str(exc))
        root.destroy()
        return

    root = tk.Tk()
    PatrolTranslateUi(root, cfg)
    root.mainloop()


if __name__ == "__main__":
    main()