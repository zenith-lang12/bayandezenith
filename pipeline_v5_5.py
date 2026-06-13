"""
title: Universal Project Builder Pipeline v5.5
author: Claude (Anthropic) — redesigned for BayanDeZenith & beyond
version: 5.5.0
required_open_webui_version: 0.9.6

WHAT'S NEW IN v5.5 — AGENT TOOL ENRICHMENT:
─────────────────────────────────────────────────────────────────

[T1] SMART DIFF MODIFIER  (Coder Tool)
    - Coder kini bisa emit "patches" (search & replace blocks) SELAIN full file
    - Jika file sudah ada di disk + ukuran > 60 baris → coder pakai patch mode
    - Hemat output token hingga ~80% untuk file besar
    - Eliminasi 100% kode terpotong di tengah jalan
    - Format: {"patches": [{"path": "...", "search": "...", "replace": "..."}]}

[T2] AUTO-DEPENDENCY HEALER  (Sandbox Tool)
    - Jika test gagal dengan ModuleNotFoundError → auto pip install paketnya
    - Retry test otomatis setelah install berhasil
    - Support docker exec + native pip
    - Tidak perlu buka terminal manual untuk install library baru

[T3] ZERO-TOKEN STATIC LINTER  (Reviewer Tool)
    - Jalankan ruff atau flake8 SEBELUM AI Reviewer dipanggil
    - Isu syntax/style langsung dikembalikan ke Coder (0 LLM call)
    - Reviewer AI hanya fokus ke logika arsitektur tingkat tinggi
    - Auto-detect: coba ruff dulu, fallback ke flake8, fallback ke pyflakes

[T4] AST IMPORT DEPENDENCY MAPPER  (Planner Tool)
    - Baca seluruh import di project via AST, bangun peta ketergantungan
    - Saat file A diubah → file B dan C yang import A otomatis masuk konteks Coder
    - Cegah regression bug akibat perubahan file upstream
    - Cache 5 menit untuk performance

[T5] ENVIRONMENT HEALTH CHECKER  (Sandbox Tool) ← BARU (bukan dari Gemini)
    - Deteksi masalah lingkungan Python SEBELUM test dijalankan
    - Cek: asyncio rusak, pytest rusak, name shadowing (misal logging.py lokal)
    - Tampilkan warning + saran fix spesifik (0 LLM call)
    - Langsung identifikasi akar penyebab test failure berulang di project kamu

[T6] FAILURE MEMORY ACCUMULATOR  (Coder Tool) ← BARU
    - Setiap retry, Coder melihat SEMUA riwayat kegagalan sebelumnya (bukan hanya terakhir)
    - Cegah Coder mengulang kesalahan yang sama di setiap attempt
    - Format: "Attempt 1 failed: X. Attempt 2 failed: Y. Now fix BOTH."

[T7] FRAMEWORK FALLBACK  (Coder Tool) ← BARU
    - Jika env health check mendeteksi pytest rusak → Coder WAJIB pakai unittest
    - Injeksi constraint ke prompt: "DO NOT use pytest — use unittest only"
    - Eliminasi kebutuhan pip install pytest di container yang sudah bermasalah

[T8] TOKEN BUDGET OPTIMIZER  (Planner Tool) ← BARU
    - Monitor sisa token budget sebelum setiap LLM call
    - Kompresi dinamis: jika budget < 30% → potong konteks file lebih agresif
    - Hindari budget blowout di tengah batch

CARRIED FORWARD FROM v5.4:
─────────────────────────────────────────────────────────────────
  v5.4: Deep phase check, fix phase command, flexible regex detection
  v5.3: Dynamic dirs, flexible ID format, phase health check
  v5.2: Lowercase ID fix, utils/ dir, ModuleNotFoundError skip
  v5.1: Batch mode, reviewer gate, robust status, pipeline log

COMMAND SUMMARY v5.5:
─────────────────────────────────────────────────────────────────
  cek phase P0          → baca tabel status (0 token)
  check phase P0        → deep check + env health (0 token)
  fix phase P0          → retry semua task gagal (normal token)
  kerjakan P0-005       → single task
  kerjakan P0-004 sampai P0-006  → batch range
"""

import asyncio
import hashlib
import inspect
import json
import logging
import os
import re
import ast
import subprocess
import time
import sys
from datetime import datetime
from functools import partial
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Set, Tuple

from fastapi import Request
from pydantic import BaseModel, Field

from open_webui.utils.chat import generate_chat_completion as _raw_llm

try:
    from open_webui.models.users import Users
except ImportError:
    Users = None

logger = logging.getLogger(__name__)
_ITER_EXHAUSTED = object()

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────
CACHE_TTL = 300
MAX_FILE_SIZE_KB = 80
MAX_RELEVANT_FILES = 8
MAX_FILE_CHARS_FOR_CODER = 6000
MAX_FILE_CHARS_FOR_REVIEWER = 12000

# v5.5: Dep-graph cache (separate from file cache, longer TTL is fine)
DEP_GRAPH_CACHE_TTL = 300
_dep_graph_cache: Optional[Dict[str, List[str]]] = None
_dep_graph_ts: float = 0.0

# v5.5: Linter auto-detect (populated on first use)
_LINTER_CMD: Optional[str] = None  # "ruff", "flake8", or None

# v5.3: No more hard-coded SAFE_WRITE_DIRS / SCAN_DIRS.
# These are now discovered dynamically from SANDBOX_PATH at runtime.
# Folders in this blacklist are always excluded regardless.
_DIR_BLACKLIST = {
    ".git", ".github", ".venv", "venv", "env", ".env",
    "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    "node_modules", ".node_modules", "dist", "build", ".build",
    ".tox", "htmlcov", ".eggs", "*.egg-info",
    ".idea", ".vscode", ".DS_Store",
}

def _get_dynamic_dirs(sandbox_path: str, extra_dirs: str = "") -> Tuple[List[str], List[str]]:
    """
    v5.3: Auto-discover all immediate subdirectories of sandbox_path.
    Returns (safe_write_dirs, scan_dirs) — both derived from the same discovery.
    Extra dirs from valve are merged in.
    """
    discovered: List[str] = []
    try:
        if os.path.isdir(sandbox_path):
            for entry in os.scandir(sandbox_path):
                if entry.is_dir(follow_symlinks=False):
                    name = entry.name
                    if name not in _DIR_BLACKLIST and not name.startswith("."):
                        discovered.append(name)
    except Exception:
        pass

    # Merge extra dirs from valve (comma-separated, e.g. "api,services,workers")
    if extra_dirs:
        for d in extra_dirs.split(","):
            d = d.strip().strip("/")
            if d and d not in discovered and d not in _DIR_BLACKLIST:
                discovered.append(d)

    safe_write = [f"{d}/" for d in discovered]
    scan = list(discovered)
    return safe_write, scan

INCOMPLETE_CODE_MARKERS = [
    "...",
    "# rest of",
    "# TODO",
    "# implement",
    "# add implementation",
    "pass  #",
    "raise NotImplementedError",
    "# ... (rest",
    "# continues",
    "# omitted",
]

# ──────────────────────────────────────────────────────────────
# KNOWLEDGE CACHE
# ──────────────────────────────────────────────────────────────
class _Cache:
    def __init__(self, ttl: int = CACHE_TTL):
        self._store: Dict[str, Tuple[str, float]] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[str]:
        if key in self._store:
            val, ts = self._store[key]
            if time.time() - ts < self._ttl:
                return val
            del self._store[key]
        return None

    def set(self, key: str, val: str) -> None:
        self._store[key] = (val, time.time())

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)


_cache = _Cache()


# ──────────────────────────────────────────────────────────────
# PIPE CLASS
# ──────────────────────────────────────────────────────────────
class Pipe:
    """
    Universal Project Builder Pipeline v5.3

    Works with any blueprint-driven project.
    Convention: knowledge folder must contain:
      - konteks_permanen.md  (or any *_permanen.md / *_context.md)
      - status_dev.md        (or any *_status.md / *_progress.md)
      - aturan_sistem.md     (or any *_rules.md / *_aturan.md)

    Task commands (flexible ID format — no lock on prefix):
      "kerjakan P0-004"                        → single task
      "kerjakan P10-001 sampai P10-010"        → batch range
      "kerjakan SPRINT-3-007"                  → also works
      "kerjakan P0-004, P0-005, P0-006"        → batch list

    Phase check commands (zero token cost):
      "cek phase P0"                           → phase health report
      "check phase SPRINT-3"                   → also works
      "status phase P10"                       → also works
    """

    class Valves(BaseModel):
        CODER_MODEL: str = Field(
            default="deepseek-chat",
            description="Model ID for Coder (needs strong code gen)",
        )
        REVIEWER_MODEL: str = Field(
            default="claude-haiku-4-5-20251001",
            description="Model ID for Reviewer (fast + cheap)",
        )
        PLANNER_MODEL: str = Field(
            default="claude-haiku-4-5-20251001",
            description="Model ID for Planner (reads context + plans approach)",
        )
        SANDBOX_PATH: str = Field(
            default="/home/user/bayandezenith",
            description="Root path of the project (shared volume)",
        )
        KNOWLEDGE_PATH: str = Field(
            default="/home/user/knowledge",
            description="Path to knowledge files folder",
        )
        MAX_RETRY: int = Field(
            default=2,
            description="Max coder retries after reviewer rejection",
        )
        LLM_TIMEOUT: int = Field(
            default=300,
            description="Streaming timeout in seconds",
        )
        CODE_EXEC_TIMEOUT: int = Field(
            default=60,
            description="Test execution timeout in seconds",
        )
        REQUIRE_USER_CONFIRM: bool = Field(
            default=True,
            description="Ask user before running test files",
        )
        READ_ONLY_MODE: bool = Field(
            default=False,
            description="Skip file writes (dry run)",
        )
        EXEC_VIA_DOCKER: bool = Field(
            default=True,
            description="Execute tests via docker exec",
        )
        SANDBOX_CONTAINER: str = Field(
            default="open-terminal-sandbox",
            description="Docker container name for test execution",
        )
        MAX_TOKEN_BUDGET: int = Field(
            default=60000,
            description="Hard token limit per task. Pipeline stops if exceeded.",
        )
        AVAILABLE_PACKAGES: str = Field(
            default="polars,duckdb,pyyaml,python-dotenv,ccxt,lightgbm,torch,mlflow,redis,aiohttp,pydantic,apscheduler,pytest,pytest-asyncio",
            description="Comma-separated list of packages installed in sandbox.",
        )
        # ── NEW IN v5.1 ──────────────────────────────────────────
        REVIEWER_THRESHOLD: int = Field(
            default=6,
            description=(
                "Complexity score threshold (1-10). Tasks scored BELOW this skip reviewer. "
                "Score is returned by Planner. Lower = more tasks skip reviewer. "
                "Set to 10 to always call reviewer (v5.0 behavior). "
                "Set to 1 to never call reviewer."
            ),
        )
        PROJECT_CONTEXT: str = Field(
            default="Algorithmic trading system. Modes: day_trade (5m/15m/1H intraday) and swing_trade (4H/Daily). Stack: Python, polars, duckdb, Redis, CCXT.",
            description=(
                "One-paragraph description of your project. Injected into coder/planner prompts. "
                "Replace with your own project description to make this pipeline universal."
            ),
        )
        BATCH_STOP_ON_FAIL: bool = Field(
            default=False,
            description=(
                "If True, batch stops immediately when a task fails (no user prompt). "
                "If False (default), pipeline asks user whether to continue after each failure."
            ),
        )
        EXTRA_WRITE_DIRS: str = Field(
            default="",
            description=(
                "v5.3: Comma-separated extra folders to allow writing to and scanning, "
                "in addition to auto-discovered subfolders of SANDBOX_PATH. "
                "Example: 'api,services,workers'. Usually not needed — "
                "pipeline auto-discovers all subfolders."
            ),
        )
        # ── NEW IN v5.5 — AGENT TOOL ENRICHMENT ─────────────────
        ENABLE_SMART_PATCH: bool = Field(
            default=True,
            description=(
                "v5.5 [T1]: Allow Coder to emit search/replace patches instead of full rewrites "
                "for existing files > 60 lines. Saves up to 80% output tokens on large files."
            ),
        )
        ENABLE_AUTO_PIP: bool = Field(
            default=True,
            description=(
                "v5.5 [T2]: Auto pip-install missing packages when test fails with "
                "ModuleNotFoundError, then re-run test. Requires sandbox to allow pip."
            ),
        )
        ENABLE_LINTER: bool = Field(
            default=True,
            description=(
                "v5.5 [T3]: Run static linter (ruff → flake8 → pyflakes fallback) "
                "before AI Reviewer. Issues go back to Coder at 0 LLM cost."
            ),
        )
        ENABLE_DEP_GRAPH: bool = Field(
            default=True,
            description=(
                "v5.5 [T4]: Build AST import dependency graph. "
                "Files that import touched files are auto-added to Coder context."
            ),
        )
        ENABLE_ENV_CHECK: bool = Field(
            default=True,
            description=(
                "v5.5 [T5]: Check Python env health before tests "
                "(broken asyncio, pytest, name shadowing). Shows specific fix hints."
            ),
        )
        PATCH_MIN_LINES: int = Field(
            default=60,
            description=(
                "v5.5 [T1]: Minimum file line count before patch mode is preferred over full rewrite."
            ),
        )

    def __init__(self):
        self.type = "pipe"
        self.valves = self.Valves()
        self._tokens: Dict[str, int] = {}
        # v5.3: Dynamic dirs — refreshed each pipe() call so new folders are picked up
        self._safe_write_dirs: List[str] = []
        self._scan_dirs: List[str] = []
        # v5.5: Env-health cache (reset each pipe() call)
        self._env_health: Optional[Dict[str, Any]] = None
        # v5.5: Per-task failure history for retry accumulation [T6]
        self._task_failure_log: Dict[str, List[str]] = {}

    def _refresh_dirs(self) -> None:
        """v5.3: Re-discover sandbox subdirs. Called at start of each pipe() invocation."""
        self._safe_write_dirs, self._scan_dirs = _get_dynamic_dirs(
            self.valves.SANDBOX_PATH,
            self.valves.EXTRA_WRITE_DIRS,
        )

    # ──────────────────────────────────────────────────────────
    # EMITTERS
    # ──────────────────────────────────────────────────────────
    async def _status(self, emit: Callable, text: str, done: bool = False) -> None:
        if emit:
            try:
                await emit({"type": "status", "data": {"description": text, "done": done}})
            except Exception:
                pass

    async def _replace(self, emit: Callable, content: str) -> None:
        if emit:
            try:
                await emit({"type": "replace", "data": {"content": content}})
            except Exception:
                pass

    async def _confirm(self, event_call: Callable, msg: str) -> bool:
        if not event_call:
            return True
        try:
            # FIXED: [Lapis 3 Bug 3 - Infinite Hang]
            resp = await asyncio.wait_for(
                event_call({
                    "type": "input",
                    "data": {
                        "title": "⚠️ Confirm",
                        "message": msg,
                        "placeholder": "yes / no",
                    },
                }),
                timeout=300.0
            )
            val = (resp.get("value", "") if isinstance(resp, dict) else str(resp)).strip().lower()
            return val in ("yes", "y", "ya", "lanjut", "ok", "oke", "continue", "lanjutkan")
        except asyncio.TimeoutError:
            logger.warning("User confirmation timed out.")
            return False
        except Exception:
            return False

    # ──────────────────────────────────────────────────────────
    # TOKEN TRACKING
    # ──────────────────────────────────────────────────────────
    def _track(self, key: str, text: str) -> None:
        self._tokens[key] = self._tokens.get(key, 0) + (len(text) // 4)

    def _total_tokens(self) -> int:
        return sum(self._tokens.values())

    def _token_report(self) -> str:
        total = self._total_tokens()
        lines = ["\n📊 **Token Usage:**"]
        for k, v in self._tokens.items():
            if v > 0:
                lines.append(f"- {k}: {v:,}")
        lines.append(f"- **Total**: {total:,}")
        if total > self.valves.MAX_TOKEN_BUDGET:
            lines.append(f"⚠️ Exceeded budget ({self.valves.MAX_TOKEN_BUDGET:,})")
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────
    # JSON EXTRACTION
    # ──────────────────────────────────────────────────────────
    def _extract_json(self, text: str) -> dict:
        """FIXED: [Lapis 2 Bug 4 - Greedy Regex JSON Parsing]"""
        decoder = json.JSONDecoder()
        for i, c in enumerate(text):
            if c == '{':
                try:
                    obj, _ = decoder.raw_decode(text[i:])
                    return obj
                except json.JSONDecodeError:
                    continue
            elif c == '[': # arrays
                try:
                    obj, _ = decoder.raw_decode(text[i:])
                    return obj
                except json.JSONDecodeError:
                    continue
        raise ValueError("No valid JSON found")

    # ──────────────────────────────────────────────────────────
    # FILE I/O
    # ──────────────────────────────────────────────────────────
    async def _read(self, path: str, use_cache: bool = True) -> str:
        if use_cache:
            hit = _cache.get(path)
            if hit is not None:
                return hit
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            if use_cache:
                _cache.set(path, content)
            return content
        except Exception as e:
            return f"[ERROR:{e}]"

    async def _write(self, path: str, content: str) -> bool:
        sandbox = os.path.normpath(self.valves.SANDBOX_PATH)
        norm = os.path.normpath(path)
        if not norm.startswith(sandbox):
            logger.warning(f"[v5.4] Write blocked (outside sandbox): {path}")
            return False
        rel = os.path.relpath(norm, sandbox)
        
        # FIXED: [Lapis 3 Bug 4 & Lapis 2 Bug 9 - Allow new folders, block blacklist properly]
        rel_root = rel.split(os.sep)[0] if os.sep in rel else rel.split("/")[0]
        if rel_root in _DIR_BLACKLIST:
            logger.warning(f"[v5.4] Write blocked (blacklisted dir): {rel}")
            return False
            
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            _cache.invalidate(path)
            return True
        except Exception as e:
            logger.error(f"[v5.4] Write error {path}: {e}")
            return False

    # ──────────────────────────────────────────────────────────
    # CODE EXECUTION
    # ──────────────────────────────────────────────────────────
    async def _exec(self, file_path: str, auto_pip: bool = True) -> Tuple[bool, str]:
        """
        v5.5 [T2]: If test fails with ModuleNotFoundError and ENABLE_AUTO_PIP,
        attempt auto pip install then retry once.
        """
        if self.valves.EXEC_VIA_DOCKER:
            ok, out = await self._exec_docker(file_path)
            if ok or "docker" not in out.lower():
                if not ok and self.valves.ENABLE_AUTO_PIP and auto_pip:
                    return await self._exec_with_auto_pip(file_path, ok, out, via_docker=True)
                return ok, out
        ok, out = await self._exec_native(file_path)
        if not ok and self.valves.ENABLE_AUTO_PIP and auto_pip:
            return await self._exec_with_auto_pip(file_path, ok, out, via_docker=False)
        return ok, out

    async def _exec_with_auto_pip(
        self, file_path: str, ok: bool, out: str, via_docker: bool
    ) -> Tuple[bool, str]:
        """v5.5 [T2]: Try auto pip install if ModuleNotFoundError detected, then retry."""
        missing = re.search(r"ModuleNotFoundError: No module named '([^']+)'", out)
        if not missing:
            return ok, out
        pkg = missing.group(1).split(".")[0]  # top-level package
        installed, pip_out = await self._auto_pip_install(pkg)
        if not installed:
            return False, f"{out}\n[auto-pip] Failed to install '{pkg}': {pip_out}"
        # Retry test once after install
        if via_docker:
            return await self._exec_docker(file_path)
        return await self._exec_native(file_path)

    async def _exec_docker(self, file_path: str) -> Tuple[bool, str]:
        proc = None
        try:
            # FIXED: [Lapis 2 Bug 1 - PYTHONPATH env var added]
            proc = await asyncio.create_subprocess_exec(
                "docker", "exec", "-w", self.valves.SANDBOX_PATH,
                "-e", f"PYTHONPATH={self.valves.SANDBOX_PATH}",
                self.valves.SANDBOX_CONTAINER, "python3", file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=self.valves.CODE_EXEC_TIMEOUT
            )
            if proc.returncode == 0:
                # FIXED: [Gemini Audit Bug 6 - Use errors='replace' to avoid UnicodeDecodeError]
                return True, stdout.decode("utf-8", errors="replace")
            return False, stderr.decode("utf-8", errors="replace") or stdout.decode("utf-8", errors="replace")
        except FileNotFoundError:
            return False, "docker command not found"
        except asyncio.TimeoutError:
            if proc:
                try:
                    proc.kill(); await proc.wait()
                except Exception:
                    pass
            return False, f"Timeout ({self.valves.CODE_EXEC_TIMEOUT}s)"
        except Exception as e:
            return False, f"Docker error: {e}"

    async def _exec_native(self, file_path: str) -> Tuple[bool, str]:
        proc = None
        try:
            # FIXED: [Lapis 2 Bug 1 & Minor 5 - PYTHONPATH env var in native exec using subprocess_exec]
            env = os.environ.copy()
            env["PYTHONPATH"] = self.valves.SANDBOX_PATH
            proc = await asyncio.create_subprocess_exec(
                "python3", file_path,
                cwd=self.valves.SANDBOX_PATH,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=self.valves.CODE_EXEC_TIMEOUT
            )
            if proc.returncode == 0:
                # FIXED: [Gemini Audit Bug 6 - Use errors='replace' to avoid UnicodeDecodeError]
                return True, stdout.decode("utf-8", errors="replace")
            return False, stderr.decode("utf-8", errors="replace") or stdout.decode("utf-8", errors="replace")
        except asyncio.TimeoutError:
            if proc:
                try:
                    proc.kill(); await proc.wait()
                except Exception:
                    pass
            return False, f"Timeout ({self.valves.CODE_EXEC_TIMEOUT}s)"
        except Exception as e:
            return False, str(e)

    # ──────────────────────────────────────────────────────────
    # LLM INVOCATION
    # ──────────────────────────────────────────────────────────
    async def _llm(
        self,
        request: Request,
        model_id: str,
        messages: List[Dict],
        user_obj: Any = None,
        track_key: str = "misc",
    ) -> str:
        # FIXED: [Lapis 2 Bug 8 - isinstance validation compat]
        if Users is not None and user_obj is not None:
            uid = user_obj.get("id", "") if isinstance(user_obj, dict) else getattr(user_obj, "id", "")
            if uid:
                try:
                    real = await Users.get_user_by_id(uid)
                    if real:
                        user_obj = real
                except Exception:
                    pass

        input_text = " ".join(m.get("content", "") for m in messages)
        self._track(f"{track_key}_in", input_text)

        form = {"model": model_id, "messages": messages, "stream": True}
        try:
            resp = await asyncio.wait_for(_raw_llm(request, form, user=user_obj), timeout=30)
        except asyncio.TimeoutError:
            return "[ERROR] LLM setup timeout"
        except Exception as e:
            logger.error(f"[v5.4] LLM call error ({model_id}): {e}")
            return await self._llm_http_fallback(request, model_id, messages)

        result = ""
        if inspect.isasyncgen(resp):
            try:
                result = await self._drain(resp)
            except asyncio.TimeoutError:
                return f"[ERROR] LLM streaming timeout ({self.valves.LLM_TIMEOUT}s)"
            except Exception as e:
                return f"[ERROR] Stream error: {e}"
        elif hasattr(resp, "body_iterator"):
            try:
                result = await self._drain(resp.body_iterator)
            except asyncio.TimeoutError:
                if hasattr(resp.body_iterator, "aclose"):
                    try:
                        await resp.body_iterator.aclose()
                    except Exception:
                        pass
                return f"[ERROR] LLM streaming timeout ({self.valves.LLM_TIMEOUT}s)"
            except Exception as e:
                return f"[ERROR] Stream error: {e}"
        else:
            result = self._extract_content(resp)

        if not result:
            result = "[ERROR] LLM returned empty response"

        self._track(f"{track_key}_out", result)
        return result

    async def _drain(self, iterator: Any) -> str:
        parts: List[str] = []
        buf = ""
        # FIXED: [Minor 4 - use LLM_TIMEOUT valve]
        deadline = time.monotonic() + self.valves.LLM_TIMEOUT
        is_async = inspect.isasyncgen(iterator)

        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise asyncio.TimeoutError()
            try:
                if is_async:
                    chunk = await asyncio.wait_for(iterator.__anext__(), timeout=min(left, 120))
                else:
                    loop = asyncio.get_running_loop()
                    chunk = await asyncio.wait_for(
                        loop.run_in_executor(None, partial(next, iterator, _ITER_EXHAUSTED)),
                        timeout=min(left, 120),
                    )
                    if chunk is _ITER_EXHAUSTED:
                        break
            except StopAsyncIteration:
                break

            decoded = chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else str(chunk)
            buf += decoded
            while "\n\n" in buf:
                evt, buf = buf.split("\n\n", 1)
                for line in evt.splitlines():
                    s = line.strip()
                    if s.startswith("data:"):
                        payload = s[5:].lstrip()
                        if not payload or payload == "[DONE]":
                            continue
                        try:
                            d = json.loads(payload)
                            for ch in d.get("choices", []):
                                c = (ch.get("delta") or {}).get("content", "")
                                if c:
                                    parts.append(c)
                        except Exception:
                            pass

        # Flush remaining buffer — v4.6 fix: last chunk may not end with \n\n
        for line in buf.splitlines():
            s = line.strip()
            if s.startswith("data:"):
                payload = s[5:].lstrip()
                if payload and payload != "[DONE]":
                    try:
                        d = json.loads(payload)
                        for ch in d.get("choices", []):
                            c = (ch.get("delta") or {}).get("content", "")
                            if c:
                                parts.append(c)
                    except Exception:
                        pass

        return "".join(parts)

    def _extract_content(self, resp: Any) -> str:
        if isinstance(resp, dict):
            for ch in resp.get("choices", []):
                msg = ch.get("message") or ch.get("delta") or {}
                c = msg.get("content", "")
                if c:
                    return c
            if "error" in resp:
                return f"[ERROR] {resp['error']}"
        if isinstance(resp, str):
            return resp
        # v4.6 fix: some OpenWebUI versions return response with .body attribute
        if hasattr(resp, "body"):
            try:
                body = resp.body
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                data = json.loads(body)
                choices = data.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content", "")
            except Exception:
                pass
        return f"[ERROR] Unexpected response type: {type(resp).__name__}"

    async def _llm_http_fallback(
        self, request: Request, model_id: str, messages: List[Dict]
    ) -> str:
        try:
            import aiohttp
            token = ""
            cookie = request.headers.get("cookie", "")
            m = re.search(r"token=([^;]+)", cookie)
            if m:
                token = m.group(1).strip()
            if not token:
                auth = request.headers.get("authorization", "")
                if auth.lower().startswith("bearer "):
                    token = auth[7:].strip()
            base = str(request.base_url).rstrip("/")
            async with aiohttp.ClientSession() as sess:
                headers = {"Content-Type": "application/json"}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    headers["Cookie"] = f"token={token}"  # v4.6 fix: some setups need this
                async with sess.post(
                    f"{base}/api/chat/completions",
                    json={"model": model_id, "messages": messages, "stream": False},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.valves.LLM_TIMEOUT),
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        choices = data.get("choices", [])
                        if choices:
                            return choices[0].get("message", {}).get("content", "")
                        return "[ERROR] No choices"
                    return f"[ERROR] HTTP {r.status}: {(await r.text())[:300]}"
        except Exception as e:
            return f"[ERROR] HTTP fallback failed: {e}"

    # ──────────────────────────────────────────────────────────
    # ERROR CLASSIFICATION
    # ──────────────────────────────────────────────────────────
    _NON_RETRYABLE = [
        "permission denied", "no such file or directory", "read-only file system",
        "write blocked", "docker command not found", "container not found",
        "llm setup timeout", "api key", "unauthorized", "rate limit",
        "returned empty response", "model not found", "no such model",
        "model does not exist", "invalid model", "not available",
        "lm api 400", "lm api 401", "lm api 403", "lm api 404",
        "lm api 422", "lm api 500", "lm api 502", "lm api 503",
    ]

    def _retryable(self, msg: str) -> bool:
        lo = msg.lower()
        return not any(p in lo for p in self._NON_RETRYABLE)

    def _classify(self, msg: str) -> str:
        lo = msg.lower()
        if "permission denied" in lo:
            return "🔒 PERMISSION — Run: `sudo chown -R 1000:1000 /root/project`"
        if "write blocked" in lo:
            return "🔒 WRITE BLOCKED — Path not in SAFE_WRITE_DIRS"
        if "docker command not found" in lo:
            return "🐳 DOCKER UNAVAILABLE — Disable EXEC_VIA_DOCKER in valves"
        if "container not found" in lo:
            return "🐳 CONTAINER NOT FOUND — Check SANDBOX_CONTAINER valve"
        if "api key" in lo or "unauthorized" in lo:
            return "🔑 AUTH ERROR — Check API key in OpenWebUI admin"
        if "rate limit" in lo:
            return "⏱️ RATE LIMIT — Wait a few minutes and retry"
        if "model not found" in lo or "no such model" in lo:
            return "🤖 MODEL NOT FOUND — Check model name in Valves"
        return "🔧 LOGICAL ERROR — Retry will be attempted"

    # ──────────────────────────────────────────────────────────
    # KNOWLEDGE COMPRESSION
    # ──────────────────────────────────────────────────────────
    def _compress_knowledge(self, knowledge: Dict[str, str], task_id: str) -> str:
        # FIXED: [Lapis 5 Bug C - Multi-Segment Phase Prefix in _compress_knowledge]
        m = re.match(r"^(.*)-\d+$", task_id)
        phase_prefix = m.group(1) if m else task_id.split("-")[0]
        parts: List[str] = []

        for fname, content in knowledge.items():
            fname_lo = fname.lower()

            if "permanen" in fname_lo or "context" in fname_lo or "permanent" in fname_lo:
                extracted = self._extract_sections(content, [
                    "## 3.", "## TECH STACK", "## 5.", "## ATURAN KERAS",
                    "## 6.", "## RISK", "## 9.", "## OUTPUT FORMAT",
                    "## 8.", "## REDIS KEY",
                ])
                if any(kw in task_id.lower() for kw in ["db", "database", "schema", "duck"]):
                    extracted += self._extract_sections(content, ["## 7.", "## DATABASE"])
                parts.append(f"### {fname} (compressed)\n{extracted}")

            elif "status" in fname_lo or "progress" in fname_lo:
                extracted = self._extract_active_phase(content, task_id, phase_prefix)
                parts.append(f"### {fname} (active phase only)\n{extracted}")

            elif "aturan" in fname_lo or "rules" in fname_lo or "sistem" in fname_lo:
                coder_part = content.split("## ATURAN UNTUK AI REVIEWER")[0] if "REVIEWER" in content else content
                coder_part = coder_part.split("## ATURAN UNTUK AI DOCS")[0] if "DOCS" in coder_part else coder_part
                parts.append(f"### {fname} (coder rules only)\n{coder_part.strip()}")

            else:
                parts.append(f"### {fname}\n{content[:2000]}")

        return "\n\n".join(parts)

    def _extract_sections(self, content: str, headers: List[str]) -> str:
        lines = content.split("\n")
        result: List[str] = []
        include = False

        for line in lines:
            if any(line.startswith(h) for h in headers if h):
                include = True
            elif line.startswith("## ") and not any(line.startswith(h) for h in headers if h):
                include = False
            if include:
                result.append(line)

        return "\n".join(result)

    def _extract_active_phase(self, content: str, task_id: str, phase_prefix: str) -> str:
        lines = content.split("\n")
        result: List[str] = []
        in_active_phase = False
        in_decisions = False
        decision_lines: List[str] = []
        
        # FIXED: [Minor 2 - Use regex for phase prefix digit]
        val_phase_num = ""
        m_num = re.search(r'\d+', phase_prefix)
        if m_num:
            val_phase_num = m_num.group(0)

        for line in lines:
            if line.startswith("## POSISI SEKARANG"):
                result.append(line)
                in_active_phase = True
                in_decisions = False
                continue

            if f"## {phase_prefix.upper()}" in line or (val_phase_num and f"## PHASE {val_phase_num}" in line):
                result.append(line)
                in_active_phase = True
                in_decisions = False
                continue

            if line.startswith("## PHASE") and (
                not val_phase_num or
                # FIXED: [Gemini Audit Bug 3 - Use exact word boundary, not substring match]
                not re.search(rf"\bPHASE\s+{re.escape(val_phase_num)}\b", line)
            ):
                in_active_phase = False
                continue
            if line.startswith("## P") and phase_prefix not in line and "|" not in line:
                in_active_phase = False
                continue

            if "## KEPUTUSAN" in line:
                in_decisions = True
                in_active_phase = False
                decision_lines.append(line)
                continue

            if in_decisions:
                decision_lines.append(line)
                continue

            if in_active_phase:
                result.append(line)

        result.extend(decision_lines[-10:] if len(decision_lines) > 10 else decision_lines)
        return "\n".join(result)

    # ──────────────────────────────────────────────────────────
    # PROJECT FILE SCANNER
    # ──────────────────────────────────────────────────────────
    async def _scan_files(
        self,
        task_desc: str,
        extra_paths: Optional[List[str]] = None,
        dep_expand_paths: Optional[List[str]] = None,  # v5.5 [T4]
        tokens_before: Optional[Dict[str, int]] = None, # v5.5 [T8]
    ) -> Dict[str, str]:
        """
        Scan project for files relevant to this task.
        v5.1: extra_paths forces inclusion of specific files (from plan.files_to_modify).
        v5.5 [T4]: dep_expand_paths auto-adds files that import changed files.
        v5.5 [T8]: Dynamic char limit based on token budget.
        """
        sandbox = self.valves.SANDBOX_PATH
        result: Dict[str, str] = {}
        keywords = self._keywords(task_desc)

        # v5.5 [T8]: Dynamic char limit
        char_limit = self._dynamic_file_char_limit(tokens_before)

        def read_file(full: str, rel: str) -> None:
            if rel in result:
                return
            try:
                with open(full, "r", encoding="utf-8") as f:
                    raw = f.read()
                if len(raw) > char_limit:
                    result[rel] = raw[:char_limit] + f"\n# ... [truncated — full file on disk]"
                else:
                    result[rel] = raw
            except Exception:
                pass

        # Force-include files from planner's files_to_modify list
        if extra_paths:
            for rel_path in extra_paths:
                # FIXED: [Lapis 4 Bug 3 - Path Traversal in extra_paths]
                full = os.path.normpath(os.path.join(sandbox, rel_path))
                if not full.startswith(os.path.normpath(sandbox)):
                    continue
                if os.path.isfile(full):
                    read_file(full, rel_path)

        # v5.5 [T4]: Auto-include files that import the modified files
        if dep_expand_paths and self.valves.ENABLE_DEP_GRAPH:
            dep_candidates = self._get_dependents(dep_expand_paths)
            for rel in dep_candidates:
                full = os.path.join(sandbox, rel)
                if os.path.isfile(full):
                    read_file(full, rel)
                if len(result) >= MAX_RELEVANT_FILES:
                    return result

        for scan_dir in self._scan_dirs:
            dir_path = os.path.join(sandbox, scan_dir)
            if not os.path.isdir(dir_path):
                continue

            for root, dirs, files in os.walk(dir_path):
                # FIXED: [Lapis 5 Bug D - Blacklist applied to subdirectories]
                dirs[:] = [
                    d for d in dirs
                    if not d.startswith((".", "__")) and d not in _DIR_BLACKLIST
                ]
                for fname in files:
                    if not fname.endswith((".py", ".yaml", ".yml", ".json")):
                        continue

                    full = os.path.join(root, fname)
                    rel = os.path.relpath(full, sandbox)

                    if rel in result:
                        continue  # already force-included

                    try:
                        size_kb = os.path.getsize(full) / 1024
                        if size_kb > MAX_FILE_SIZE_KB:
                            continue
                    except Exception:
                        continue

                    rel_lo = rel.lower()
                    fname_lo = fname.lower().replace(".py", "").replace(".yaml", "")

                    is_relevant = (
                        fname_lo in task_desc.lower()
                        or any(kw in rel_lo for kw in keywords)
                        or any(part in task_desc.lower() for part in rel_lo.split("/"))
                    )

                    if is_relevant:
                        read_file(full, rel)

                    if len(result) >= MAX_RELEVANT_FILES:
                        return result

        return result

    def _keywords(self, task_desc: str) -> List[str]:
        stop = {
            "buat", "tambah", "perbaiki", "fix", "update", "implementasi",
            "implement", "add", "create", "modify", "dan", "atau", "yang",
            "di", "ke", "dari", "dengan", "untuk", "test", "kerjakan", "task",
        }
        words = re.findall(r"[a-zA-Z_]+", task_desc.lower())
        return [w for w in words if w not in stop and len(w) > 3]

    # ──────────────────────────────────────────────────────────
    # STATUS_DEV.MD PARSER & UPDATER (v5.1 — robust)
    # ──────────────────────────────────────────────────────────
    def _parse_tasks(self, content: str) -> List[Dict[str, str]]:
        """
        v5.3: Parse tasks from status_dev.md table.
        Supports flexible ID formats: P0-001, P10-025, SPRINT-3-007, B2-007, etc.
        """
        tasks: List[Dict[str, str]] = []
        in_table = False
        for line in content.split("\n"):
            s = line.strip()
            if not s or "|" not in s:
                continue
            if re.match(r"^\|[\s\-:]+\|", s):
                in_table = True
                continue
            # v5.3: flexible ID — any alphanum+dash sequence followed by -NNN
            if in_table and re.match(r"^\|?\s*[A-Za-z0-9][A-Za-z0-9\-]*-\d+", s):
                # FIXED: [Lapis 4 Bug 4 + Gemini Audit Bug 2 - Column index shift for no-leading-pipe tables]
                parts = [p.strip() for p in s.split("|")]
                # Detect offset: if line starts with "|", parts[0] is "" and ID is at parts[1]
                # If line does NOT start with "|", parts[0] is the ID directly
                offset = 1 if parts[0] == "" else 0
                if len(parts) > offset:
                    tasks.append({
                        "id":     parts[offset].upper(),
                        "name":   parts[offset + 1] if len(parts) > offset + 1 else "",
                        "status": parts[offset + 2] if len(parts) > offset + 2 else "",
                        "files":  parts[offset + 3] if len(parts) > offset + 3 else "",
                        "notes":  parts[offset + 4] if len(parts) > offset + 4 else "",
                    })
        return tasks

    def _update_status(self, content: str, task_id: str, update: Dict[str, str]) -> Tuple[str, bool]:
        """
        v5.1: Returns (updated_content, success_bool).
        If table row update fails, appends a fallback note at the bottom.
        Always returns valid content — never silently fails.
        """
        lines = content.split("\n")
        updated = False

        for i, line in enumerate(lines):
            # FIXED: [Lapis 1/2 Bug 2 - Substring Matching column corrupts status]
            if task_id in line and "|" in line:
                parts = line.split("|")
                # Ensuring we exactly match the ID on the ID column (usually col 1 or 2 due to leading pipe)
                idx = 1 if line.strip().startswith('|') else 0
                if len(parts) > idx and parts[idx].strip().upper() == task_id.upper():
                    if "status" in update and len(parts) > idx + 2:
                        parts[idx + 2] = f" {update['status']} "
                    if "files" in update and len(parts) > idx + 3:
                        parts[idx + 3] = f" {update['files']} "
                    if "notes" in update and len(parts) > idx + 4:
                        parts[idx + 4] = f" {update['notes'][:150]} "
                    lines[i] = "|".join(parts)
                    updated = True
                    break

        result = "\n".join(lines)

        if not updated:
            # Fallback: append note at bottom
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            fallback = (
                f"\n\n<!-- Pipeline v5.4 update [{ts}] -->\n"
                f"<!-- {task_id}: {update.get('status', '?')} | "
                f"files: {update.get('files', '-')} | "
                f"notes: {update.get('notes', '-')} -->"
            )
            result += fallback

        return result, updated

    async def _commit_status(
        self,
        knowledge: Dict[str, str],
        status_dev_content: str,
        task_id: str,
        update: Dict[str, str],
        emit: Callable,
    ) -> str:
        """
        v5.1: Commit status update to file. Always called, regardless of verdict.
        Returns updated content string.
        """
        updated_content, table_updated = self._update_status(status_dev_content, task_id, update)

        if not self.valves.READ_ONLY_MODE:
            for fname in knowledge.keys():
                if "status" in fname.lower() or "progress" in fname.lower():
                    status_path = os.path.join(self.valves.KNOWLEDGE_PATH, fname)
                    try:
                        with open(status_path, "w", encoding="utf-8") as f:
                            f.write(updated_content)
                        _cache.invalidate(status_path)
                        method = "✅ Status updated" if table_updated else "⚠️ Status appended (table row not found)"
                        await self._status(emit, method)
                    except Exception as e:
                        await self._status(emit, f"⚠️ Status write failed: {e}")
                    break

        return updated_content

    # ──────────────────────────────────────────────────────────
    # STATIC VALIDATOR
    # ──────────────────────────────────────────────────────────
    def _validate_code(self, files: List[Dict]) -> List[str]:
        issues: List[str] = []
        allowed_pkgs = set(
            p.strip().lower().replace("-", "_")
            for p in self.valves.AVAILABLE_PACKAGES.split(",")
        )
        
        # FIXED: [Lapis 5 Bug E - Import Alias match handling]
        _PKG_IMPORT_ALIAS = {
            "pyyaml": "yaml",
            "python-dotenv": "dotenv",
            "pillow": "PIL",
            "scikit-learn": "sklearn",
        }
        for pkg_raw in self.valves.AVAILABLE_PACKAGES.split(","):
            p = pkg_raw.strip().lower()
            if p in _PKG_IMPORT_ALIAS:
                allowed_pkgs.add(_PKG_IMPORT_ALIAS[p])

        # FIXED: [Lapis 1/2 Bug 3 - Hard-reject Standard Library completion]
        stdlib = set()
        if hasattr(sys, 'stdlib_module_names'):
            stdlib = set(sys.stdlib_module_names)
        else:
            stdlib = set(sys.builtin_module_names) | {
                "os", "sys", "re", "json", "time", "math", "logging", "typing",
                "pathlib", "datetime", "collections", "functools", "asyncio",
                "abc", "io", "copy", "random", "itertools", "contextlib",
                "dataclasses", "enum", "unittest", "inspect", "tempfile",
                "argparse", "hashlib", "threading", "subprocess", "csv",
                "shutil", "struct", "base64", "uuid", "warnings", "traceback",
                "weakref", "glob", "pickle", "queue", "signal", "statistics"
            }

        for f in files:
            path = f.get("path", "?")
            content = f.get("content", "")
            
            # FIXED: [Lapis 5 Bug F - Allow detecting empty files early]
            if not content.strip():
                issues.append(f"{path}: empty content — file will not be written")

            # FIXED: [Lapis 1/2 Bug 5 - INCOMPLETE_CODE_MARKERS context checking (only check strings isolated to stand-alone lines if applicable)]
            for line in content.split("\n"):
                stripped = line.strip()
                if stripped == "...":
                    issues.append(f"{path}: bare ellipsis '...' — likely placeholder")
                # Exclude '...' from standard marker iteration.
                for marker in INCOMPLETE_CODE_MARKERS:
                    if marker != "..." and marker in stripped:
                        issues.append(f"{path}: found placeholder marker '{marker}'")

            if content.count('"""') % 2 != 0:
                issues.append(f"{path}: odd number of triple-quotes — likely truncated")

            stripped_full = content.rstrip()
            if stripped_full.endswith(":") and not stripped_full.endswith('":'):
                issues.append(f"{path}: file ends with ':' — likely missing function body")

            # FIXED: [Gemini Audit Bug 1 - Local module imports no longer in hard issues]
            # Import checks moved to _validate_code_soft (soft warnings only, never triggers retry)

        return issues

    def _validate_code_soft(self, files: List[Dict]) -> List[str]:
        """
        Soft-only warnings: unknown imports that might be project-local modules.
        These are NEVER added to hard validation_issues and NEVER trigger retry.
        Used only for display/logging purposes.
        """
        soft: List[str] = []
        allowed_pkgs = set(
            p.strip().lower().replace("-", "_")
            for p in self.valves.AVAILABLE_PACKAGES.split(",")
        )
        _PKG_IMPORT_ALIAS = {
            "pyyaml": "yaml", "python-dotenv": "dotenv",
            "pillow": "PIL", "scikit-learn": "sklearn",
        }
        for pkg_raw in self.valves.AVAILABLE_PACKAGES.split(","):
            p = pkg_raw.strip().lower()
            if p in _PKG_IMPORT_ALIAS:
                allowed_pkgs.add(_PKG_IMPORT_ALIAS[p])
        stdlib = set(sys.stdlib_module_names) if hasattr(sys, "stdlib_module_names") else set(sys.builtin_module_names)

        # FIXED: [Gemini Audit Bug 5 - Use AST for accurate multi-import detection]
        for f in files:
            path = f.get("path", "?")
            content = f.get("content", "")
            if not content.strip():
                continue
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    pkgs: List[str] = []
                    if isinstance(node, ast.Import):
                        pkgs = [alias.name.split(".")[0] for alias in node.names]
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            pkgs = [node.module.split(".")[0]]
                    for pkg in pkgs:
                        pkg_lo = pkg.lower()
                        if pkg_lo not in stdlib and pkg_lo not in allowed_pkgs and "." not in pkg_lo:
                            soft.append(
                                f"{path}: imported '{pkg}' — not in AVAILABLE_PACKAGES. "
                                f"If project-local module, ignore."
                            )
            except SyntaxError:
                pass  # syntax errors handled separately by ast.parse in deep check
        return soft

    # ──────────────────────────────────────────────────────────
    # PIPELINE LOGGER
    # ──────────────────────────────────────────────────────────
    async def _log_task(
        self,
        task_id: str,
        verdict: str,
        files_written: List[str],
        tokens: int,
        reviewer_skipped: bool,
        error: str = "",
    ) -> None:
        """Append-only log to pipeline_log.jsonl in SANDBOX_PATH."""
        if self.valves.READ_ONLY_MODE:
            return
        log_path = os.path.join(self.valves.SANDBOX_PATH, "pipeline_log.jsonl")
        entry = {
            "ts": datetime.now().isoformat(),
            "task_id": task_id,
            "verdict": verdict,
            "files": files_written,
            "tokens": tokens,
            "reviewer_skipped": reviewer_skipped,
            "error": error,
            "pipeline_version": "5.4",
        }
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[v5.4] Log write failed: {e}")

    # ──────────────────────────────────────────────────────────
    # v5.5 [T1] SMART DIFF MODIFIER — apply_patch
    # ──────────────────────────────────────────────────────────
    async def _apply_patch(self, path: str, search: str, replace: str) -> Tuple[bool, str]:
        """
        Apply a search-and-replace patch to an existing file on disk.
        Returns (success, message).
        Used when coder emits 'patches' format instead of full file content.
        """
        sandbox = os.path.normpath(self.valves.SANDBOX_PATH)
        full = os.path.normpath(path) if os.path.isabs(path) else os.path.normpath(
            os.path.join(sandbox, path)
        )
        if not full.startswith(sandbox):
            return False, f"Patch blocked: path outside sandbox ({path})"
        if not os.path.isfile(full):
            return False, f"Patch target not found: {path}"
        try:
            with open(full, "r", encoding="utf-8") as f:
                original = f.read()
            if search not in original:
                return False, f"Patch search string not found in {path}"
            patched = original.replace(search, replace, 1)
            with open(full, "w", encoding="utf-8") as f:
                f.write(patched)
            _cache.invalidate(full)
            return True, f"Patch applied: {path}"
        except Exception as e:
            return False, f"Patch error on {path}: {e}"

    def _file_line_count(self, rel_path: str) -> int:
        """Return line count of a file in sandbox, or 0 if not found."""
        full = os.path.join(self.valves.SANDBOX_PATH, rel_path)
        try:
            with open(full, "r", encoding="utf-8") as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    # ──────────────────────────────────────────────────────────
    # v5.5 [T2] AUTO-DEPENDENCY HEALER — auto_pip_install
    # ──────────────────────────────────────────────────────────
    async def _auto_pip_install(self, package_name: str) -> Tuple[bool, str]:
        """
        Auto pip-install a missing package inside the sandbox.
        Tries docker exec first, then native pip. Returns (success, output).
        """
        # Sanitise package name — only allow safe chars
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", package_name):
            return False, f"Unsafe package name: {package_name}"

        pip_cmd = ["pip", "install", "--quiet", package_name]

        if self.valves.EXEC_VIA_DOCKER:
            try:
                proc = await asyncio.create_subprocess_exec(
                    "docker", "exec", self.valves.SANDBOX_CONTAINER,
                    *pip_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=120
                )
                if proc.returncode == 0:
                    return True, f"pip install {package_name} ✅"
                err = stderr.decode("utf-8", errors="replace")
                if "docker" not in err.lower():
                    return False, err[:200]
            except Exception:
                pass  # fall through to native

        # Native fallback
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "--quiet", package_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode == 0:
                return True, f"pip install {package_name} ✅ (native)"
            return False, stderr.decode("utf-8", errors="replace")[:200]
        except Exception as e:
            return False, f"pip install failed: {e}"

    # ──────────────────────────────────────────────────────────
    # v5.5 [T3] ZERO-TOKEN STATIC LINTER — run_static_linter
    # ──────────────────────────────────────────────────────────
    def _detect_linter(self) -> Optional[str]:
        """Auto-detect available linter: ruff > flake8 > pyflakes > None."""
        global _LINTER_CMD
        if _LINTER_CMD is not None:
            return _LINTER_CMD if _LINTER_CMD != "none" else None
        for cmd in ("ruff", "flake8", "pyflakes"):
            try:
                result = subprocess.run(
                    [cmd, "--version"], capture_output=True, timeout=5
                )
                if result.returncode == 0:
                    _LINTER_CMD = cmd
                    return cmd
            except Exception:
                continue
        _LINTER_CMD = "none"
        return None

    async def _run_static_linter(self, files: List[Dict]) -> List[str]:
        """
        Run ruff/flake8/pyflakes on the written files (0 LLM tokens).
        Returns list of issue strings. Empty list = all clean.
        """
        if not self.valves.ENABLE_LINTER:
            return []
        linter = self._detect_linter()
        if not linter:
            return []

        issues: List[str] = []
        sandbox = self.valves.SANDBOX_PATH

        for fi in files:
            rel_path = fi.get("path", "")
            if not rel_path.endswith(".py"):
                continue
            full = os.path.join(sandbox, rel_path)
            if not os.path.isfile(full):
                continue

            try:
                if linter == "ruff":
                    cmd = ["ruff", "check", "--select=E,F,W", "--output-format=text", full]
                elif linter == "flake8":
                    cmd = ["flake8", "--max-line-length=120", full]
                else:
                    cmd = ["pyflakes", full]

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
                raw = (result.stdout + result.stderr).strip()
                if raw:
                    # Truncate per-file to avoid flooding the coder prompt
                    for line in raw.splitlines()[:10]:
                        line = line.replace(full, rel_path)
                        issues.append(f"[linter] {line}")
            except Exception as e:
                issues.append(f"[linter] {linter} error on {rel_path}: {e}")

        return issues

    # ──────────────────────────────────────────────────────────
    # v5.5 [T4] AST IMPORT DEPENDENCY MAPPER
    # ──────────────────────────────────────────────────────────
    def _get_project_dependency_graph(self) -> Dict[str, List[str]]:
        """
        Build import graph: {rel_path: [rel_paths_that_rel_path_imports]}.
        Cached for DEP_GRAPH_CACHE_TTL seconds.
        Only maps intra-project imports (files that exist in sandbox).
        """
        global _dep_graph_cache, _dep_graph_ts
        now = time.time()
        if _dep_graph_cache is not None and (now - _dep_graph_ts) < DEP_GRAPH_CACHE_TTL:
            return _dep_graph_cache

        sandbox = os.path.normpath(self.valves.SANDBOX_PATH)
        graph: Dict[str, List[str]] = {}

        # Collect all .py files
        all_py: List[str] = []
        for root, dirs, files in os.walk(sandbox):
            dirs[:] = [d for d in dirs if d not in _DIR_BLACKLIST and not d.startswith(".")]
            for fname in files:
                if fname.endswith(".py"):
                    full = os.path.join(root, fname)
                    rel = os.path.relpath(full, sandbox)
                    all_py.append(rel)

        # Build module-name → rel-path map (for resolving imports)
        mod_map: Dict[str, str] = {}
        for rel in all_py:
            parts = rel.replace("\\", "/").rstrip(".py").split("/")
            # e.g. "pipeline/redis_client.py" → "pipeline.redis_client" and "redis_client"
            dotted = ".".join(p for p in parts if p != "__init__")
            dotted = dotted.removesuffix(".py") if dotted.endswith(".py") else dotted
            mod_map[dotted] = rel
            mod_map[parts[-1].removesuffix(".py")] = rel  # short name

        # Parse imports for each file
        for rel in all_py:
            full = os.path.join(sandbox, rel)
            deps: List[str] = []
            try:
                with open(full, "r", encoding="utf-8") as f:
                    src = f.read()
                tree = ast.parse(src)
                for node in ast.walk(tree):
                    candidates: List[str] = []
                    if isinstance(node, ast.Import):
                        candidates = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        candidates = [node.module]
                    for cand in candidates:
                        # Check full dotted name and each prefix
                        for length in range(len(cand.split(".")), 0, -1):
                            key = ".".join(cand.split(".")[:length])
                            if key in mod_map and mod_map[key] != rel:
                                target = mod_map[key]
                                if target not in deps:
                                    deps.append(target)
                                break
            except Exception:
                pass
            graph[rel] = deps

        _dep_graph_cache = graph
        _dep_graph_ts = now
        return graph

    def _get_dependents(self, changed_files: List[str]) -> List[str]:
        """
        Return all files that import ANY of the changed_files.
        Used to auto-expand Coder context when files are modified.
        """
        if not self.valves.ENABLE_DEP_GRAPH:
            return []
        graph = self._get_project_dependency_graph()
        changed_set: Set[str] = set(changed_files)
        # Normalise path separators
        changed_set = {p.replace("\\", "/") for p in changed_set}
        dependents: List[str] = []
        for rel, deps in graph.items():
            norm_deps = {d.replace("\\", "/") for d in deps}
            if norm_deps & changed_set and rel.replace("\\", "/") not in changed_set:
                dependents.append(rel)
        return dependents

    # ──────────────────────────────────────────────────────────
    # v5.5 [T5] ENVIRONMENT HEALTH CHECKER
    # ──────────────────────────────────────────────────────────
    async def _check_env_health(self) -> Dict[str, Any]:
        """
        Check Python environment for common issues BEFORE running tests.
        Returns {
            "ok": bool,
            "issues": [...],   # blocking problems
            "warnings": [...], # non-blocking
            "pytest_ok": bool,
            "asyncio_ok": bool,
            "shadowed_modules": [...]
        }
        Cached per pipe() invocation.
        """
        if self._env_health is not None:
            return self._env_health

        result: Dict[str, Any] = {
            "ok": True, "issues": [], "warnings": [],
            "pytest_ok": True, "asyncio_ok": True,
            "shadowed_modules": [],
        }

        sandbox = self.valves.SANDBOX_PATH
        stdlib_names = {"logging", "asyncio", "os", "sys", "json", "re",
                        "typing", "collections", "functools", "pathlib",
                        "datetime", "threading", "subprocess", "abc"}

        # Check 1: Name shadowing — project files that shadow stdlib
        shadowed: List[str] = []
        for name in stdlib_names:
            shadow_path = os.path.join(sandbox, f"{name}.py")
            if os.path.isfile(shadow_path):
                shadowed.append(name)
        if shadowed:
            result["shadowed_modules"] = shadowed
            result["ok"] = False
            for name in shadowed:
                result["issues"].append(
                    f"⚠️ SHADOW: `{name}.py` di root project memblokir stdlib `{name}`. "
                    f"Rename ke `{name}_utils.py` atau pindah ke subfolder."
                )

        # Check 2: asyncio health (quick import test)
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-c", "import asyncio; print('ok')",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                cwd=sandbox,
                env={**os.environ, "PYTHONPATH": sandbox},
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
            if proc.returncode != 0 or b"ok" not in stdout:
                result["asyncio_ok"] = False
                result["ok"] = False
                err = stderr.decode("utf-8", errors="replace")[:200]
                result["issues"].append(f"❌ asyncio rusak: {err}")
        except Exception as e:
            result["asyncio_ok"] = False
            result["warnings"].append(f"asyncio check skipped: {e}")

        # Check 3: pytest health
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-c", "import pytest; print('ok')",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                cwd=sandbox,
                env={**os.environ, "PYTHONPATH": sandbox},
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
            if proc.returncode != 0 or b"ok" not in stdout:
                result["pytest_ok"] = False
                err = stderr.decode("utf-8", errors="replace")[:300]
                result["warnings"].append(
                    f"pytest tidak bisa diimport: {err}\n"
                    f"💡 Coder akan menggunakan unittest sebagai fallback [T7]."
                )
        except Exception as e:
            result["warnings"].append(f"pytest check skipped: {e}")

        self._env_health = result
        return result

    # ──────────────────────────────────────────────────────────
    # v5.5 [T6] FAILURE MEMORY — accumulate retry context
    # ──────────────────────────────────────────────────────────
    def _record_failure(self, task_id: str, attempt: int, reason: str) -> None:
        """Record a failure for a task (for retry context accumulation)."""
        if task_id not in self._task_failure_log:
            self._task_failure_log[task_id] = []
        self._task_failure_log[task_id].append(
            f"Attempt {attempt + 1}: {reason[:300]}"
        )

    def _failure_history(self, task_id: str) -> str:
        """Return formatted failure history for a task."""
        history = self._task_failure_log.get(task_id, [])
        if not history:
            return ""
        lines = ["# 🔴 FAILURE HISTORY (semua attempt sebelumnya — JANGAN ulangi kesalahan ini):"]
        for entry in history:
            lines.append(f"  - {entry}")
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────
    # v5.5 [T8] TOKEN BUDGET OPTIMIZER
    # ──────────────────────────────────────────────────────────
    def _budget_remaining_ratio(self, tokens_before: Optional[Dict[str, int]] = None) -> float:
        """Return ratio of remaining token budget (1.0 = full, 0.0 = empty)."""
        budget = self.valves.MAX_TOKEN_BUDGET
        if budget <= 0:
            return 1.0
        used = self._total_tokens() - sum((tokens_before or {}).values())
        return max(0.0, 1.0 - (used / budget))

    def _dynamic_file_char_limit(self, tokens_before: Optional[Dict[str, int]] = None) -> int:
        """
        Dynamically adjust how many chars of each file to include in context
        based on remaining token budget.
        """
        ratio = self._budget_remaining_ratio(tokens_before)
        if ratio > 0.6:
            return MAX_FILE_CHARS_FOR_CODER        # 6000
        elif ratio > 0.35:
            return MAX_FILE_CHARS_FOR_CODER // 2   # 3000
        else:
            return MAX_FILE_CHARS_FOR_CODER // 4   # 1500

    # ──────────────────────────────────────────────────────────
    # PROMPT BUILDERS
    # ──────────────────────────────────────────────────────────
    def _prompt_plan_batch(
        self,
        batch_tasks: List[Dict[str, str]],
        compressed_knowledge: str,
        relevant_files: Dict[str, str],
    ) -> str:
        """
        v5.1 BATCH PLANNER — sees ALL tasks in current batch.
        """
        file_list = "\n".join(f"- {p}" for p in relevant_files.keys()) or "(none yet)"
        tasks_block = "\n".join(
            f"  [{i+1}] ID={t.get('id')} | Name={t.get('name', '')} | Desc={t.get('description', t.get('name', ''))}"
            for i, t in enumerate(batch_tasks)
        )
        project_ctx = self.valves.PROJECT_CONTEXT

        return f"""You are a planning agent for a software project.
Read ALL tasks in this batch and produce a plan for each one.

# PROJECT CONTEXT
{project_ctx}

# TASKS IN THIS BATCH
{tasks_block}

# COMPRESSED PROJECT KNOWLEDGE
{compressed_knowledge}

# EXISTING RELEVANT FILES
{file_list}

# COMPLEXITY SCORING GUIDE
Score 1-3: setup/config/schema/simple test — no reviewer needed
Score 4-5: single module, clear spec — borderline
Score 6-7: multi-file, async, integration — reviewer recommended
Score 8-10: cross-module pipeline logic — reviewer required

# YOUR JOB
Return ONLY a JSON array, one object per task, IN ORDER:
[
  {{
    "task_id": "P0-004",
    "approach": "One paragraph: what to build and how",
    "files_to_create": ["list of file paths"],
    "files_to_modify": ["existing files to change, if any"],
    "key_imports": ["main packages needed"],
    "test_approach": "How to test (one sentence)",
    "risks": ["potential issues"],
    "complexity_score": 5,
    "depends_on_previous": false
  }}
]

IMPORTANT:
- depends_on_previous: true if this task requires output from the previous task in this batch
- complexity_score must be an integer 1-10
- Return ONLY the JSON array, no markdown, no explanation
"""

    def _prompt_code(
        self,
        task: Dict[str, str],
        plan: Dict,
        compressed_knowledge: str,
        relevant_files: Dict[str, str],
        patch_candidates: Optional[List[str]] = None,   # v5.5 [T1]
        force_unittest: bool = False,                    # v5.5 [T7]
    ) -> str:
        """
        v5.1: Removed hard-coded mode_note. Uses PROJECT_CONTEXT valve instead.
        v5.5: Supports patch mode [T1] and framework fallback [T7].
        """
        files_section = ""
        for fname, content in relevant_files.items():
            files_section += f"\n\n### {fname}\n```python\n{content}\n```\n"
        if not files_section:
            files_section = "\n(No existing relevant files — create new ones as needed)"

        packages = self.valves.AVAILABLE_PACKAGES
        project_ctx = self.valves.PROJECT_CONTEXT

        # v5.5 [T7]: Framework constraint
        test_framework_note = ""
        if force_unittest:
            test_framework_note = (
                "\n⚠️ WAJIB: pytest TIDAK TERSEDIA di environment ini. "
                "Tulis semua test menggunakan `unittest` + `unittest.mock` ONLY. "
                "Jangan import pytest sama sekali.\n"
            )

        # v5.5 [T1]: Patch mode instructions
        patch_mode_section = ""
        if self.valves.ENABLE_SMART_PATCH and patch_candidates:
            pc_list = "\n".join(f"  - {p}" for p in patch_candidates)
            patch_mode_section = f"""
# ✂️ SMART PATCH MODE (v5.5)
File berikut sudah ada di disk dengan banyak baris.
Jika kamu HANYA mengubah sebagian kecil → gunakan format PATCHES (hemat token).
Jika kamu menulis ulang total → gunakan format FILES biasa.
File yang bisa dipatch:
{pc_list}

Format patches:
{{
  "patches": [
    {{
      "path": "relative/path/file.py",
      "search": "exact string to find (multiline OK)",
      "replace": "replacement string"
    }}
  ],
  "files": [...],   ← file BARU atau yang ditulis ulang total
  "test_file": "...",
  "summary": "..."
}}
Boleh gabungkan patches + files dalam 1 response.
"""

        return f"""You are an AI Coder for a software project.

# PROJECT CONTEXT
{project_ctx}
{test_framework_note}
# TASK
ID: {task.get('id')}
Name: {task.get('name', '')}
Description: {task.get('description', task.get('name', ''))}

# APPROVED PLAN
Approach: {plan.get('approach', '')}
Files to create: {plan.get('files_to_create', [])}
Files to modify: {plan.get('files_to_modify', [])}
Key imports: {plan.get('key_imports', [])}
Test approach: {plan.get('test_approach', '')}
Watch out for: {plan.get('risks', [])}

# PROJECT RULES (must follow)
{compressed_knowledge}

# EXISTING PROJECT FILES (for reference)
{files_section}

# AVAILABLE PACKAGES IN SANDBOX
{packages}
IMPORTANT: ONLY use packages from the list above. Do NOT import anything else.
For mocking in tests: use unittest.mock (AsyncMock, MagicMock, patch).
{patch_mode_section}
# ⚠️ ANTI-TRUNCATION RULES — MANDATORY
- Output code MUST be 100% complete and directly runnable
- NEVER use "...", "# rest of", "# TODO", or "pass #" as placeholders
- Every method mentioned in the task MUST have a FULL implementation
- Split into multiple files if content is too long — do NOT truncate
- Reviewer WILL reject any code with empty method bodies or truncation markers

# OUTPUT FORMAT
Return ONLY valid JSON (no markdown, no explanation):
{{
  "files": [
    {{
      "path": "relative/path/to/file.py",
      "content": "complete file content here"
    }}
  ],
  "test_file": "relative/path/to/test_file.py or empty string",
  "summary": "One sentence: what you built"
}}
"""

    def _prompt_review(
        self,
        task: Dict[str, str],
        written_files: Dict[str, str],
        coder_summary: str,
    ) -> str:
        files_section = ""
        for path, content in written_files.items():
            total_lines = content.count("\n") + 1
            total_chars = len(content)
            content_hash = hashlib.md5(content.encode()).hexdigest()[:8]
            preview = content[:MAX_FILE_CHARS_FOR_REVIEWER]
            if total_chars > MAX_FILE_CHARS_FOR_REVIEWER:
                shown_lines = preview.count("\n") + 1
                preview += (
                    f"\n# ... [TRUNCATED: showing {shown_lines}/{total_lines} lines,"
                    f" {MAX_FILE_CHARS_FOR_REVIEWER}/{total_chars} chars,"
                    f" md5={content_hash}]"
                    f"\n# NOTE TO REVIEWER: File is truncated. Do NOT reject solely for"
                    f" apparent incompleteness — judge only what is visible."
                )
            files_section += f"\n\n### {path}\n```python\n{preview}\n```\n"

        packages = self.valves.AVAILABLE_PACKAGES
        
        return f"""You are a code reviewer. Review the code below against the task requirements.

# TASK
ID: {task.get('id')}
Name: {task.get('name', '')}
Requirements: {task.get('description', task.get('name', ''))}

# CODER SUMMARY
{coder_summary}

# FILES WRITTEN
{files_section}

# REVIEW CHECKLIST
- [ ] Code is complete (no truncation, no placeholder implementations)
- [ ] All methods mentioned in requirements are implemented with real bodies
- [ ] Error handling is present (no silent fails)
- [ ] Type hints present
- [ ] Unit tests included (if applicable)
- [ ] No hardcoded values (config/env for parameters)
- [ ] Correct packages used (see below)

# AVAILABLE PACKAGES (sandbox has ONLY these)
{packages}
unittest.mock is available (AsyncMock, MagicMock, patch).
Do NOT reject for using stdlib or unittest.mock.
Do NOT reject for packages that are in the list above.

# IMPORTANT
- Output ONLY JSON, no markdown, no explanation
- catatan: plain string, max 2 sentences
- harus_diubah: plain string with comma-separated items (NOT a JSON array)

{{
  "verdict": "APPROVED" | "APPROVED_WITH_NOTES" | "REJECTED",
  "catatan": "Brief review note",
  "harus_diubah": "Specific items to fix (only if REJECTED)"
}}
"""

    def _prompt_retry(
        self,
        task: Dict[str, str],
        reviewer_feedback: str,
        file_list: List[str],
        compressed_knowledge: str,
        force_unittest: bool = False,    # v5.5 [T7]
        task_id: str = "",               # v5.5 [T6]
    ) -> str:
        """
        v5.1: Removed hard-coded mode_note. Uses PROJECT_CONTEXT.
        v5.5: Added failure history accumulation [T6] and framework fallback [T7].
        """
        packages = self.valves.AVAILABLE_PACKAGES
        project_ctx = self.valves.PROJECT_CONTEXT

        # v5.5 [T6]: Include all previous failures
        failure_history = self._failure_history(task_id) if task_id else ""

        # v5.5 [T7]: Framework constraint
        test_framework_note = ""
        if force_unittest:
            test_framework_note = (
                "\n⚠️ WAJIB: pytest TIDAK TERSEDIA. "
                "Gunakan unittest + unittest.mock ONLY. Jangan import pytest.\n"
            )

        return f"""You are an AI Coder. Your previous attempt was REJECTED.

# PROJECT CONTEXT
{project_ctx}
{test_framework_note}
# TASK (same as before)
ID: {task.get('id')}
Name: {task.get('name', '')}
Description: {task.get('description', task.get('name', ''))}

{failure_history}

# 🔴 CURRENT FEEDBACK (MUST address ALL points)
{reviewer_feedback}

# FILES TO REWRITE
{chr(10).join(f"- {f}" for f in file_list)}

# CRITICAL RULES (non-negotiable)
- Output code MUST be 100% complete — no "...", "# TODO", "pass #"
- Use ONLY these packages: {packages}
- For mock in tests: use unittest.mock (AsyncMock, MagicMock, patch)
- Fix ALL points in failure history AND current feedback above

# PROJECT CONVENTIONS
{compressed_knowledge}

# OUTPUT FORMAT
Return ONLY valid JSON:
{{
  "files": [
    {{
      "path": "relative/path/file.py",
      "content": "complete fixed content"
    }}
  ],
  "test_file": "path or empty",
  "summary": "What you fixed"
}}
"""

    # ──────────────────────────────────────────────────────────
    # KNOWLEDGE LOADER
    # ──────────────────────────────────────────────────────────
    async def _load_knowledge(self, emit: Callable) -> Optional[Dict[str, str]]:
        knowledge_dir = self.valves.KNOWLEDGE_PATH
        if not os.path.isdir(knowledge_dir):
            await self._status(emit, f"❌ Knowledge path not found: {knowledge_dir}", done=True)
            return None

        patterns = [
            r".*permanen.*\.md$", r".*permanent.*\.md$", r".*context.*\.md$",
            r".*status.*\.md$", r".*progress.*\.md$",
            r".*aturan.*\.md$", r".*rules.*\.md$", r".*sistem.*\.md$",
        ]

        knowledge: Dict[str, str] = {}
        found_files = os.listdir(knowledge_dir)

        for pattern in patterns:
            for fname in found_files:
                if re.match(pattern, fname.lower()) and fname not in knowledge:
                    fpath = os.path.join(knowledge_dir, fname)
                    content = await self._read(fpath)
                    if content.startswith("[ERROR"):
                        await self._status(emit, f"❌ Failed to read {fname}: {content}", done=True)
                        return None
                    knowledge[fname] = content
                    await self._status(emit, f"📖 Loaded: {fname}")

        if not knowledge:
            await self._status(emit, "❌ No knowledge files found in knowledge path", done=True)
            return None

        return knowledge

    # ──────────────────────────────────────────────────────────
    # TASK ID PARSER (v5.1 — batch support)
    # ──────────────────────────────────────────────────────────
    def _parse_task_ids(self, user_msg: str) -> List[str]:
        """
        v5.3: Parse task IDs — fully flexible format, no lock on prefix shape.
        """
        raw_ids = re.findall(r"[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+", user_msg)
        all_ids = [tid.upper() for tid in raw_ids]
        if not all_ids:
            return []

        def split_prefix_num(tid: str) -> Tuple[str, int]:
            m = re.match(r"^(.*)-(\d+)$", tid)
            if m:
                return m.group(1), int(m.group(2))
            return tid, 0

        range_match = re.search(
            r"([A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+)"
            r"\s+(?:sampai|to|hingga|until)\s+"
            r"([A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+)",
            user_msg, re.IGNORECASE
        )
        if range_match:
            id_a = range_match.group(1).upper()
            id_b = range_match.group(2).upper()
            prefix_a, num_a = split_prefix_num(id_a)
            prefix_b, num_b = split_prefix_num(id_b)
            # FIXED: [Lapis 2 Bug 7 - Retain IDs outside the range]
            if prefix_a == prefix_b and num_a <= num_b:
                width = max(len(str(num_a)), len(str(num_b)))
                expanded = [f"{prefix_a}-{str(n).zfill(width)}" for n in range(num_a, num_b + 1)]
                range_ids = {id_a, id_b}
                extra = [tid for tid in all_ids if tid not in expanded and tid not in range_ids]
                return expanded + extra

        seen: set = set()
        result: List[str] = []
        for tid in all_ids:
            if tid not in seen:
                seen.add(tid)
                result.append(tid)
        return result

    # ──────────────────────────────────────────────────────────
    # COMMAND DETECTION (v5.4 — flexible, not locked to exact strings)
    # ──────────────────────────────────────────────────────────

    _CMD_QUICK_CHECK = {
        "action": ["cek", "lihat", "status", "ringkasan", "summary", "rekap", "recap"],
        "subject": ["phase", "fase", "tahap"],
    }
    _CMD_DEEP_CHECK = {
        "action": ["check", "periksa", "verifikasi", "verify", "validasi", "validate",
                   "audit", "inspect", "diagnosa", "diagnose", "deep"],
        "subject": ["phase", "fase", "tahap"],
    }
    _CMD_FIX = {
        "action": ["fix", "perbaiki", "ulangi", "retry", "redo", "ulang",
                   "kerjain ulang", "kerjakan ulang", "jalanin ulang"],
        "subject": ["phase", "fase", "tahap"],
    }

    def _detect_command(self, user_msg: str) -> Optional[Tuple[str, str]]:
        """
        v5.4: Flexible command detection — not locked to exact strings.
        """
        msg_lo = user_msg.lower()

        phase_prefix: Optional[str] = None

        # FIXED: [Gemini Audit Bug 1 - Extract phase_prefix from AFTER subject keyword, not any capitalized word]
        # Strategy: look for subject keyword (phase/fase/tahap) followed by the prefix
        subject_match = re.search(
            r"(?:phase|fase|tahap)\s+([A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*)",
            user_msg, re.IGNORECASE
        )
        if subject_match:
            candidate = subject_match.group(1).upper()
            # Must not be a full task ID (e.g. P0-001)
            if not re.match(r"^[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+$", candidate):
                phase_prefix = candidate

        # Fallback: scan for standalone phase-like token (e.g. "fix P0") only if format is strict alphanum+digits
        if not phase_prefix:
            for m in re.finditer(r"[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*", user_msg):
                candidate = m.group(0).upper()
                if candidate.lower() in {
                    "phase", "fase", "tahap", "check", "cek", "fix", "perbaiki",
                    "status", "lihat", "periksa", "ulangi", "retry", "semua", "all",
                    "the", "yang", "dan", "atau",
                }:
                    continue
                if re.match(r"^[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+$", candidate):
                    continue
                # Only accept strict phase-format: letters+digits (e.g. P0, P10, SPRINT3)
                if re.match(r"^[A-Z]+\d+$", candidate):
                    phase_prefix = candidate
                    break

        if not phase_prefix:
            return None

        def has_any(keywords: List[str]) -> bool:
            return any(kw in msg_lo for kw in keywords)

        is_fix = has_any(self._CMD_FIX["action"])
        is_deep = has_any(self._CMD_DEEP_CHECK["action"])
        is_quick = has_any(self._CMD_QUICK_CHECK["action"])

        has_subject = has_any(self._CMD_QUICK_CHECK["subject"])

        # FIXED: [Lapis 3 Bug 2 - Command Hijacker Regex fix]
        if is_fix and has_subject:
            return ("fix_phase", phase_prefix)
        elif is_fix and re.match(r"^[A-Z]+\d+$", phase_prefix or ""):
            return ("fix_phase", phase_prefix)
            
        if is_deep and has_subject:
            return ("deep_check", phase_prefix)
        elif is_deep and re.match(r"^[A-Z]+\d+$", phase_prefix or ""):
            return ("deep_check", phase_prefix)
            
        if is_quick and has_subject:
            return ("quick_check", phase_prefix)
        elif is_quick and re.match(r"^[A-Z]+\d+$", phase_prefix or ""):
            return ("quick_check", phase_prefix)

        return None

    # ──────────────────────────────────────────────────────────
    # QUICK PHASE CHECK — reads status table only (0 token)
    # ──────────────────────────────────────────────────────────
    def _phase_quick_check(self, phase_prefix: str, all_tasks: List[Dict[str, str]]) -> str:
        """Read status_dev.md table, return summary. No disk I/O beyond what's loaded."""
        def get_prefix(tid: str) -> str:
            m = re.match(r"^(.*)-\d+$", tid)
            return m.group(1) if m else tid

        phase_tasks = [t for t in all_tasks if get_prefix(t["id"]) == phase_prefix.upper()]

        if not phase_tasks:
            return (
                f"## 🔍 Phase {phase_prefix} — Tidak Ditemukan\n\n"
                f"Tidak ada task dengan prefix `{phase_prefix}` di status_dev.md.\n"
                f"Pastikan prefix phase sesuai (case-insensitive)."
            )

        done, failed, in_progress, pending = [], [], [], []
        for t in phase_tasks:
            s = t.get("status", "").lower()
            if any(kw in s for kw in ["✅", "selesai", "done", "complete", "approved", "finish"]):
                done.append(t)
            elif any(kw in s for kw in ["❌", "error", "fail", "gagal", "blocked", "rejected"]):
                failed.append(t)
            elif any(kw in s for kw in ["🔄", "in progress", "sedang", "running", "wip"]):
                in_progress.append(t)
            else:
                pending.append(t)

        total = len(phase_tasks)
        n_done = len(done)
        bar = "█" * int((n_done / total) * 20) + "░" * (20 - int((n_done / total) * 20))

        def fmt(t: Dict, e: str) -> str:
            name = t.get("name", "")
            n = f" — {name}" if name and name != t["id"] else ""
            note = t.get("notes", "")
            nt = f"\n   _{note[:80]}_" if note else ""
            return f"{e} `{t['id']}`{n}{nt}"

        lines = [
            f"## 📊 Phase {phase_prefix} — {n_done}/{total} selesai",
            f"`{bar}` {int(n_done/total*100)}%", "",
        ]
        if done:
            lines += ["### ✅ Selesai"] + [fmt(t, "✅") for t in done] + [""]
        if in_progress:
            lines += ["### 🔄 In Progress"] + [fmt(t, "🔄") for t in in_progress] + [""]
        if failed:
            lines += ["### ❌ Gagal"] + [fmt(t, "❌") for t in failed] + [""]
        if pending:
            lines += ["### ⏳ Pending"] + [fmt(t, "⏳") for t in pending] + [""]

        parts = [f"✅ {n_done} selesai"]
        if in_progress: parts.append(f"🔄 {len(in_progress)} in progress")
        if failed: parts.append(f"❌ {len(failed)} gagal")
        if pending: parts.append(f"⏳ {len(pending)} pending")
        lines += ["---", " · ".join(parts)]

        if failed or in_progress:
            lines.append(f"\n💡 Untuk deep check file di disk: `check phase {phase_prefix}`")
            lines.append(f"💡 Untuk retry otomatis: `fix phase {phase_prefix}`")
        elif pending:
            lines.append(f"\n💡 Lanjut task berikutnya: `kerjakan {pending[0]['id']}`")
        elif n_done == total:
            lines.append(f"\n🎉 **Phase {phase_prefix} complete!**")
            lines.append(f"💡 Verifikasi file asli: `check phase {phase_prefix}`")

        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────
    # DEEP PHASE CHECK — checks actual files on disk (0 token)
    # ──────────────────────────────────────────────────────────
    async def _phase_deep_check(
        self,
        phase_prefix: str,
        all_tasks: List[Dict[str, str]],
        emit: Callable,
        __event_call__: Optional[Callable] = None, # FIXED: [Lapis 2 Bug 6 - Pass event_call to deep check]
    ) -> Tuple[str, List[str]]:
        """
        v5.4: For each ✅ task in phase, actually verify the written files.
        """
        def get_prefix(tid: str) -> str:
            m = re.match(r"^(.*)-\d+$", tid)
            return m.group(1) if m else tid

        phase_tasks = [t for t in all_tasks if get_prefix(t["id"]) == phase_prefix.upper()]

        if not phase_tasks:
            return (
                f"## 🔍 Phase {phase_prefix} — Tidak Ditemukan\n\n"
                f"Tidak ada task dengan prefix `{phase_prefix}` di status_dev.md.",
                []
            )

        confirmed: List[Dict] = []   # all checks passed
        suspect: List[Dict] = []     # issues found but not hard fail
        failed_tasks: List[Dict] = []  # hard fail (file missing / syntax error)
        skipped: List[Dict] = []     # status not ✅ — skip deep check

        sandbox = self.valves.SANDBOX_PATH

        for t in phase_tasks:
            tid = t["id"]
            s = t.get("status", "").lower()
            is_done = any(kw in s for kw in ["✅", "selesai", "done", "complete", "approved", "finish"])

            if not is_done:
                skipped.append(t)
                continue

            await self._status(emit, f"🔬 Deep check: {tid}...")

            files_str = t.get("files", "")
            file_paths = [f.strip() for f in re.split(r"[,\n]", files_str) if f.strip()]

            task_issues: List[str] = []
            task_warnings: List[str] = []

            for rel_path in file_paths:
                if not rel_path:
                    continue

                full_path = os.path.join(sandbox, rel_path)

                # FIXED: [Lapis 5 Bug G - Check existence for all files, even non-python]
                if not os.path.isfile(full_path):
                    task_issues.append(f"`{rel_path}` — file tidak ada di disk!")
                    continue
                
                if not rel_path.endswith((".py", ".yaml", ".yml", ".json")):
                    continue

                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        code = f.read()
                except Exception as e:
                    task_issues.append(f"`{rel_path}` — gagal baca: {e}")
                    continue

                if not rel_path.endswith(".py"):
                    continue

                # Check 2: syntax
                try:
                    ast.parse(code)
                except SyntaxError as e:
                    task_issues.append(f"`{rel_path}` — syntax error baris {e.lineno}: {e.msg}")
                    continue

                # Check 3: truncation markers (hard issues only)
                validation = self._validate_code([{"path": rel_path, "content": code}])
                for issue in validation:
                    task_issues.append(issue)

                # FIXED: [Gemini Audit Bug 4 - Consistent import handling: use soft warnings same as execute_task]
                soft = self._validate_code_soft([{"path": rel_path, "content": code}])
                for issue in soft:
                    task_warnings.append(issue)

            # Check 4: run test file
            # FIXED: [Gemini Audit Bug 3 - Don't require "test" in filename; fallback to any .py file]
            test_result_str = ""
            all_py_files = [f.strip() for f in re.split(r"[,\n]", files_str) if f.strip().endswith(".py")]
            # Priority 1: explicit test/qa/check/spec/verify naming
            test_files = [f for f in all_py_files if any(kw in f.lower() for kw in ("test", "qa", "check", "spec", "verify"))]
            # Priority 2: any .py file as fallback (last resort)
            if not test_files:
                test_files = all_py_files
            if test_files:
                test_full = os.path.join(sandbox, test_files[0])
                if os.path.isfile(test_full):
                    # FIXED: [Lapis 2 Bug 6 - Ask confirmation here]
                    proceed_test = True
                    if self.valves.REQUIRE_USER_CONFIRM and __event_call__:
                        proceed_test = await self._confirm(__event_call__, f"Run test for {tid}: {test_files[0]}?")
                    
                    if not proceed_test:
                        task_warnings.append("test skipped — user declined")
                    else:
                        await self._status(emit, f"  🧪 Running test: {test_files[0]}...")
                        ok, output = await self._exec(test_full)
                        if not ok:
                            missing_pkg = re.search(r"ModuleNotFoundError: No module named '([^']+)'", output)
                            if missing_pkg:
                                task_warnings.append(f"test skipped — missing package '{missing_pkg.group(1)}'")
                            else:
                                task_issues.append(f"test failed: {output[:150]}")
                            test_result_str = output[:200]
                        else:
                            test_result_str = "✅ passed"

            # Classify this task
            t_copy = dict(t)
            t_copy["_issues"] = task_issues
            t_copy["_warnings"] = task_warnings
            t_copy["_test"] = test_result_str

            if task_issues:
                failed_tasks.append(t_copy)
            elif task_warnings:
                suspect.append(t_copy)
            else:
                confirmed.append(t_copy)

        # ── Build report ─────────────────────────────────────────
        total_checked = len(confirmed) + len(suspect) + len(failed_tasks)
        total = len(phase_tasks)

        lines = [
            f"## 🔬 Deep Check Phase {phase_prefix} — {len(confirmed)}/{total_checked} OK",
            f"_(Checked {total_checked} task ✅, skipped {len(skipped)} task non-✅)_",
            "",
        ]

        if confirmed:
            lines.append("### ✅ Confirmed — File & Test OK")
            for t in confirmed:
                test_note = f" · test {t['_test']}" if t["_test"] else ""
                lines.append(f"✅ `{t['id']}` — {t.get('name','')}{test_note}")
            lines.append("")

        if suspect:
            lines.append("### ⚠️ Suspect — Ada Warning")
            for t in suspect:
                lines.append(f"⚠️ `{t['id']}` — {t.get('name','')}")
                for w in t["_warnings"]:
                    lines.append(f"   - {w}")
            lines.append("")

        if failed_tasks:
            lines.append("### ❌ Failed — Perlu Dikerjakan Ulang")
            for t in failed_tasks:
                lines.append(f"❌ `{t['id']}` — {t.get('name','')}")
                for iss in t["_issues"]:
                    lines.append(f"   - {iss}")
            lines.append("")

        if skipped:
            non_done_ids = ", ".join(f"`{t['id']}`" for t in skipped)
            lines.append(f"_⏭️ Dilewati (bukan ✅): {non_done_ids}_")
            lines.append("")

        # Summary & next action
        problem_ids = [t["id"] for t in failed_tasks + suspect]
        lines.append("---")
        if not problem_ids:
            lines.append(f"🎉 **Semua task phase {phase_prefix} verified OK!**")
        else:
            lines.append(f"⚠️ **{len(problem_ids)} task bermasalah**: {', '.join(f'`{i}`' for i in problem_ids)}")
            lines.append(f"\n💡 Kerjakan ulang otomatis: `fix phase {phase_prefix}`")
            lines.append(f"💡 Atau manual: `kerjakan {', '.join(problem_ids)}`")

        return "\n".join(lines), problem_ids

    # ──────────────────────────────────────────────────────────
    # FIX PHASE — retry failed/suspect tasks (v5.4)
    # ──────────────────────────────────────────────────────────
    def _get_failed_task_ids(
        self,
        phase_prefix: str,
        all_tasks: List[Dict[str, str]],
    ) -> List[str]:
        """
        v5.4: Get task IDs that need fixing for a phase.
        Reads from status_dev.md table — tasks marked ❌/error/gagal.
        Also includes non-started (pending) tasks with no status.
        """
        def get_prefix(tid: str) -> str:
            m = re.match(r"^(.*)-\d+$", tid)
            return m.group(1) if m else tid

        phase_tasks = [t for t in all_tasks if get_prefix(t["id"]) == phase_prefix.upper()]
        failed = []
        for t in phase_tasks:
            s = t.get("status", "").lower()
            if any(kw in s for kw in ["❌", "error", "fail", "gagal", "blocked", "rejected", "suspect", "⚠️"]):
                failed.append(t["id"])
        return failed

    # ──────────────────────────────────────────────────────────
    # TASK EXECUTION
    # ──────────────────────────────────────────────────────────
    async def _execute_task(
        self,
        task_id: str,
        task_info: Dict[str, str],
        plan: Dict,
        knowledge: Dict[str, str],
        status_dev_content: str,
        compressed: str,
        __request__: Request,
        __user__: Any,
        __event_call__: Callable,
        emit: Callable,
        task_num: int,
        total_tasks: int,
        tokens_before: Optional[Dict[str, int]] = None,  # FIXED: [Gemini Audit Bug 2 - per-task delta budget]
    ) -> Tuple[bool, str, str]:
        """
        Execute a single task through the full pipeline.
        Returns: (success, final_status_content, result_markdown)
        """
        prefix = f"[{task_num}/{total_tasks}] {task_id}"
        reviewer_skipped = False
        complexity_score = plan.get("complexity_score", 5)
        threshold = self.valves.REVIEWER_THRESHOLD

        # v5.5 [T5]: Check env health once per batch (cached)
        if self.valves.ENABLE_ENV_CHECK and self._env_health is None:
            await self._status(emit, "🏥 Checking environment health...")
            env_health = await self._check_env_health()
            for issue in env_health.get("issues", []):
                await self._status(emit, f"⚠️ ENV: {issue}")
            for warn in env_health.get("warnings", []):
                await self._status(emit, f"ℹ️ ENV: {warn}")
        else:
            env_health = self._env_health or {"ok": True, "pytest_ok": True}

        # v5.5 [T7]: Force unittest if pytest is broken
        force_unittest = not env_health.get("pytest_ok", True)

        # ── Enrich relevant files using plan.files_to_modify ────
        files_to_modify = plan.get("files_to_modify", [])
        files_to_create = plan.get("files_to_create", [])

        # v5.5 [T4]: Expand context via dep graph
        all_touched = list(files_to_modify) + list(files_to_create)
        relevant_files = await self._scan_files(
            task_info.get("description", task_info.get("name", "")),
            extra_paths=files_to_modify if isinstance(files_to_modify, list) else [],
            dep_expand_paths=all_touched if isinstance(all_touched, list) else [],
            tokens_before=tokens_before,
        )
        await self._status(emit, f"{prefix}: 📁 {len(relevant_files)} file(s) in context")

        # v5.5 [T1]: Identify patch candidates (existing files that are large)
        patch_candidates: List[str] = []
        if self.valves.ENABLE_SMART_PATCH:
            for rel in (files_to_modify or []):
                if self._file_line_count(rel) >= self.valves.PATCH_MIN_LINES:
                    patch_candidates.append(rel)

        # ── STEP 2-4: CODE + VALIDATE + REVIEW LOOP ─────────────
        reviewer_feedback = ""
        verdict: Dict = {}
        v = ""
        written_files: Dict[str, str] = {}
        coder_output: Dict = {}
        original_prompt = self._prompt_code(
            task_info, plan, compressed, relevant_files,
            patch_candidates=patch_candidates,
            force_unittest=force_unittest,
        )

        for attempt in range(self.valves.MAX_RETRY + 1):
            # Token budget check — use delta (task-only spend) so batch report stays cumulative
            # FIXED: [Gemini Audit Bug 2 - delta check, not cumulative check]
            tokens_before_total = sum((tokens_before or {}).values())
            task_tokens_used = self._total_tokens() - tokens_before_total
            if task_tokens_used > self.valves.MAX_TOKEN_BUDGET:
                await self._status(emit, f"⚠️ Token budget exceeded", done=True)
                await self._commit_status(
                    knowledge, status_dev_content, task_id,
                    {"status": "⚠️ Budget exceeded", "notes": f"tokens={self._total_tokens()}"},
                    emit,
                )
                return False, status_dev_content, f"⚠️ Token budget exceeded\n\n{self._token_report()}"

            label = "first attempt" if attempt == 0 else f"retry {attempt}"
            await self._status(emit, f"{prefix}: 🧠 Coding ({label})...")

            coder_prompt = (
                original_prompt if attempt == 0
                else self._prompt_retry(
                    task_info, reviewer_feedback,
                    list(written_files.keys()) or plan.get("files_to_create", []),
                    compressed,
                    force_unittest=force_unittest,  # v5.5 [T7]
                    task_id=task_id,                # v5.5 [T6]
                )
            )

            coder_response = await self._llm(
                __request__, self.valves.CODER_MODEL,
                [{"role": "user", "content": coder_prompt}],
                user_obj=__user__, track_key=f"coder_{task_id}",
            )

            if not coder_response or coder_response.startswith("[ERROR]"):
                if not self._retryable(coder_response or ""):
                    err = self._classify(coder_response or "")
                    await self._status(emit, f"{prefix}: 🛑 Infrastructure error", done=False)
                    await self._commit_status(
                        knowledge, status_dev_content, task_id,
                        {"status": "❌ Infra error", "notes": err[:100]},
                        emit,
                    )
                    await self._log_task(task_id, "INFRA_ERROR", [], self._total_tokens(), False, err)
                    return False, status_dev_content, f"❌ Infrastructure Error: {err}"
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = coder_response or "LLM failed"
                    self._record_failure(task_id, attempt, reviewer_feedback)  # v5.5 [T6]
                    continue
                await self._commit_status(
                    knowledge, status_dev_content, task_id,
                    {"status": "❌ Coder failed", "notes": f"failed after {self.valves.MAX_RETRY} retries"},
                    emit,
                )
                await self._log_task(task_id, "CODER_FAILED", [], self._total_tokens(), False)
                return False, status_dev_content, f"❌ Coder failed after {self.valves.MAX_RETRY} retries"

            # Parse coder output
            try:
                # FIXED: [Lapis 2 Bug 4 - proper json decoder]
                coder_output = self._extract_json(coder_response)
                files_to_write = coder_output.get("files", [])

                # v5.5 [T1]: Handle patch format
                patches_to_apply = coder_output.get("patches", [])
                if patches_to_apply and not files_to_write:
                    raise ValueError("patches present but no 'files' list — will apply patches only")
                if not files_to_write and not patches_to_apply:
                    raise ValueError("No files or patches in output")
            except Exception as e:
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = f"JSON parse error: {e}. Return valid JSON."
                    self._record_failure(task_id, attempt, reviewer_feedback)  # v5.5 [T6]
                    continue
                await self._commit_status(
                    knowledge, status_dev_content, task_id,
                    {"status": "❌ Parse error", "notes": str(e)[:100]},
                    emit,
                )
                return False, status_dev_content, f"❌ Coder output invalid: {e}"

            # ── STEP 3: STATIC VALIDATION ────────────────────────
            all_files_for_validation = list(files_to_write or [])
            validation_issues = self._validate_code(all_files_for_validation)
            # FIXED: [Gemini Audit Bug 1 - Soft warnings shown separately, never trigger retry]
            soft_warnings = self._validate_code_soft(all_files_for_validation)
            if soft_warnings:
                await self._status(emit, f"{prefix}: ℹ️ Soft warnings (local modules?): {len(soft_warnings)} — will not retry")
            if validation_issues:
                issues_text = "\n".join(f"- {i}" for i in validation_issues)
                await self._status(emit, f"{prefix}: ⚠️ Static validation: {len(validation_issues)} issue(s)")
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = (
                        f"Static validation failed — fix these:\n{issues_text}\n"
                        f"Ensure code is 100% complete with no truncation."
                    )
                    self._record_failure(task_id, attempt, reviewer_feedback)  # v5.5 [T6]
                    continue

            # ── Write files (full rewrites) ──────────────────────
            if not self.valves.READ_ONLY_MODE:
                await self._status(emit, f"{prefix}: 💾 Writing {len(files_to_write)} file(s)...")
                written_files = {}
                failed_writes = []

                for fi in (files_to_write or []):
                    path = fi.get("path", "")
                    content = fi.get("content", "")
                    if not path or not content:
                        # FIXED: [Lapis 5 Bug F - Skip empty content gracefully]
                        await self._status(emit, f"  ⚠️ Skipped {path} — empty content")
                        failed_writes.append(path)
                        continue
                        
                    full = os.path.join(self.valves.SANDBOX_PATH, path)
                    ok = await self._write(full, content)
                    if ok:
                        written_files[path] = content
                        await self._status(emit, f"  ✅ {path}")
                    else:
                        failed_writes.append(path)
                        await self._status(emit, f"  ❌ Failed: {path}")

                # v5.5 [T1]: Apply patches
                if patches_to_apply:
                    await self._status(emit, f"{prefix}: ✂️ Applying {len(patches_to_apply)} patch(es)...")
                    for patch in patches_to_apply:
                        p_path = patch.get("path", "")
                        p_search = patch.get("search", "")
                        p_replace = patch.get("replace", "")
                        if not p_path or not p_search:
                            await self._status(emit, f"  ⚠️ Skipped malformed patch for {p_path}")
                            continue
                        ok, msg = await self._apply_patch(p_path, p_search, p_replace)
                        if ok:
                            # Read updated content into written_files for review
                            full = os.path.join(self.valves.SANDBOX_PATH, p_path)
                            try:
                                with open(full, "r", encoding="utf-8") as f:
                                    written_files[p_path] = f.read()
                            except Exception:
                                pass
                            await self._status(emit, f"  ✅ patch: {p_path}")
                        else:
                            await self._status(emit, f"  ❌ patch failed: {msg}")
                            failed_writes.append(p_path)

                if failed_writes:
                    # FIXED: [Lapis 4 Bug 5 - Sabotage by hardcoding error string]
                    err_msg = f"failed to write {failed_writes[0]}"
                    if not self._retryable(err_msg):
                        err = self._classify(err_msg)
                        await self._commit_status(
                            knowledge, status_dev_content, task_id,
                            {"status": "❌ Write error", "notes": err[:100]},
                            emit,
                        )
                        return False, status_dev_content, f"❌ Write failed: {', '.join(failed_writes)}\n{err}"
            else:
                written_files = {
                    fi.get("path", f"file_{i}"): fi.get("content", "")
                    for i, fi in enumerate(files_to_write or [])
                }

            # v5.5 [T3]: ZERO-TOKEN STATIC LINTER before reviewer
            linter_issues = await self._run_static_linter(
                [{"path": p, "content": c} for p, c in written_files.items()]
            )
            if linter_issues:
                linter_text = "\n".join(linter_issues[:20])
                await self._status(emit, f"{prefix}: 🔍 Linter: {len(linter_issues)} issue(s) — sending back to coder")
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = (
                        f"Static linter ({self._detect_linter()}) found issues — fix them:\n"
                        f"{linter_text}\n"
                        f"These are objective code quality issues. Fix all of them."
                    )
                    self._record_failure(task_id, attempt, f"linter: {len(linter_issues)} issues")  # v5.5 [T6]
                    continue
                # If no retries left, pass linter issues to reviewer as context

            # ── STEP 4: SMART REVIEWER GATE (v5.1) ───────────────
            if complexity_score < threshold:
                # Skip reviewer — auto-approve
                reviewer_skipped = True
                v = "AUTO_APPROVED"
                verdict = {
                    "verdict": v,
                    "catatan": f"Auto-approved (complexity={complexity_score} < threshold={threshold})",
                    "harus_diubah": "",
                }
                await self._status(emit, f"{prefix}: ⚡ Auto-approved (complexity {complexity_score} < {threshold})")
                break
            else:
                # Call reviewer
                await self._status(emit, f"{prefix}: 🔍 Reviewing (complexity={complexity_score})...")
                coder_summary = coder_output.get("summary", "(no summary)")
                reviewer_prompt = self._prompt_review(task_info, written_files, coder_summary)
                reviewer_response = await self._llm(
                    __request__, self.valves.REVIEWER_MODEL,
                    [{"role": "user", "content": reviewer_prompt}],
                    user_obj=__user__, track_key=f"reviewer_{task_id}",
                )

                try:
                    # FIXED: [Lapis 4 Bug 1 - Bypass Reviewer fix]
                    verdict = self._extract_json(reviewer_response)
                    v = verdict.get("verdict", "REJECTED")
                except Exception as e:
                    v = "REJECTED"
                    verdict = {"verdict": v, "catatan": f"Reviewer parse error: {e}. Return valid JSON.", "harus_diubah": ""}

                if "APPROVED" in v:
                    await self._status(emit, f"{prefix}: ✅ {v}!")
                    break

                # Process rejection
                raw_fb = verdict.get("harus_diubah", "") or verdict.get("catatan", "")
                reviewer_feedback = ", ".join(str(x) for x in raw_fb) if isinstance(raw_fb, list) else str(raw_fb)

                if not reviewer_feedback.strip():
                    reviewer_feedback = (
                        "Reviewer rejected without specific feedback. "
                        "Ensure: complete implementations, error handling, type hints, "
                        "unit tests present, no truncated code."
                    )

                self._record_failure(task_id, attempt, reviewer_feedback)  # v5.5 [T6]

                if not self._retryable(reviewer_feedback):
                    err = self._classify(reviewer_feedback)
                    await self._commit_status(
                        knowledge, status_dev_content, task_id,
                        {"status": "❌ Infra rejection", "notes": err[:100]},
                        emit,
                    )
                    return False, status_dev_content, f"❌ Infrastructure rejection\n{err}"

                if attempt < self.valves.MAX_RETRY:
                    await self._status(emit, f"{prefix}: ❌ REJECTED — {self.valves.MAX_RETRY - attempt} retries left...")
                else:
                    await self._status(emit, f"{prefix}: ❌ REJECTED after {self.valves.MAX_RETRY} retries.")
                    await self._commit_status(
                        knowledge, status_dev_content, task_id,
                        {"status": "❌ Max retries", "notes": reviewer_feedback[:100]},
                        emit,
                    )
                    await self._log_task(task_id, "MAX_RETRIES", list(written_files.keys()), self._total_tokens(), False)
                    return False, status_dev_content, (
                        f"Task {task_id} failed after {self.valves.MAX_RETRY} retries.\n"
                        f"**Feedback:** {reviewer_feedback}\n\n{self._token_report()}"
                    )

        # ── STEP 5: EXECUTE TESTS ────────────────────────────────
        test_result = ""
        test_file = coder_output.get("test_file", "")
        if test_file and not self.valves.READ_ONLY_MODE:
            test_path = os.path.join(self.valves.SANDBOX_PATH, test_file)
            if os.path.isfile(test_path):
                proceed = True
                if self.valves.REQUIRE_USER_CONFIRM:
                    proceed = await self._confirm(__event_call__, f"Run test for {task_id}: {test_file}?")
                if proceed:
                    await self._status(emit, f"{prefix}: 🧪 Running: {test_file}...")
                    ok, output = await self._exec(test_path)
                    test_result = output
                    missing_pkg_match = re.search(
                        r"ModuleNotFoundError: No module named '([^']+)'", output
                    )
                    if not ok and missing_pkg_match:
                        missing_pkg = missing_pkg_match.group(1)
                        await self._status(
                            emit,
                            f"⚠️ Tests skipped — missing package '{missing_pkg}' in sandbox. "
                            f"Add to AVAILABLE_PACKAGES valve or install in container."
                        )
                        test_result = f"[SKIPPED] Missing package: '{missing_pkg}'. Install in sandbox and re-run."
                    else:
                        await self._status(emit, "✅ Tests passed" if ok else f"❌ Tests failed: {output[:100]}")
                else:
                    test_result = "Cancelled by user"

        # ── STEP 6: UPDATE STATUS (ALWAYS) ──────────────────────
        files_str = ", ".join(written_files.keys())
        notes = verdict.get("catatan", "")[:150] if verdict else ""
        if reviewer_skipped:
            notes = f"auto-approved (score={complexity_score})" + (f"; {notes}" if notes else "")

        status_dev_content = await self._commit_status(
            knowledge, status_dev_content, task_id,
            {"status": "✅ Selesai", "files": files_str, "notes": notes},
            emit,
        )
        for fname in knowledge.keys():
            if "status" in fname.lower() or "progress" in fname.lower():
                knowledge[fname] = status_dev_content

        # ── STEP 7: LOG ──────────────────────────────────────────
        await self._log_task(
            task_id, v, list(written_files.keys()),
            self._total_tokens(), reviewer_skipped,
        )

        # ── BUILD RESULT MARKDOWN ────────────────────────────────
        files_md = "\n".join(f"- `{p}`" for p in written_files.keys())
        reviewer_gate_note = (
            f"⚡ **Reviewer Skipped** (complexity score {complexity_score} < threshold {threshold})"
            if reviewer_skipped
            else f"🔍 **Reviewer Called** (complexity score {complexity_score})"
        )

        result_md = f"""### ✅ {task_id} — Complete

**Verdict**: {v}
{reviewer_gate_note}

**Summary**: {coder_output.get('summary', '-')}

**Files**:
{files_md or '(none — read-only mode)'}

**Test**: {test_result[:400] if test_result else '(no tests run)'}

**Reviewer Notes**: {verdict.get('catatan', '-') if verdict else '-'}
"""
        return True, status_dev_content, result_md

    # ──────────────────────────────────────────────────────────
    # MAIN PIPE
    # ──────────────────────────────────────────────────────────
    async def pipe(
        self,
        body: Dict[str, Any],
        __user__: Optional[Dict[str, Any]] = None,
        __request__: Optional[Request] = None,
        __event_emitter__: Optional[Callable] = None,
        __event_call__: Optional[Callable] = None,
    ) -> AsyncGenerator[str, None] | str:

        emit = __event_emitter__
        self._tokens = {}
        self._env_health = None          # v5.5 [T5]: Reset per invocation
        self._task_failure_log = {}      # v5.5 [T6]: Reset per invocation

        self._refresh_dirs()

        await self._status(emit, "🚀 Pipeline v5.5 starting...")

        # ── Parse user message ───────────────────────────────────
        messages = body.get("messages", [])
        user_msg = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                c = m.get("content", "")
                user_msg = (
                    next((i.get("text", "") for i in c if isinstance(i, dict) and i.get("type") == "text"), "")
                    if isinstance(c, list) else str(c)
                )
                break

        if not user_msg:
            return "❌ No user message found."

        cmd = self._detect_command(user_msg)
        if cmd:
            cmd_type, phase_prefix = cmd

            await self._status(emit, "📖 Loading knowledge files...")
            knowledge = await self._load_knowledge(emit)
            if knowledge is None:
                return "❌ Failed to load knowledge files. Check KNOWLEDGE_PATH valve."

            status_dev_content = ""
            for fname, content in knowledge.items():
                if "status" in fname.lower() or "progress" in fname.lower():
                    status_dev_content = content
                    break
            all_tasks = self._parse_tasks(status_dev_content)

            if cmd_type == "quick_check":
                await self._status(emit, f"📊 Quick check phase {phase_prefix}...")
                report = self._phase_quick_check(phase_prefix, all_tasks)
                await self._status(emit, "✅ Done", done=True)
                await self._replace(emit, report)
                return report

            elif cmd_type == "deep_check":
                await self._status(emit, f"🔬 Deep check phase {phase_prefix}...")
                # v5.5 [T5]: Run env health as part of deep check
                if self.valves.ENABLE_ENV_CHECK:
                    await self._status(emit, "🏥 Running environment health check...")
                    env_h = await self._check_env_health()
                    if not env_h["ok"]:
                        for iss in env_h["issues"]:
                            await self._status(emit, f"⚠️ {iss}")
                    for w in env_h.get("warnings", []):
                        await self._status(emit, f"ℹ️ {w}")
                report, problem_ids = await self._phase_deep_check(phase_prefix, all_tasks, emit, __event_call__)
                await self._status(emit, "✅ Deep check done", done=True)
                await self._replace(emit, report)
                return report

            elif cmd_type == "fix_phase":
                await self._status(emit, f"🔧 Resolving failed tasks for phase {phase_prefix}...")
                failed_ids = self._get_failed_task_ids(phase_prefix, all_tasks)
                if not failed_ids:
                    msg = (
                        f"## ✅ Phase {phase_prefix} — Tidak Ada yang Perlu Di-fix\n\n"
                        f"Semua task di phase {phase_prefix} statusnya OK di tabel.\n\n"
                        f"Kalau mau verifikasi file aslinya: `check phase {phase_prefix}`"
                    )
                    await self._status(emit, "✅ Nothing to fix", done=True)
                    await self._replace(emit, msg)
                    return msg
                await self._status(emit, f"📋 Found {len(failed_ids)} task(s) to retry: {', '.join(failed_ids)}")
                user_msg = f"kerjakan {', '.join(failed_ids)}"

        # ── Parse task IDs (batch support) ───────────────────────
        task_ids = self._parse_task_ids(user_msg)
        if not task_ids:
            return (
                "❌ No task ID found.\n\n"
                "**Supported formats:**\n"
                "- `kerjakan P0-005`\n"
                "- `kerjakan P0-004 sampai P0-006`\n"
                "- `kerjakan P0-004, P0-005, P0-006`\n"
                "- `kerjakan SPRINT-3-007`\n"
                "- `kerjakan P10-001 sampai P10-010`\n\n"
                "**Phase commands (prompt bebas):**\n"
                "- `cek phase P0` / `status P0` / `lihat fase P0`\n"
                "- `check phase P0` / `periksa phase P0` / `verifikasi fase P0`\n"
                "- `fix phase P0` / `perbaiki phase P0` / `ulangi fase P0`\n"
            )

        await self._status(emit, f"📋 Tasks: {', '.join(task_ids)} ({len(task_ids)} task(s))")

        if "knowledge" not in locals() or knowledge is None:
            await self._status(emit, "📖 Loading knowledge files...")
            knowledge = await self._load_knowledge(emit)
            if knowledge is None:
                return "❌ Failed to load knowledge files. Check KNOWLEDGE_PATH valve."

        status_dev_content = ""
        for fname, content in knowledge.items():
            if "status" in fname.lower() or "progress" in fname.lower():
                status_dev_content = content
                break

        all_tasks_parsed = self._parse_tasks(status_dev_content)
        task_map: Dict[str, Dict] = {t["id"]: t for t in all_tasks_parsed}

        batch_tasks: List[Dict[str, str]] = []
        for tid in task_ids:
            info: Dict[str, str] = {"id": tid, "name": tid, "description": user_msg}
            if tid in task_map:
                info.update(task_map[tid])
                if not info.get("description") or info["description"] == user_msg:
                    info["description"] = info.get("name", tid)
            batch_tasks.append(info)

        await self._status(emit, "🗜️ Compressing context...")
        compressed = self._compress_knowledge(knowledge, task_ids[0])

        await self._status(emit, "🔍 Scanning project files...")
        batch_desc = " ".join(t.get("description", t.get("name", "")) for t in batch_tasks)
        initial_files = await self._scan_files(batch_desc)
        await self._status(emit, f"📁 Found {len(initial_files)} relevant file(s)")

        await self._status(emit, f"🧭 Planning batch of {len(batch_tasks)} task(s)...")
        plan_prompt = self._prompt_plan_batch(batch_tasks, compressed, initial_files)
        plan_response = await self._llm(
            __request__, self.valves.PLANNER_MODEL,
            [{"role": "user", "content": plan_prompt}],
            user_obj=__user__, track_key="planner",
        )

        plans: List[Dict] = []
        try:
            arr_match = re.search(r"\[[\s\S]*\]", plan_response)
            if arr_match:
                plans = json.loads(arr_match.group(0))
            else:
                plans = [self._extract_json(plan_response)]
        except Exception:
            plans = []

        plan_map: Dict[str, Dict] = {}
        for p in plans:
            if isinstance(p, dict) and p.get("task_id"):
                plan_map[p["task_id"]] = p

        for task_info in batch_tasks:
            tid = task_info["id"]
            if tid not in plan_map:
                # FIXED: [Lapis 4 Bug 2 - Fallback complexity raised to 10]
                plan_map[tid] = {
                    "approach": "Implement the task as described",
                    "files_to_create": [],
                    "files_to_modify": [],
                    "key_imports": [],
                    "test_approach": "Write unit tests",
                    "risks": [],
                    "complexity_score": 10,
                    "depends_on_previous": False,
                }

        for task_info in batch_tasks:
            tid = task_info["id"]
            p = plan_map[tid]
            score = p.get("complexity_score", 10)
            gate = "skip reviewer" if score < self.valves.REVIEWER_THRESHOLD else "call reviewer"
            await self._status(emit, f"  📐 {tid}: score={score} → {gate}")

        results: List[str] = []
        succeeded = 0
        failed_ids: List[str] = []

        for idx, task_info in enumerate(batch_tasks, 1):
            tid = task_info["id"]
            plan = plan_map[tid]
            
            # FIXED: [Gemini Audit Bug 2 - Use separate snapshot for per-task budget check]
            # self._tokens is cumulative (for final report). We snapshot it before each task
            # and check the DELTA against MAX_TOKEN_BUDGET inside _execute_task instead.
            _tokens_before = dict(self._tokens)

            await self._status(emit, f"▶️ Starting task {idx}/{len(batch_tasks)}: {tid}")

            # FIXED: [Lapis 3 Bug 1 - Context isolation re-compressed always]
            task_compressed = self._compress_knowledge(knowledge, tid)

            success, status_dev_content, result_md = await self._execute_task(
                task_id=tid,
                task_info=task_info,
                plan=plan,
                knowledge=knowledge,
                status_dev_content=status_dev_content,
                compressed=task_compressed,
                __request__=__request__,
                __user__=__user__,
                __event_call__=__event_call__,
                emit=emit,
                task_num=idx,
                total_tasks=len(batch_tasks),
                tokens_before=_tokens_before,  # FIXED: [Gemini Audit Bug 2]
            )

            results.append(result_md)

            if success:
                succeeded += 1
            else:
                failed_ids.append(tid)
                if len(batch_tasks) > 1 and idx < len(batch_tasks):
                    if self.valves.BATCH_STOP_ON_FAIL:
                        await self._status(emit, f"🛑 Batch stopped (BATCH_STOP_ON_FAIL=True)")
                        break
                    else:
                        remaining = [t["id"] for t in batch_tasks[idx:]]
                        proceed = await self._confirm(
                            __event_call__,
                            f"Task {tid} failed. Continue with remaining tasks: {', '.join(remaining)}?"
                        )
                        if not proceed:
                            await self._status(emit, "🛑 Batch cancelled by user")
                            break

        await self._status(emit, f"✅ Batch complete: {succeeded}/{len(batch_tasks)} succeeded", done=True)

        batch_header = f"""## Pipeline v5.5 — Batch Complete

**Tasks**: {', '.join(task_ids)}
**Result**: {succeeded}/{len(batch_tasks)} succeeded
{f"**Failed**: {', '.join(failed_ids)}" if failed_ids else "**All tasks passed** ✅"}

---

"""
        full_result = batch_header + "\n\n---\n\n".join(results) + f"\n\n{self._token_report()}"
        await self._replace(emit, full_result)
        return full_result
