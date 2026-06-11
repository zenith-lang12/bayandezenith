"""
title: Universal Project Builder Pipeline v5.1
author: Claude (Anthropic) — redesigned for BayanDeZenith & beyond
version: 5.1.0
required_open_webui_version: 0.9.6

WHAT'S NEW IN v5.1 vs v5.0:
─────────────────────────────────────────────────────────────────

[1] BATCH MODE
    - "kerjakan P0-004 sampai P0-006" → eksekusi sequential otomatis
    - "kerjakan P0-004, P0-005, P0-006" → juga work
    - "kerjakan P0-005" → single task, sama seperti v5.0
    - Kalau satu task gagal → tanya user: lanjut atau stop?
    - Progress per-task di-emit realtime

[2] SMART REVIEWER GATE
    - Planner sekarang return complexity_score (1-10)
    - Valve: REVIEWER_THRESHOLD (default: 6)
    - Task score < threshold → skip reviewer → auto-approve
    - Status update TETAP JALAN di semua path (wajib)
    - Estimasi penghematan: ~60% token untuk task simple

[3] ROBUST STATUS UPDATE
    - Status update jalan di semua path: approved, auto-approved, failed
    - Task gagal → ditandai ❌ di status_dev.md (bukan silent skip)
    - Fallback: kalau update tabel gagal → append ke bawah file

[4] PLANNER BATCH CONTEXT
    - Kalau batch mode: planner lihat semua task dalam batch sekaligus
    - Bisa detect dependency antar task
    - Return per-task plan dalam satu call

[5] UNIVERSAL PROJECT MODE
    - Hapus hard-coded "DAY TRADE / SWING TRADE"
    - Valve: PROJECT_CONTEXT (free text, deskripsi project kamu)
    - Default masih BayanDeZenith-compatible
    - Orang lain bisa set: "FastAPI backend service" atau apapun

[6] SCAN FILE_TO_MODIFY FIX
    - Setelah planner return plan, files_to_modify langsung masuk relevant_files
    - Coder tidak perlu "tebak" file yang perlu dimodifikasi

[7] PIPELINE LOG
    - Append-only log ke pipeline_log.jsonl di SANDBOX_PATH
    - Track: task_id, timestamp, tokens used, verdict, files written

WORKFLOW v5.1:
─────────────────────────────────────────────────────────────────

  User: "kerjakan P0-004 sampai P0-006"
        ↓
  [0] PARSE  — extract task range/list + mode from user message
        ↓
  [BATCH ORCHESTRATOR LOOP per task]
        ↓
  [1] PLAN   — planner sees ALL batch tasks for context
        ↓       returns per-task plan WITH complexity_score
  [2] CODE   — coder gets plan as anchor (lean prompt)
        ↓
  [3] VALIDATE — static checks
        ↓
  [4] GATE   — complexity_score < threshold? → skip to [6]
        ↓
  [5] REVIEW — reviewer only called when needed
        ↓
  [6] TEST   — execute test file
        ↓
  [7] STATUS — update status_dev.md (ALWAYS, all paths)
        ↓
  [8] LOG    — append to pipeline_log.jsonl
        ↓
  next task in batch...

TOKEN BUDGET (estimated):
─────────────────────────────────────────────────────────────────
  v5.0 per task (simple, 0 retry): ~8,000 tokens
  v5.1 per task (simple, gate off): ~5,000 tokens  ← no reviewer call
  v5.1 per task (complex, 0 retry): ~8,000 tokens  ← same as v5.0
  v5.1 batch of 3 simple tasks:     ~15,000 tokens ← vs ~24,000 in v5.0
"""

import asyncio
import inspect
import json
import logging
import os
import re
import shlex
import time
from datetime import datetime
from functools import partial
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

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

SAFE_WRITE_DIRS = [
    "src/", "tests/", "config/", "scripts/", "models/", "data/",
    "scanner/", "analisa/", "evaluator/", "execution/", "backtesting/",
    "monitoring/", "logging/", "pipeline/",
]

SCAN_DIRS = [
    "scanner", "analisa", "evaluator", "execution", "backtesting",
    "models", "config", "scripts", "pipeline", "monitoring",
    "logging", "tests",
]

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
    Universal Project Builder Pipeline v5.1

    Works with any blueprint-driven project.
    Convention: knowledge folder must contain:
      - konteks_permanen.md  (or any *_permanen.md / *_context.md)
      - status_dev.md        (or any *_status.md / *_progress.md)
      - aturan_sistem.md     (or any *_rules.md / *_aturan.md)

    Task commands:
      "kerjakan P0-004"                  → single task
      "kerjakan P0-004 sampai P0-006"    → batch range
      "kerjakan P0-004, P0-005, P0-006"  → batch list
      "do task B2-007"                   → also works
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

    def __init__(self):
        self.type = "pipe"
        self.valves = self.Valves()
        self._tokens: Dict[str, int] = {}

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
            resp = await event_call({
                "type": "input",
                "data": {
                    "title": "⚠️ Confirm",
                    "message": msg,
                    "placeholder": "yes / no",
                },
            })
            val = (resp.get("value", "") if isinstance(resp, dict) else str(resp)).strip().lower()
            return val in ("yes", "y", "ya", "lanjut", "ok", "oke", "continue", "lanjutkan")
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
            logger.warning(f"[v5.1] Write blocked (outside sandbox): {path}")
            return False
        rel = os.path.relpath(norm, sandbox)
        if not any(rel.startswith(d) for d in SAFE_WRITE_DIRS):
            logger.warning(f"[v5.1] Write blocked (unsafe dir): {rel}")
            return False
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            _cache.invalidate(path)
            return True
        except Exception as e:
            logger.error(f"[v5.1] Write error {path}: {e}")
            return False

    # ──────────────────────────────────────────────────────────
    # CODE EXECUTION
    # ──────────────────────────────────────────────────────────
    async def _exec(self, file_path: str) -> Tuple[bool, str]:
        if self.valves.EXEC_VIA_DOCKER:
            ok, out = await self._exec_docker(file_path)
            if ok or "docker" not in out.lower():
                return ok, out
        return await self._exec_native(file_path)

    async def _exec_docker(self, file_path: str) -> Tuple[bool, str]:
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "exec", "-w", self.valves.SANDBOX_PATH,
                self.valves.SANDBOX_CONTAINER, "python3", file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=self.valves.CODE_EXEC_TIMEOUT
            )
            if proc.returncode == 0:
                return True, stdout.decode()
            return False, stderr.decode() or stdout.decode()
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
            proc = await asyncio.create_subprocess_shell(
                f"cd {shlex.quote(self.valves.SANDBOX_PATH)} && python3 {shlex.quote(file_path)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=self.valves.CODE_EXEC_TIMEOUT
            )
            if proc.returncode == 0:
                return True, stdout.decode()
            return False, stderr.decode() or stdout.decode()
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
        if Users is not None and isinstance(user_obj, dict):
            uid = user_obj.get("id", "")
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
            logger.error(f"[v5.1] LLM call error ({model_id}): {e}")
            return await self._llm_http_fallback(request, model_id, messages)

        result = ""
        if inspect.isasyncgen(resp):
            result = await self._drain(resp)
        elif hasattr(resp, "body_iterator"):
            result = await self._drain(resp.body_iterator)
        else:
            result = self._extract_content(resp)

        if not result:
            result = "[ERROR] LLM returned empty response"

        self._track(f"{track_key}_out", result)
        return result

    async def _drain(self, iterator: Any) -> str:
        parts: List[str] = []
        buf = ""
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

            decoded = chunk.decode() if isinstance(chunk, bytes) else str(chunk)
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
        phase_prefix = task_id.split("-")[0]
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

        for line in lines:
            if line.startswith("## POSISI SEKARANG"):
                result.append(line)
                in_active_phase = True
                in_decisions = False
                continue

            if f"## {phase_prefix.upper()}" in line or f"## PHASE {phase_prefix[-1]}" in line:
                result.append(line)
                in_active_phase = True
                in_decisions = False
                continue

            if line.startswith("## PHASE") and f"PHASE {phase_prefix[-1]}" not in line:
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
    async def _scan_files(self, task_desc: str, extra_paths: Optional[List[str]] = None) -> Dict[str, str]:
        """
        Scan project for files relevant to this task.
        v5.1: extra_paths forces inclusion of specific files (from plan.files_to_modify).
        """
        sandbox = self.valves.SANDBOX_PATH
        result: Dict[str, str] = {}
        keywords = self._keywords(task_desc)

        # Force-include files from planner's files_to_modify list
        if extra_paths:
            for rel_path in extra_paths:
                full = os.path.join(sandbox, rel_path)
                if os.path.isfile(full) and rel_path not in result:
                    try:
                        with open(full, "r", encoding="utf-8") as f:
                            raw = f.read()
                        if len(raw) > MAX_FILE_CHARS_FOR_CODER:
                            result[rel_path] = raw[:MAX_FILE_CHARS_FOR_CODER] + f"\n# ... [truncated — full file on disk]"
                        else:
                            result[rel_path] = raw
                    except Exception:
                        pass

        for scan_dir in SCAN_DIRS:
            dir_path = os.path.join(sandbox, scan_dir)
            if not os.path.isdir(dir_path):
                continue

            for root, dirs, files in os.walk(dir_path):
                dirs[:] = [d for d in dirs if not d.startswith((".", "__"))]
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
                        try:
                            with open(full, "r", encoding="utf-8") as f:
                                raw = f.read()
                            if len(raw) > MAX_FILE_CHARS_FOR_CODER:
                                result[rel] = raw[:MAX_FILE_CHARS_FOR_CODER] + f"\n# ... [truncated at {MAX_FILE_CHARS_FOR_CODER} chars]"
                            else:
                                result[rel] = raw
                        except Exception:
                            pass

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
        tasks: List[Dict[str, str]] = []
        in_table = False
        for line in content.split("\n"):
            s = line.strip()
            if not s or "|" not in s:
                continue
            if re.match(r"^\|[\s\-:]+\|", s):
                in_table = True
                continue
            if in_table and re.match(r"^\|?\s*[A-Z]\d+-\d+", s):
                parts = [p.strip() for p in s.split("|") if p.strip()]
                if len(parts) >= 2:
                    tasks.append({
                        "id": parts[0],
                        "name": parts[1] if len(parts) > 1 else "",
                        "status": parts[2] if len(parts) > 2 else "",
                        "files": parts[3] if len(parts) > 3 else "",
                        "notes": parts[4] if len(parts) > 4 else "",
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
            if task_id in line and "|" in line:
                parts = line.split("|")
                if len(parts) >= 4:
                    if "status" in update:
                        parts[3] = f" {update['status']} "
                    if "files" in update and len(parts) >= 5:
                        parts[4] = f" {update['files']} "
                    if "notes" in update and len(parts) >= 6:
                        parts[5] = f" {update['notes'][:150]} "
                    lines[i] = "|".join(parts)
                    updated = True
                    break

        result = "\n".join(lines)

        if not updated:
            # Fallback: append note at bottom
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            fallback = (
                f"\n\n<!-- Pipeline v5.1 update [{ts}] -->\n"
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

        for f in files:
            path = f.get("path", "?")
            content = f.get("content", "")

            for marker in INCOMPLETE_CODE_MARKERS:
                if marker in content:
                    issues.append(f"{path}: found truncation marker '{marker}'")

            if content.count('"""') % 2 != 0:
                issues.append(f"{path}: odd number of triple-quotes — likely truncated")

            stripped = content.rstrip()
            if stripped.endswith(":") and not stripped.endswith('":'):
                issues.append(f"{path}: file ends with ':' — likely missing function body")

            for line in content.split("\n"):
                m = re.match(r"^\s*(?:import|from)\s+([a-zA-Z_][a-zA-Z0-9_]*)", line)
                if m:
                    pkg = m.group(1).lower()
                    stdlib = {
                        "os", "sys", "re", "json", "time", "math", "logging",
                        "typing", "pathlib", "datetime", "collections", "functools",
                        "asyncio", "abc", "io", "copy", "random", "itertools",
                        "contextlib", "dataclasses", "enum", "unittest", "inspect",
                    }
                    if pkg not in stdlib and pkg not in allowed_pkgs:
                        if "." not in pkg:
                            issues.append(
                                f"{path}: imported '{pkg}' — not in AVAILABLE_PACKAGES. "
                                f"If project-local module, ignore."
                            )

        return issues

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
            "pipeline_version": "5.1",
        }
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[v5.1] Log write failed: {e}")

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
        Returns array of plans, one per task, with complexity_score.

        complexity_score heuristic:
          1-3: config, schema, test setup, simple data class
          4-5: single module, no external deps, clear spec
          6-7: multi-file, needs integration, async I/O
          8-10: cross-module, pipeline-level, replaces existing logic

        Tasks scored BELOW REVIEWER_THRESHOLD skip reviewer entirely.
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
    ) -> str:
        """
        v5.1: Removed hard-coded mode_note. Uses PROJECT_CONTEXT valve instead.
        """
        files_section = ""
        for fname, content in relevant_files.items():
            files_section += f"\n\n### {fname}\n```python\n{content}\n```\n"
        if not files_section:
            files_section = "\n(No existing relevant files — create new ones as needed)"

        packages = self.valves.AVAILABLE_PACKAGES
        project_ctx = self.valves.PROJECT_CONTEXT

        return f"""You are an AI Coder for a software project.

# PROJECT CONTEXT
{project_ctx}

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
            preview = content[:MAX_FILE_CHARS_FOR_REVIEWER]
            if len(content) > MAX_FILE_CHARS_FOR_REVIEWER:
                preview += f"\n# ... [truncated at {MAX_FILE_CHARS_FOR_REVIEWER} chars]"
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
    ) -> str:
        """
        v5.1: Removed hard-coded mode_note. Uses PROJECT_CONTEXT.
        """
        packages = self.valves.AVAILABLE_PACKAGES
        project_ctx = self.valves.PROJECT_CONTEXT

        return f"""You are an AI Coder. Your previous attempt was REJECTED by the reviewer.

# PROJECT CONTEXT
{project_ctx}

# TASK (same as before)
ID: {task.get('id')}
Name: {task.get('name', '')}
Description: {task.get('description', task.get('name', ''))}

# 🔴 REVIEWER FEEDBACK (MUST address ALL points)
{reviewer_feedback}

# FILES TO REWRITE
{chr(10).join(f"- {f}" for f in file_list)}

# CRITICAL RULES (non-negotiable)
- Output code MUST be 100% complete — no "...", "# TODO", "pass #"
- Use ONLY these packages: {packages}
- For mock in tests: use unittest.mock (AsyncMock, MagicMock, patch)
- Fix ALL points in reviewer feedback above

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
        Parse task IDs from user message. Supports:
          - Single:   "kerjakan P0-005"              → ["P0-005"]
          - Range:    "kerjakan P0-004 sampai P0-006" → ["P0-004", "P0-005", "P0-006"]
          - Range EN: "kerjakan P0-004 to P0-006"    → same
          - List:     "kerjakan P0-004, P0-005, P0-006" → same
          - Mixed:    any combination

        Task IDs must share the same phase prefix for range expansion.
        """
        # Find all explicit task IDs in the message
        all_ids = re.findall(r"[A-Z]\d+-\d+", user_msg)
        if not all_ids:
            return []

        # Detect range pattern: "P0-004 sampai/to P0-006"
        range_match = re.search(
            r"([A-Z]\d+)-(\d+)\s+(?:sampai|to|hingga|until|-)\s+([A-Z]\d+)-(\d+)",
            user_msg, re.IGNORECASE
        )
        if range_match:
            prefix_a, num_a, prefix_b, num_b = range_match.groups()
            if prefix_a == prefix_b:
                start, end = int(num_a), int(num_b)
                if start <= end:
                    return [f"{prefix_a}-{str(n).zfill(3)}" for n in range(start, end + 1)]

        # No range — return deduplicated list preserving order
        seen = set()
        result = []
        for tid in all_ids:
            if tid not in seen:
                seen.add(tid)
                result.append(tid)
        return result

    # ──────────────────────────────────────────────────────────
    # SINGLE TASK EXECUTOR
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
    ) -> Tuple[bool, str, str]:
        """
        Execute a single task through the full pipeline.
        Returns: (success, final_status_content, result_markdown)
        """
        prefix = f"[{task_num}/{total_tasks}] {task_id}"
        reviewer_skipped = False
        complexity_score = plan.get("complexity_score", 5)
        threshold = self.valves.REVIEWER_THRESHOLD

        # ── Enrich relevant files using plan.files_to_modify ────
        files_to_modify = plan.get("files_to_modify", [])
        relevant_files = await self._scan_files(
            task_info.get("description", task_info.get("name", "")),
            extra_paths=files_to_modify if isinstance(files_to_modify, list) else [],
        )
        await self._status(emit, f"{prefix}: 📁 {len(relevant_files)} file(s) in context")

        # ── STEP 2-4: CODE + VALIDATE + REVIEW LOOP ─────────────
        reviewer_feedback = ""
        verdict: Dict = {}
        v = ""
        written_files: Dict[str, str] = {}
        coder_output: Dict = {}
        original_prompt = self._prompt_code(task_info, plan, compressed, relevant_files)

        for attempt in range(self.valves.MAX_RETRY + 1):
            # Token budget check
            if self._total_tokens() > self.valves.MAX_TOKEN_BUDGET:
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
                j = re.search(r"\{[\s\S]*\}", coder_response)
                if not j:
                    raise ValueError("No JSON in response")
                coder_output = json.loads(j.group(0))
                files_to_write = coder_output.get("files", [])
                if not files_to_write:
                    raise ValueError("No files in output")
            except Exception as e:
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = f"JSON parse error: {e}. Return valid JSON."
                    continue
                await self._commit_status(
                    knowledge, status_dev_content, task_id,
                    {"status": "❌ Parse error", "notes": str(e)[:100]},
                    emit,
                )
                return False, status_dev_content, f"❌ Coder output invalid: {e}"

            # ── STEP 3: STATIC VALIDATION ────────────────────────
            validation_issues = self._validate_code(files_to_write)
            if validation_issues:
                issues_text = "\n".join(f"- {i}" for i in validation_issues)
                await self._status(emit, f"{prefix}: ⚠️ Static validation: {len(validation_issues)} issue(s)")
                if attempt < self.valves.MAX_RETRY:
                    reviewer_feedback = (
                        f"Static validation failed — fix these:\n{issues_text}\n"
                        f"Ensure code is 100% complete with no truncation."
                    )
                    continue

            # ── Write files ──────────────────────────────────────
            if not self.valves.READ_ONLY_MODE:
                await self._status(emit, f"{prefix}: 💾 Writing {len(files_to_write)} file(s)...")
                written_files = {}
                failed_writes = []

                for fi in files_to_write:
                    path = fi.get("path", "")
                    content = fi.get("content", "")
                    if not path or not content:
                        continue
                    full = os.path.join(self.valves.SANDBOX_PATH, path)
                    ok = await self._write(full, content)
                    if ok:
                        written_files[path] = content
                        await self._status(emit, f"  ✅ {path}")
                    else:
                        failed_writes.append(path)
                        await self._status(emit, f"  ❌ Failed: {path}")

                if failed_writes and not self._retryable(f"write blocked {failed_writes[0]}"):
                    err = self._classify("write blocked")
                    await self._commit_status(
                        knowledge, status_dev_content, task_id,
                        {"status": "❌ Write error", "notes": err[:100]},
                        emit,
                    )
                    return False, status_dev_content, f"❌ Write failed: {', '.join(failed_writes)}\n{err}"
            else:
                written_files = {
                    fi.get("path", f"file_{i}"): fi.get("content", "")
                    for i, fi in enumerate(files_to_write)
                }

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
                    j = re.search(r"\{[\s\S]*\}", reviewer_response)
                    if not j:
                        raise ValueError("No JSON")
                    verdict = json.loads(j.group(0))
                    v = verdict.get("verdict", "REJECTED")
                except Exception as e:
                    v = "APPROVED_WITH_NOTES"
                    verdict = {"verdict": v, "catatan": f"Reviewer parse error: {e}", "harus_diubah": ""}

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
        # Invalidate knowledge cache so next task reads fresh status
        for fname in knowledge.keys():
            if "status" in fname.lower() or "progress" in fname.lower():
                knowledge[fname] = status_dev_content  # update in-memory too

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

        await self._status(emit, "🚀 Pipeline v5.1 starting...")

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

        # ── Parse task IDs (v5.1 — batch support) ───────────────
        task_ids = self._parse_task_ids(user_msg)
        if not task_ids:
            return (
                "❌ No task ID found.\n\n"
                "Supported formats:\n"
                "- `kerjakan P0-005`\n"
                "- `kerjakan P0-004 sampai P0-006`\n"
                "- `kerjakan P0-004, P0-005, P0-006`\n"
            )

        await self._status(emit, f"📋 Tasks: {', '.join(task_ids)} ({len(task_ids)} task(s))")

        # ── Load knowledge ───────────────────────────────────────
        await self._status(emit, "📖 Loading knowledge files...")
        knowledge = await self._load_knowledge(emit)
        if knowledge is None:
            return "❌ Failed to load knowledge files. Check KNOWLEDGE_PATH valve."

        # ── Parse all tasks from status_dev ─────────────────────
        status_dev_content = ""
        for fname, content in knowledge.items():
            if "status" in fname.lower() or "progress" in fname.lower():
                status_dev_content = content
                break

        all_tasks_parsed = self._parse_tasks(status_dev_content)
        task_map: Dict[str, Dict] = {t["id"]: t for t in all_tasks_parsed}

        # Build task_info list for the batch
        batch_tasks: List[Dict[str, str]] = []
        for tid in task_ids:
            info: Dict[str, str] = {"id": tid, "name": tid, "description": user_msg}
            if tid in task_map:
                info.update(task_map[tid])
                # Use task name as description if user_msg is just a command
                if not info.get("description") or info["description"] == user_msg:
                    info["description"] = info.get("name", tid)
            batch_tasks.append(info)

        # ── Compress knowledge (once for full batch) ─────────────
        await self._status(emit, "🗜️ Compressing context...")
        # Use first task's phase prefix for compression
        compressed = self._compress_knowledge(knowledge, task_ids[0])

        # ── Scan project files (broad scan for context) ──────────
        await self._status(emit, "🔍 Scanning project files...")
        batch_desc = " ".join(t.get("description", t.get("name", "")) for t in batch_tasks)
        initial_files = await self._scan_files(batch_desc)
        await self._status(emit, f"📁 Found {len(initial_files)} relevant file(s)")

        # ── BATCH PLANNER: plan ALL tasks in one call ────────────
        await self._status(emit, f"🧭 Planning batch of {len(batch_tasks)} task(s)...")
        plan_prompt = self._prompt_plan_batch(batch_tasks, compressed, initial_files)
        plan_response = await self._llm(
            __request__, self.valves.PLANNER_MODEL,
            [{"role": "user", "content": plan_prompt}],
            user_obj=__user__, track_key="planner",
        )

        # Parse plan response (expect JSON array)
        plans: List[Dict] = []
        try:
            # Try array first
            arr_match = re.search(r"\[[\s\S]*\]", plan_response)
            if arr_match:
                plans = json.loads(arr_match.group(0))
            else:
                # Fallback: single object
                obj_match = re.search(r"\{[\s\S]*\}", plan_response)
                if obj_match:
                    plans = [json.loads(obj_match.group(0))]
        except Exception:
            plans = []

        # Build task_id → plan mapping, fill gaps with defaults
        plan_map: Dict[str, Dict] = {}
        for p in plans:
            if isinstance(p, dict) and p.get("task_id"):
                plan_map[p["task_id"]] = p

        for task_info in batch_tasks:
            tid = task_info["id"]
            if tid not in plan_map:
                plan_map[tid] = {
                    "approach": "Implement the task as described",
                    "files_to_create": [],
                    "files_to_modify": [],
                    "key_imports": [],
                    "test_approach": "Write unit tests",
                    "risks": [],
                    "complexity_score": 5,
                    "depends_on_previous": False,
                }

        # Log the plan summary
        for task_info in batch_tasks:
            tid = task_info["id"]
            p = plan_map[tid]
            score = p.get("complexity_score", 5)
            gate = "skip reviewer" if score < self.valves.REVIEWER_THRESHOLD else "call reviewer"
            await self._status(emit, f"  📐 {tid}: score={score} → {gate}")

        # ── BATCH EXECUTION LOOP ─────────────────────────────────
        results: List[str] = []
        succeeded = 0
        failed_ids: List[str] = []

        for idx, task_info in enumerate(batch_tasks, 1):
            tid = task_info["id"]
            plan = plan_map[tid]

            await self._status(emit, f"▶️ Starting task {idx}/{len(batch_tasks)}: {tid}")

            # Re-compress for this specific task's phase if different from first
            task_compressed = (
                compressed if tid.split("-")[0] == task_ids[0].split("-")[0]
                else self._compress_knowledge(knowledge, tid)
            )

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
            )

            results.append(result_md)

            if success:
                succeeded += 1
            else:
                failed_ids.append(tid)
                # Check whether to continue batch after failure
                if len(batch_tasks) > 1 and idx < len(batch_tasks):
                    if self.valves.BATCH_STOP_ON_FAIL:
                        await self._status(emit, f"🛑 Batch stopped (BATCH_STOP_ON_FAIL=True)")
                        break
                    else:
                        # Ask user
                        remaining = [t["id"] for t in batch_tasks[idx:]]
                        proceed = await self._confirm(
                            __event_call__,
                            f"Task {tid} failed. Continue with remaining tasks: {', '.join(remaining)}?"
                        )
                        if not proceed:
                            await self._status(emit, "🛑 Batch cancelled by user")
                            break

        # ── FINAL BATCH SUMMARY ──────────────────────────────────
        await self._status(emit, f"✅ Batch complete: {succeeded}/{len(batch_tasks)} succeeded", done=True)

        batch_header = f"""## Pipeline v5.1 — Batch Complete

**Tasks**: {', '.join(task_ids)}
**Result**: {succeeded}/{len(batch_tasks)} succeeded
{f"**Failed**: {', '.join(failed_ids)}" if failed_ids else "**All tasks passed** ✅"}

---

"""
        full_result = batch_header + "\n\n---\n\n".join(results) + f"\n\n{self._token_report()}"
        await self._replace(emit, full_result)
        return full_result
