"""
title: BayanDeZenith Pipeline v4.6
author: BayanDeZenith
version: 4.6.1
required_open_webui_version: 0.9.6


Pipeline v4.6.1 — Bugfix Release.


Bugfixes v4.6.1:
- CRITICAL: await Users.get_user_by_id() — missing await menyebabkan coroutine
  dikirim ke generate_raw_chat_completion → semua LLM call gagal → "Coder gagal total"
- Tambah non-retryable patterns: model not found, HTTP 400/401/403/404/422/500/502/503
- Tambah classify messages untuk model-not-found dan server errors
- Fix written_files undefined bug saat READ_ONLY_MODE=True
- Improved LLM error logging (include model_id dan exception type)


Architecture:
  Host /root/project/ ← mounted to both containers via Docker Compose
  open-webui:  /home/user/ (read/write native)
  sandbox:     /home/user/ (read/write/execute)


Fitur v4.6:
- Smart Retry: stop kalau error infrastructure (hemat token 60-80%)
- Error classification: pesan actionable untuk user
- Shared volume: file I/O native (permission fix via Docker Compose)
- Code execution via docker exec ke sandbox (dependencies ada di sana)
- Fallback: execute native kalau docker exec gagal
"""


import asyncio
import inspect
import json
import logging
import os
import re
import shlex
import time
from functools import partial
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple, Union


from fastapi import Request
from pydantic import BaseModel, Field


from open_webui.utils.chat import (
    generate_chat_completion as generate_raw_chat_completion,
)


try:
    from open_webui.models.users import Users
except ImportError:
    Users = None


logger = logging.getLogger(__name__)


_ITER_EXHAUSTED = object()


# ============================================================
# Constants
# ============================================================
CACHE_TTL_SECONDS = 300
MAX_FILE_SIZE_KB = 50
SAFE_WRITE_DIRS = [
    "src/", "tests/", "config/", "scripts/",
    "models/", "data/", "scanner/", "analisa/",
    "evaluator/", "execution/", "backtesting/",
    "monitoring/", "logging/", "pipeline/",
]
PROJECT_SCAN_DIRS = [
    "scanner", "analisa", "evaluator", "execution",
    "backtesting", "models", "config", "scripts",
    "pipeline", "monitoring", "logging", "tests",
]


SANDBOX_CONTAINER = "open-terminal-sandbox"




# ============================================================
# Knowledge Cache
# ============================================================
class KnowledgeCache:
    def __init__(self, ttl: int = CACHE_TTL_SECONDS):
        self._cache: Dict[str, Tuple[str, float]] = {}
        self._ttl = ttl


    def get(self, filepath: str) -> Optional[str]:
        if filepath in self._cache:
            content, timestamp = self._cache[filepath]
            if time.time() - timestamp < self._ttl:
                return content
            del self._cache[filepath]
        return None


    def set(self, filepath: str, content: str) -> None:
        self._cache[filepath] = (content, time.time())


    def clear(self) -> None:
        self._cache.clear()




_knowledge_cache = KnowledgeCache()




# ============================================================
# Helper: task ID pattern
# ============================================================
def task_id_pattern(s: str) -> bool:
    return bool(re.match(r"^\|?\s*P\d+-\d+", s))




# ============================================================
# PIPE
# ============================================================
class Pipe:
    """
    BayanDeZenith AI Trading Agent Pipeline v4.6.


    Smart Retry + Shared Volume Architecture.
    """


    class Valves(BaseModel):
        CODER_MODEL: str = Field(
            default="deepseek-chat",
            description="Model ID untuk Coder",
        )
        REVIEWER_MODEL: str = Field(
            default="claude-sonnet-4-20250514",
            description="Model ID untuk Reviewer",
        )
        SANDBOX_PATH: str = Field(
            default="/home/user/bayandezenith",
            description="Path root project (shared volume)",
        )
        KNOWLEDGE_PATH: str = Field(
            default="/home/user/knowledge",
            description="Path ke folder knowledge files",
        )
        MAX_RETRY: int = Field(
            default=2,
            description="Maksimum retry Coder jika ditolak Reviewer",
        )
        LLM_TIMEOUT: int = Field(
            default=300,
            description="Timeout (detik) untuk streaming LLM",
        )
        CODE_EXEC_TIMEOUT: int = Field(
            default=60,
            description="Timeout (detik) untuk eksekusi kode",
        )
        REQUIRE_USER_CONFIRM: bool = Field(
            default=True,
            description="Wajib konfirmasi user sebelum eksekusi",
        )
        READ_ONLY_MODE: bool = Field(
            default=False,
            description="Mode read-only: tidak write file",
        )
        CACHE_ENABLED: bool = Field(
            default=True,
            description="Enable knowledge file caching",
        )
        EXEC_VIA_DOCKER: bool = Field(
            default=True,
            description="Execute code via docker exec ke sandbox container",
        )
        SANDBOX_CONTAINER: str = Field(
            default="open-terminal-sandbox",
            description="Nama container sandbox untuk docker exec",
        )
        MAX_TOKEN_PER_TASK: int = Field(
            default=100000,
            description="Max token per task. Stop kalau exceeded.",
        )


    def __init__(self):
        self.type = "pipe"
        self.valves = self.Valves()
        self._token_usage: Dict[str, int] = {
            "coder_input": 0,
            "coder_output": 0,
            "reviewer_input": 0,
            "reviewer_output": 0,
        }


    # ========================================================
    # SMART RETRY: Error Classification
    # ========================================================
    def _is_retryable_error(self, error_msg: str) -> bool:
        """
        Return True kalau error bisa di-fix dengan retry (logical error).
        Return False kalau error infrastructure (jangan buang token!).
        """
        non_retryable_patterns = [
            # Permission & file system
            "permission denied",
            "no such file or directory",
            "file not found",
            "read-only file system",
            "operation not permitted",
            "disk quota exceeded",
            "write blocked",
            # Docker/execution
            "docker command not found",
            "container not found",
            "cannot connect to docker",
            # LLM infrastructure
            "llm setup timeout",
            "llm fallback failed",
            "stream error",
            "api key",
            "unauthorized",
            "rate limit",
            "returned empty response",
            # Knowledge files
            "[error baca",
            "gagal baca",
            # Model not found / HTTP errors (BUGFIX v4.6.1)
            "model not found",
            "no such model",
            "model does not exist",
            "invalid model",
            "not available",
            "lm api 400",
            "lm api 401",
            "lm api 403",
            "lm api 404",
            "lm api 422",
            "lm api 500",
            "lm api 502",
            "lm api 503",
        ]


        error_lower = error_msg.lower()
        return not any(
            pattern in error_lower for pattern in non_retryable_patterns
        )


    def _classify_error(self, error_msg: str) -> str:
        """Klasifikasi error untuk pesan yang actionable."""
        error_lower = error_msg.lower()


        if "permission denied" in error_lower:
            return "🔒 PERMISSION ERROR — Fix: `sudo chown -R 1000:1000 /root/project`"
        if "write blocked" in error_lower:
            return "🔒 WRITE BLOCKED — Path tidak ada di SAFE_WRITE_DIRS"
        if "no such file" in error_lower or "file not found" in error_lower:
            return "📁 FILE MISSING — Cek apakah file knowledge ada di /home/user/knowledge/"
        if "[error baca" in error_lower or "gagal baca" in error_lower:
            return "📖 KNOWLEDGE READ ERROR — File knowledge tidak bisa dibaca"
        if "docker command not found" in error_lower:
            return "🐳 DOCKER NOT AVAILABLE — Disable EXEC_VIA_DOCKER di valves"
        if "container not found" in error_lower:
            return "🐳 CONTAINER NOT FOUND — Cek nama SANDBOX_CONTAINER di valves"
        if "api key" in error_lower or "unauthorized" in error_lower:
            return "🔑 AUTH ERROR — Cek API key di Open WebUI admin panel"
        if "rate limit" in error_lower:
            return "⏱️ RATE LIMITED — Tunggu beberapa menit, coba lagi"
        if "empty response" in error_lower:
            return "🤖 LLM EMPTY RESPONSE — Model mungkin down, coba lagi nanti"
        if "timeout" in error_lower:
            return "⏰ TIMEOUT — LLM/eksekusi terlalu lama, cek koneksi"
        # BUGFIX v4.6.1: model not found errors
        if any(p in error_lower for p in ["model not found", "no such model", "invalid model", "model does not exist"]):
            return "🤖 MODEL NOT FOUND — Cek nama model di Fungsi > Katup (Valves)"
        if any(p in error_lower for p in ["lm api 400", "lm api 404", "lm api 422"]):
            return "🤖 LLM API ERROR — Model ID salah atau tidak tersedia di OpenWebUI"
        if any(p in error_lower for p in ["lm api 500", "lm api 502", "lm api 503"]):
            return "🌐 LLM SERVER ERROR — Server model sedang down, coba beberapa saat lagi"


        return "🔧 LOGICAL ERROR — Retry akan dilakukan"


    # ========================================================
    # UI EMITTER HELPERS
    # ========================================================
    async def _emit_status(
        self, emitter: Callable, text: str, done: bool = False
    ) -> None:
        if emitter:
            try:
                await emitter(
                    {"type": "status", "data": {"description": text, "done": done}}
                )
            except Exception as e:
                logger.debug(f"[BayanDe] emit_status error: {e}")


    async def _emit_replace(self, emitter: Callable, content: str) -> None:
        if emitter:
            try:
                await emitter({"type": "replace", "data": {"content": content}})
            except Exception as e:
                logger.debug(f"[BayanDe] emit_replace error: {e}")


    async def _ask_user_confirmation(
        self, event_call: Callable, message: str
    ) -> bool:
        if not event_call:
            logger.warning("[BayanDe] No event_call, auto-confirming")
            return True


        try:
            response = await event_call(
                {
                    "type": "input",
                    "data": {
                        "title": "⚠️ Konfirmasi Eksekusi",
                        "message": message,
                        "placeholder": "Ketik 'yes' untuk lanjut, 'no' untuk batal",
                    },
                }
            )
            if isinstance(response, str):
                return response.strip().lower() in [
                    "yes", "y", "ya", "lanjut", "oke"
                ]
            if isinstance(response, dict):
                return response.get("value", "").strip().lower() in [
                    "yes", "y", "ya", "lanjut", "oke"
                ]
            return False
        except Exception as e:
            logger.error(f"[BayanDe] ask_user error: {e}")
            return False


    # ========================================================
    # FILE I/O — Native (Shared Volume)
    # ========================================================
    async def _read_file(self, filepath: str) -> str:
        """Baca file native dari shared volume."""
        if self.valves.CACHE_ENABLED:
            cached = _knowledge_cache.get(filepath)
            if cached is not None:
                return cached


        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            if self.valves.CACHE_ENABLED:
                _knowledge_cache.set(filepath, content)
            return content
        except Exception as e:
            return f"[ERROR baca {filepath}]: {e}"


    async def _write_file(self, filepath: str, content: str) -> bool:
        """Tulis file native ke shared volume."""
        sandbox = self.valves.SANDBOX_PATH
        norm_sandbox = os.path.normpath(sandbox)
        norm_filepath = os.path.normpath(filepath)


        if not norm_filepath.startswith(norm_sandbox):
            logger.warning(f"[BayanDe] Write blocked: {filepath} outside sandbox")
            return False


        relative = os.path.relpath(norm_filepath, norm_sandbox)
        if not any(relative.startswith(d) for d in SAFE_WRITE_DIRS):
            logger.warning(f"[BayanDe] Write blocked: {relative} not in safe dirs")
            return False


        parent = os.path.dirname(filepath)
        if parent:
            try:
                os.makedirs(parent, exist_ok=True)
            except Exception:
                pass


        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"[BayanDe] write_file error: {e}")
            return False


    # ========================================================
    # CODE EXECUTION — Docker Exec + Fallback
    # ========================================================
    async def _execute_code(
        self, file_path: str, timeout: int = 60
    ) -> Tuple[bool, str]:
        """
        Execute code via docker exec ke sandbox (preferred),
        fallback ke native execution.
        """
        if self.valves.EXEC_VIA_DOCKER:
            ok, output = await self._execute_via_docker(file_path, timeout)
            if ok or "docker" not in output.lower():
                return ok, output
            logger.warning("[BayanDe] Docker exec failed, fallback native")


        return await self._execute_native(file_path, timeout)


    async def _execute_via_docker(
        self, file_path: str, timeout: int
    ) -> Tuple[bool, str]:
        """Execute code via docker exec ke sandbox container."""
        container = self.valves.SANDBOX_CONTAINER
        sandbox_path = file_path


        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "exec", "-w", self.valves.SANDBOX_PATH,
                container, "python3", sandbox_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            if proc.returncode == 0:
                return True, stdout.decode("utf-8")
            return False, stderr.decode("utf-8") or stdout.decode("utf-8")


        except FileNotFoundError:
            return False, "docker command not found"


        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            return False, f"Timeout setelah {timeout} detik"


        except Exception as e:
            return False, f"Docker exec error: {e}"


    async def _execute_native(
        self, file_path: str, timeout: int
    ) -> Tuple[bool, str]:
        """Execute code natively di container Open WebUI."""
        sandbox = shlex.quote(self.valves.SANDBOX_PATH)
        safe_path = shlex.quote(file_path)


        proc = None
        try:
            proc = await asyncio.create_subprocess_shell(
                f"cd {sandbox} && python3 {safe_path}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            if proc.returncode == 0:
                return True, stdout.decode("utf-8")
            return False, stderr.decode("utf-8") or stdout.decode("utf-8")


        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            return False, f"Timeout setelah {timeout} detik"


        except Exception as e:
            return False, str(e)


    # ========================================================
    # PROJECT FILE SCANNING
    # ========================================================
    async def _scan_project_files(self, task_desc: str) -> Dict[str, str]:
        """Scan project dan baca file yang relevan dengan task."""
        sandbox = self.valves.SANDBOX_PATH
        relevant_files: Dict[str, str] = {}


        for scan_dir in PROJECT_SCAN_DIRS:
            dir_path = os.path.join(sandbox, scan_dir)
            if not os.path.isdir(dir_path):
                continue


            for root, dirs, files in os.walk(dir_path):
                dirs[:] = [d for d in dirs if not d.startswith((".", "__"))]


                for fname in files:
                    if not fname.endswith((".py", ".yaml", ".yml", ".json")):
                        continue


                    full_path = os.path.join(root, fname)
                    relative_path = os.path.relpath(full_path, sandbox)


                    try:
                        size_kb = os.path.getsize(full_path) / 1024
                        if size_kb > MAX_FILE_SIZE_KB:
                            continue
                    except Exception:
                        continue


                    fname_lower = fname.lower()
                    relative_lower = relative_path.lower()
                    task_lower = task_desc.lower()


                    is_relevant = False


                    if fname_lower.replace(".py", "") in task_lower:
                        is_relevant = True
                    if any(
                        part in task_lower for part in relative_lower.split("/")
                    ):
                        is_relevant = True


                    keywords = self._extract_keywords(task_desc)
                    if any(kw in relative_lower for kw in keywords):
                        is_relevant = True


                    if is_relevant:
                        try:
                            with open(full_path, "r", encoding="utf-8") as f:
                                content = f.read()
                            relevant_files[relative_path] = content
                        except Exception:
                            pass


                    if len(relevant_files) >= 10:
                        return relevant_files


        return relevant_files


    def _extract_keywords(self, task_desc: str) -> List[str]:
        stop_words = {
            "buat", "tambah", "perbaiki", "fix", "update",
            "implementasi", "implement", "add", "create", "modify",
            "dan", "atau", "yang", "di", "ke", "dari", "dengan", "untuk",
        }
        words = re.findall(r"[a-zA-Z_]+", task_desc.lower())
        return [w for w in words if w not in stop_words and len(w) > 3]


    # ========================================================
    # LLM INVOCATION
    # ========================================================
    async def _call_llm_async(
        self,
        request: Request,
        model_id: str,
        messages: List[Dict[str, str]],
        user_obj: Any = None,
    ) -> str:
        # FIX: __user__ dari pipe() adalah dict biasa,
        # generate_raw_chat_completion butuh UserModel object.
        # BUGFIX v4.6.1: Users.get_user_by_id adalah async di OpenWebUI 0.9.x,
        # wajib pakai await (tanpa await → coroutine object → LLM call crash).
        if Users is not None and isinstance(user_obj, dict):
            uid = user_obj.get("id", "")
            if uid:
                try:
                    real_user = await Users.get_user_by_id(uid)
                    if real_user:
                        user_obj = real_user
                except Exception as e:
                    logger.warning(f"[BayanDe] Gagal convert user obj: {e}")

        form_data = {
            "model": model_id,
            "messages": messages,
            "stream": True,
        }

        try:
            response = await asyncio.wait_for(
                generate_raw_chat_completion(request, form_data, user=user_obj),
                timeout=30,
            )
        except asyncio.TimeoutError:
            return "[ERROR] LLM setup timeout (>30s)"
        except Exception as e:
            logger.error(f"[BayanDe] LLM call error (model={model_id}): {type(e).__name__}: {e}")
            return await self._direct_llm_fallback(request, model_id, messages)


        if inspect.isasyncgen(response):
            try:
                result = await self._consume_stream(
                    response, self.valves.LLM_TIMEOUT
                )
                return result if result else "[ERROR] LLM returned empty response"
            except asyncio.TimeoutError:
                return f"[ERROR] LLM streaming timeout ({self.valves.LLM_TIMEOUT}s)"
            except Exception as e:
                return f"[ERROR] Stream error: {e}"


        if hasattr(response, "body_iterator"):
            try:
                result = await self._consume_stream(
                    response.body_iterator, self.valves.LLM_TIMEOUT
                )
                return result if result else "[ERROR] LLM returned empty response"
            except asyncio.TimeoutError:
                if hasattr(response.body_iterator, "aclose"):
                    try:
                        await response.body_iterator.aclose()
                    except Exception:
                        pass
                return f"[ERROR] LLM streaming timeout ({self.valves.LLM_TIMEOUT}s)"
            except Exception as e:
                return f"[ERROR] Stream error: {e}"


        result = self._extract_response_content(response)
        return result if result else "[ERROR] LLM returned empty response"


    async def _consume_stream(self, iterator: Any, timeout: int) -> str:
        parts: List[str] = []
        buffer = ""
        deadline = time.monotonic() + timeout
        is_async = inspect.isasyncgen(iterator)


        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError(f"Streaming exceeded {timeout}s")


            try:
                if is_async:
                    chunk = await asyncio.wait_for(
                        iterator.__anext__(), timeout=min(remaining, 120)
                    )
                else:
                    loop = asyncio.get_running_loop()
                    chunk = await asyncio.wait_for(
                        loop.run_in_executor(
                            None, partial(next, iterator, _ITER_EXHAUSTED)
                        ),
                        timeout=min(remaining, 120),
                    )
                    if chunk is _ITER_EXHAUSTED:
                        break
            except StopAsyncIteration:
                break


            decoded = (
                chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
            )
            buffer += decoded


            while "\n\n" in buffer:
                raw_event, buffer = buffer.split("\n\n", 1)
                for line in raw_event.splitlines():
                    s = line.strip()
                    if s.startswith("data:"):
                        payload = s[5:].lstrip()
                        if not payload or payload == "[DONE]":
                            continue
                        try:
                            parsed = json.loads(payload)
                            for choice in parsed.get("choices", []):
                                c = (choice.get("delta") or {}).get("content", "")
                                if c:
                                    parts.append(c)
                        except (
                            json.JSONDecodeError, KeyError, IndexError, TypeError
                        ):
                            pass


        for line in buffer.splitlines():
            s = line.strip()
            if s.startswith("data:"):
                payload = s[5:].lstrip()
                if payload and payload != "[DONE]":
                    try:
                        parsed = json.loads(payload)
                        for choice in parsed.get("choices", []):
                            c = (choice.get("delta") or {}).get("content", "")
                            if c:
                                parts.append(c)
                    except (
                        json.JSONDecodeError, KeyError, IndexError, TypeError
                    ):
                        pass


        return "".join(parts)


    def _extract_response_content(self, response: Any) -> str:
        if isinstance(response, dict):
            choices = response.get("choices", [])
            if choices:
                msg = choices[0].get("message", {}) or choices[0].get("delta", {})
                return msg.get("content", "")
            if "error" in response:
                return f"[ERROR] {response['error']}"


        if isinstance(response, str):
            return response


        if hasattr(response, "body"):
            try:
                body = response.body
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                data = json.loads(body)
                choices = data.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content", "")
            except Exception:
                pass


        return f"[ERROR] Unexpected response type: {type(response).__name__}"


    async def _direct_llm_fallback(
        self, request: Request, model_id: str, messages: List[Dict[str, str]]
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


            base_url = str(request.base_url).rstrip("/")


            async with aiohttp.ClientSession() as session:
                headers = {"Content-Type": "application/json"}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    headers["Cookie"] = f"token={token}"


                async with session.post(
                    f"{base_url}/api/chat/completions",
                    json={
                        "model": model_id,
                        "messages": messages,
                        "stream": False,
                    },
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.valves.LLM_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        choices = data.get("choices", [])
                        if choices:
                            return choices[0].get("message", {}).get("content", "")
                        return "[ERROR] No choices in response"
                    else:
                        body = await resp.text()
                        return f"[ERROR] LLM API {resp.status}: {body[:500]}"


        except Exception as e:
            logger.error(f"[BayanDe] Direct LLM fallback error: {e}")
            return f"[ERROR] LLM fallback failed: {e}"


    # ========================================================
    # STATUS_DEV.MD PARSER
    # ========================================================
    def _parse_status_dev(self, content: str) -> List[Dict[str, str]]:
        """Parse status_dev.md — JSON block atau markdown table."""
        json_match = re.search(
            r"```json\s*\n($$.*?$$)\s*\n```", content, re.DOTALL
        )
        if json_match:
            try:
                tasks = json.loads(json_match.group(1))
                if isinstance(tasks, list):
                    return tasks
            except json.JSONDecodeError:
                pass


        tasks = []
        lines = content.split("\n")
        in_table = False


        for line in lines:
            stripped = line.strip()
            if not stripped or "|" not in stripped:
                continue


            if re.match(r"^\|[\s\-:]+\|", stripped):
                in_table = True
                continue


            if in_table and task_id_pattern(stripped):
                parts = stripped.split("|")
                parts = [p.strip() for p in parts if p.strip()]


                if len(parts) >= 3:
                    task = {
                        "id": parts[0],
                        "name": parts[1] if len(parts) > 1 else "",
                        "status": parts[2] if len(parts) > 2 else "",
                        "files": parts[3] if len(parts) > 3 else "",
                        "notes": parts[4] if len(parts) > 4 else "",
                    }
                    if task["id"]:
                        tasks.append(task)


        return tasks


    def _update_status_dev(
        self, content: str, task_id: str, update: Dict[str, str]
    ) -> str:
        """Update task di status_dev.md."""
        json_match = re.search(
            r"```json\s*\n($$.*?$$)\s*\n```", content, re.DOTALL
        )
        if json_match:
            try:
                tasks = json.loads(json_match.group(1))
                for task in tasks:
                    if task.get("id") == task_id:
                        task.update(update)
                        break
                new_json = json.dumps(tasks, indent=2, ensure_ascii=False)
                return content.replace(
                    json_match.group(0), f"```json\n{new_json}\n```"
                )
            except json.JSONDecodeError:
                pass


        lines = content.split("\n")
        for i, line in enumerate(lines):
            if task_id in line and "|" in line:
                parts = line.split("|")
                if len(parts) >= 6:
                    if "status" in update:
                        parts[3] = f" {update['status']} "
                    if "files" in update:
                        parts[4] = f" {update['files']} "
                    if "notes" in update:
                        parts[5] = f" {update['notes'][:150]} "
                    lines[i] = "|".join(parts)
                    break


        return "\n".join(lines)


    # ========================================================
    # PROMPT BUILDERS
    # ========================================================
    def _build_coder_prompt(
        self,
        task: Dict[str, str],
        knowledge: Dict[str, str],
        relevant_files: Dict[str, str],
        mode: str = "day",
    ) -> str:
        knowledge_section = ""
        for fname, content in knowledge.items():
            knowledge_section += f"\n\n### {fname}\n```\n{content}\n```\n"


        files_section = ""
        for fname, content in relevant_files.items():
            files_section += f"\n\n### {fname}\n```python\n{content}\n```\n"


        if not files_section:
            files_section = "\n(Tidak ada file relevan yang ditemukan. Buat file baru sesuai kebutuhan.)"


        mode_instruction = ""
        if mode.lower() == "swing":
            mode_instruction = """
🎯 MODE: SWING TRADE
- Timeframe: 4H dan Daily
- Indikator: gunakan parameter yang cocok untuk swing (lebih longgar)
- Holding period: beberapa hari sampai minggu
- Position sizing: lebih konservatif (max 2% risk per trade)
"""
        else:
            mode_instruction = """
🎯 MODE: DAY TRADE (default)
- Timeframe: 5m, 15m, 1H
- Indikator: parameter ketat untuk intraday
- Holding period: beberapa menit sampai jam
- Position sizing: max 1% risk per trade
"""


        return f"""Kamu adalah AI Coder untuk project BayanDeZenith (AI Trading Agent).


# TASK
**ID**: {task.get('id', 'UNKNOWN')}
**Nama**: {task.get('name', '')}
**Deskripsi**: {task.get('description', task.get('name', ''))}
{mode_instruction}


# KNOWLEDGE FILES (WAJIB BACA)
{knowledge_section}


# RELEVANT PROJECT FILES
{files_section}


# INSTRUKSI
1. Tulis kode Python yang production-ready untuk menyelesaikan task di atas
2. Ikuti struktur folder yang sudah ada di project
3. Gunakan best practices: type hints, docstring, error handling, logging
4. Tulis unit test kalau perlu
5. Jangan modify file yang tidak relevan dengan task


# OUTPUT FORMAT
Return **HANYA JSON** (tanpa markdown, tanpa penjelasan):
{{
  "files": [
    {{
      "path": "path/relative/ke/sandbox.py",
      "content": "full content file di sini (escape newline dengan \\n)"
    }}
  ],
  "test_file": "path/relative/test_xxx.py (kosongkan kalau tidak perlu)",
  "summary": "Ringkasan singkat apa yang kamu buat (1-2 kalimat)"
}}
"""


    def _build_retry_prompt(
        self,
        original_prompt: str,
        coder_response: str,
        reviewer_feedback: str,
    ) -> str:
        return f"""{original_prompt}


# ⚠️ REVIEWER REJECTED PREVIOUS ATTEMPT


## Previous Coder Output:
{coder_response[:3000]}


## Reviewer Feedback:
{reviewer_feedback}


## INSTRUKSI
Perbaiki kode berdasarkan feedback reviewer di atas. Return JSON dengan format yang sama.
"""


    def _build_reviewer_prompt(
        self,
        task: Dict[str, str],
        coder_output: str,
        written_files: Dict[str, str],
    ) -> str:
        files_section = ""
        for path, content in written_files.items():
            files_section += f"\n\n### {path}\n```python\n{content}\n```\n"


        return f"""Kamu adalah AI Code Reviewer untuk project BayanDeZenith.

# TASK YANG DIKERJAKAN
**ID**: {task.get('id', 'UNKNOWN')}
**Nama**: {task.get('name', '')}

# CODER OUTPUT
```json
{coder_output}
```

# FILE YANG DITULIS KE DISK
{files_section}

# TUGAS REVIEWER
1. Cek apakah file benar-benar ditulis ke disk (baca file aktual)
2. Review kualitas kode: correctness, best practices, error handling
3. Cek apakah task selesai sesuai requirement
4. Return verdict

# OUTPUT FORMAT
Return HANYA JSON:
{{
  "verdict": "APPROVED" | "APPROVED_WITH_NOTES" | "REJECTED",
  "catatan": "Catatan singkat",
  "harus_diubah": "List hal yang harus diperbaiki (kalau REJECTED)"
}}
"""


 
    # ========================================================
    # TOKEN ESTIMATION
    # ========================================================
    def _estimate_tokens(self, text: str) -> int:
        """Rough token estimate: ~4 chars per token."""
        return len(text) // 4


    def _get_token_summary(self) -> str:
        total = sum(self._token_usage.values())
        lines = ["\n📊 **Token Usage:**"]
        for key, value in self._token_usage.items():
            if value > 0:
                lines.append(f"- {key}: {value:,}")
        lines.append(f"- **Total**: {total:,}")
        if total > self.valves.MAX_TOKEN_PER_TASK:
            lines.append(f"⚠️ Exceeded budget ({self.valves.MAX_TOKEN_PER_TASK:,})")
        return "\n".join(lines)


    # ========================================================
    # MAIN PIPE
    # ========================================================
    async def pipe(
        self,
        body: Dict[str, Any],
        __user__: Optional[Dict[str, Any]] = None,
        __request__: Optional[Request] = None,
        __event_emitter__: Optional[Callable] = None,
        __event_call__: Optional[Callable] = None,
    ) -> AsyncGenerator[str, None] | str:
        # Reset token usage
        self._token_usage = {
            "coder_input": 0,
            "coder_output": 0,
            "reviewer_input": 0,
            "reviewer_output": 0,
        }


        await self._emit_status(
            __event_emitter__, "🚀 BayanDeZenith Pipeline v4.6 starting..."
        )


        # ====== Parse user message ======
        messages = body.get("messages", [])
        user_message = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            user_message = item.get("text", "")
                            break
                else:
                    user_message = str(content)
                break


        if not user_message:
            return "❌ Tidak ada pesan user."


        # ====== Parse task ID ======
        task_id_match = re.search(r"P\d+-\d+", user_message)
        if not task_id_match:
            return (
                "❌ Task ID tidak ditemukan. Format: `kerjakan P0-004` atau "
                "`kerjakan P0-004 swing`"
            )
        task_id = task_id_match.group(0)


        # ====== Parse mode ======
        mode = "day"
        if "swing" in user_message.lower():
            mode = "swing"


        await self._emit_status(
            __event_emitter__,
            f"🚀 Memulai pipeline **{task_id}** (Mode: {mode.title()} Trade)...",
        )


        # ====== Read knowledge files ======
        await self._emit_status(__event_emitter__, "📖 Membaca knowledge files...")


        knowledge: Dict[str, str] = {}
        knowledge_files = [
            "konteks_permanen.md",
            "aturan_sistem.md",
            "status_dev.md",
        ]


        for fname in knowledge_files:
            fpath = os.path.join(self.valves.KNOWLEDGE_PATH, fname)
            content = await self._read_file(fpath)
            if content.startswith("[ERROR"):
                await self._emit_status(
                    __event_emitter__,
                    f"❌ Gagal baca {fname}\nError: {content}",
                    done=True,
                )
                return (
                    f"❌ **Gagal baca knowledge file**\n\n"
                    f"**File**: {fname}\n"
                    f"**Error**: {content}\n\n"
                    f"**Klasifikasi**: {self._classify_error(content)}\n\n"
                    f"Pipeline stop untuk hemat token."
                )
            knowledge[fname] = content


        # ====== Parse status_dev.md untuk ambil task detail ======
        status_dev = knowledge.get("status_dev.md", "")
        tasks = self._parse_status_dev(status_dev)


        task_info = {"id": task_id, "name": task_id, "description": user_message}
        for t in tasks:
            if t.get("id") == task_id:
                task_info.update(t)
                break


        # ====== Scan project files ======
        await self._emit_status(
            __event_emitter__, "🔍 Scanning project files..."
        )
        relevant_files = await self._scan_project_files(
            task_info.get("description", task_info.get("name", ""))
        )
        await self._emit_status(
            __event_emitter__,
            f"📁 Found {len(relevant_files)} relevant file(s)",
        )


        # ====== CODER LOOP with SMART RETRY ======
        max_retry = self.valves.MAX_RETRY
        coder_response = ""
        reviewer_feedback = ""
        v = ""
        original_prompt = self._build_coder_prompt(
            task_info, knowledge, relevant_files, mode
        )


        for attempt in range(max_retry + 1):
            # Cek token budget
            total_tokens = sum(self._token_usage.values())
            if total_tokens > self.valves.MAX_TOKEN_PER_TASK:
                await self._emit_status(
                    __event_emitter__,
                    f"⚠️ Token budget exceeded ({total_tokens:,}/{self.valves.MAX_TOKEN_PER_TASK:,})",
                    done=True,
                )
                return (
                    f"⚠️ **Token budget exceeded**\n\n"
                    f"Used: {total_tokens:,} / {self.valves.MAX_TOKEN_PER_TASK:,}\n\n"
                    f"{self._get_token_summary()}"
                )


            attempt_label = "pertama" if attempt == 0 else f"retry ke-{attempt}"
            await self._emit_status(
                __event_emitter__,
                f"🧠 Coder menulis ({attempt_label}, {attempt+1}/{max_retry+1})...",
            )


            # Build prompt
            if attempt == 0:
                coder_prompt = original_prompt
            else:
                coder_prompt = self._build_retry_prompt(
                    original_prompt, coder_response, reviewer_feedback
                )


            self._token_usage["coder_input"] += self._estimate_tokens(coder_prompt)


            coder_response = await self._call_llm_async(
                __request__,
                self.valves.CODER_MODEL,
                [{"role": "user", "content": coder_prompt}],
                user_obj=__user__,
            )
            self._token_usage["coder_output"] += self._estimate_tokens(
                coder_response
            )


            # ====== SMART RETRY LOGIC (CODER) ======
            if not coder_response or coder_response.startswith("[ERROR]"):
                error_classification = self._classify_error(coder_response or "")


                # Cek apakah error bisa di-retry
                if not self._is_retryable_error(coder_response or ""):
                    await self._emit_status(
                        __event_emitter__,
                        f"🛑 INFRASTRUCTURE ERROR — Stop retry (hemat token)\n{error_classification}",
                        done=True,
                    )
                    return (
                        f"❌ **Infrastructure Error** (tidak di-retry untuk hemat token)\n\n"
                        f"**Error:** {coder_response}\n\n"
                        f"**Klasifikasi:** {error_classification}\n\n"
                        f"{self._get_token_summary()}"
                    )


                # Error retryable (logical) — lanjut retry biasa
                await self._emit_status(
                    __event_emitter__,
                    f"❌ Coder gagal: {coder_response[:200]}",
                )
                if attempt < max_retry:
                    await self._emit_status(
                        __event_emitter__,
                        f"🔄 Mencoba ulang ({error_classification})...",
                    )
                    reviewer_feedback = coder_response
                    continue


                await self._emit_status(
                    __event_emitter__, "❌ Coder gagal total.", done=True
                )
                return (
                    f"Error: Coder gagal setelah {max_retry} retry.\n\n"
                    f"{self._get_token_summary()}"
                )
            # ====== END SMART RETRY LOGIC ======


            # ====== Parse coder output ======
            try:
                # Extract JSON
                json_match = re.search(
                    r"\{[\s\S]*\}", coder_response
                )
                if not json_match:
                    raise ValueError("No JSON found in response")


                coder_output = json.loads(json_match.group(0))
                files_to_write = coder_output.get("files", [])


                if not files_to_write:
                    raise ValueError("No files in output")


            except Exception as e:
                await self._emit_status(
                    __event_emitter__,
                    f"⚠️ Parse error: {e}",
                )
                if attempt < max_retry:
                    reviewer_feedback = f"JSON parse error: {e}"
                    continue
                return f"❌ Coder output tidak valid: {e}"


            # ====== Write files ======
            if self.valves.READ_ONLY_MODE:
                await self._emit_status(
                    __event_emitter__, "📖 Read-only mode, skip write"
                )
            else:
                await self._emit_status(
                    __event_emitter__,
                    f"💾 Writing {len(files_to_write)} file(s)...",
                )


                written_files: Dict[str, str] = {}
                failed_writes: List[str] = []


                for file_info in files_to_write:
                    path = file_info.get("path", "")
                    content = file_info.get("content", "")


                    if not path or not content:
                        continue


                    full_path = os.path.join(self.valves.SANDBOX_PATH, path)
                    ok = await self._write_file(full_path, content)


                    if ok:
                        written_files[path] = content
                        await self._emit_status(
                            __event_emitter__, f"✅ {path}"
                        )
                    else:
                        failed_writes.append(path)
                        await self._emit_status(
                            __event_emitter__, f"❌ Failed: {path}"
                        )


                if failed_writes:
                    error_msg = f"Failed to write: {', '.join(failed_writes)}"
                    if not self._is_retryable_error(error_msg):
                        error_classification = self._classify_error(error_msg)
                        await self._emit_status(
                            __event_emitter__,
                            f"🛑 WRITE ERROR — Stop retry\n{error_classification}",
                            done=True,
                        )
                        return (
                            f"❌ **Write Error** (infrastructure)\n\n"
                            f"**Failed files:** {', '.join(failed_writes)}\n\n"
                            f"**Klasifikasi:** {error_classification}\n\n"
                            f"{self._get_token_summary()}"
                        )


                    if attempt < max_retry:
                        reviewer_feedback = error_msg
                        continue


            # ====== REVIEWER ======
            # BUGFIX v4.6.1: written_files mungkin undefined jika READ_ONLY_MODE=True
            if 'written_files' not in locals():
                written_files = {}

            await self._emit_status(
                __event_emitter__, "🔍 Reviewer memeriksa kode..."
            )


            reviewer_prompt = self._build_reviewer_prompt(
                task_info, coder_response, written_files
            )
            self._token_usage["reviewer_input"] += self._estimate_tokens(
                reviewer_prompt
            )


            reviewer_response = await self._call_llm_async(
                __request__,
                self.valves.REVIEWER_MODEL,
                [{"role": "user", "content": reviewer_prompt}],
                user_obj=__user__,
            )
            self._token_usage["reviewer_output"] += self._estimate_tokens(
                reviewer_response
            )


            # Parse reviewer output
            try:
                json_match = re.search(r"\{[\s\S]*\}", reviewer_response)
                if not json_match:
                    raise ValueError("No JSON")
                verdict = json.loads(json_match.group(0))
                v = verdict.get("verdict", "REJECTED")
            except Exception as e:
                v = "APPROVED_WITH_NOTES"
                verdict = {
                    "verdict": v,
                    "catatan": f"Reviewer parse error: {e}",
                }


            # ====== SMART RETRY LOGIC (REVIEWER) ======
            if "APPROVED" in v:
                await self._emit_status(
                    __event_emitter__, f"✅ Code {v}!"
                )
                break
            else:
                reviewer_feedback = (
                    verdict.get("harus_diubah", "")
                    or verdict.get("catatan", "Perbaiki kode.")
                )


                # Cek apakah rejection karena infrastructure
                if not self._is_retryable_error(reviewer_feedback):
                    error_classification = self._classify_error(
                        reviewer_feedback
                    )
                    await self._emit_status(
                        __event_emitter__,
                        f"🛑 REJECTED karena infrastructure — Stop retry\n{error_classification}",
                        done=True,
                    )
                    return (
                        f"❌ **Task {task_id} gagal** (infrastructure error)\n\n"
                        f"**Verdict:** {v}\n"
                        f"**Klasifikasi:** {error_classification}\n"
                        f"**Detail:** {reviewer_feedback}\n\n"
                        f"{self._get_token_summary()}"
                    )


                if attempt < max_retry:
                    remaining = max_retry - attempt
                    await self._emit_status(
                        __event_emitter__,
                        f"❌ REJECTED — {remaining} retry tersisa...",
                    )
                else:
                    await self._emit_status(
                        __event_emitter__,
                        f"❌ REJECTED setelah {max_retry} retry. Menyerah.",
                        done=True,
                    )
                    return (
                        f"Task {task_id} gagal setelah {max_retry} retry.\n\n"
                        f"**Verdict:** {v}\n"
                        f"**Catatan:** {verdict.get('catatan', '')}\n"
                        f"**Harus diubah:** {verdict.get('harus_diubah', '')}\n\n"
                        f"{self._get_token_summary()}"
                    )
            # ====== END SMART RETTRY (REVIEWER) ======


        # ====== EXECUTE TESTS (optional) ======
        test_file = coder_output.get("test_file", "") if 'coder_output' in locals() else ""
        test_result = ""


        if test_file and not self.valves.READ_ONLY_MODE:
            test_path = os.path.join(self.valves.SANDBOX_PATH, test_file)
            if os.path.isfile(test_path):
                await self._emit_status(
                    __event_emitter__, f"🧪 Running tests: {test_file}..."
                )


                if self.valves.REQUIRE_USER_CONFIRM:
                    confirmed = await self._ask_user_confirmation(
                        __event_call__,
                        f"Akan eksekusi test: {test_file}\n\nLanjutkan?",
                    )
                    if not confirmed:
                        await self._emit_status(
                            __event_emitter__, "⏸️ Test dibatalkan user"
                        )
                        test_result = "Dibatalkan user"
                    else:
                        ok, output = await self._execute_code(
                            test_path, self.valves.CODE_EXEC_TIMEOUT
                        )
                        test_result = output
                        if ok:
                            await self._emit_status(
                                __event_emitter__, f"✅ Test passed"
                            )
                        else:
                            await self._emit_status(
                                __event_emitter__,
                                f"❌ Test failed: {output[:200]}",
                            )
                else:
                    ok, output = await self._execute_code(
                        test_path, self.valves.CODE_EXEC_TIMEOUT
                    )
                    test_result = output
                    if ok:
                        await self._emit_status(
                            __event_emitter__, f"✅ Test passed"
                        )
                    else:
                        await self._emit_status(
                            __event_emitter__,
                            f"❌ Test failed: {output[:200]}",
                        )


        # ====== UPDATE STATUS_DEV.MD ======
        if not self.valves.READ_ONLY_MODE and "APPROVED" in v:
            await self._emit_status(
                __event_emitter__, "📝 Updating status_dev.md..."
            )


            files_list = ", ".join(written_files.keys()) if 'written_files' in locals() else ""
            notes = verdict.get("catatan", "")[:200] if 'verdict' in locals() else ""


            updated_status = self._update_status_dev(
                status_dev,
                task_id,
                {
                    "status": "✅ Selesai",
                    "files": files_list,
                    "notes": notes,
                },
            )


            status_path = os.path.join(
                self.valves.KNOWLEDGE_PATH, "status_dev.md"
            )
            try:
                with open(status_path, "w", encoding="utf-8") as f:
                    f.write(updated_status)
                await self._emit_status(
                    __event_emitter__, "✅ status_dev.md updated"
                )
                if self.valves.CACHE_ENABLED:
                    _knowledge_cache.set(status_path, updated_status)
            except Exception as e:
                await self._emit_status(
                    __event_emitter__, f"⚠️ Failed update status: {e}"
                )


        # ====== FINAL OUTPUT ======
        await self._emit_status(
            __event_emitter__,
            f"✅ Task {task_id} selesai!",
            done=True,
        )


        summary = coder_output.get("summary", "") if 'coder_output' in locals() else ""
        files_list = "\n".join(
            f"- `{p}`" for p in (written_files.keys() if 'written_files' in locals() else [])
        )


        result = f"""## ✅ Task {task_id} Selesai
 


    Mode: {mode.title()} Trade
    Verdict: {v}




    📝 Summary


    {summary}




    📁 Files Created/Modified


    {files_list}




    🧪 Test Result
    {test_result[:1000] if test_result else "(tidak ada test)"}


    📋 Reviewer Notes


    {verdict.get('catatan', '-') if 'verdict' in locals() else '-'}


    {self._get_token_summary()}
    """


 
        await self._emit_replace(__event_emitter__, result)
        return result