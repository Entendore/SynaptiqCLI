#!/usr/bin/env python3
"""
Synaptiq Project Generator & Manager CLI
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections import deque, OrderedDict
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import IntEnum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, TypeAlias, final, runtime_checkable

if TYPE_CHECKING:
    from synaptiq.core.orchestrator import Orchestrator
    from synaptiq.core.registry import ToolRegistry
    from synaptiq.core.session import Session

try:
    from rich.console import Console as RichCore
    from rich.markdown import Markdown
    from rich.panel import Panel
    from rich.prompt import Confirm, Prompt
    from rich.table import Table
    from rich.syntax import Syntax
    from rich.live import Live
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion, PathCompleter
    from prompt_toolkit.document import Document
    from prompt_toolkit.history import FileHistory
    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False

__all__ = [
    "AppConfig", "GlobalStats", "ExitCode", "SynaptiqError",
    "InvalidNameError", "ProjectNotFoundError", "App", "main",
]

# ═══════════════════════════════════════════════════════════════════════════
# 1. DOMAIN TYPES & EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════

VERSION: Final = "23.0.1"
FRAMEWORK_VERSION: Final = "5.1.0"

CACHE_DIR: Final = Path.home() / ".synaptiq"
LOG_DIR: Final = CACHE_DIR / "logs"
STATS_FILE: Final = CACHE_DIR / "stats.json"
HISTORY_FILE: Final = CACHE_DIR / "repl_history"

MAX_NAME_LENGTH: Final = 64
VALID_PROVIDERS: Final = frozenset({"anthropic", "openai", "ollama", "mock"})
DEFAULT_MODEL: Final = "claude-3-5-sonnet-20241022"
RESERVED_NAMES: Final = frozenset({
    "__init__", "base", "core", "tools", "agents", "ui", "tests",
    "conftest", "setup", "build", "synaptiq",
})

_LOGGER = logging.getLogger("synaptiq")


class ExitCode(IntEnum):
    SUCCESS = 0
    GENERIC_ERROR = auto()
    NOT_FOUND = auto()
    ALREADY_EXISTS = auto()
    VALIDATION_ERROR = auto()
    INTERRUPTED = 130


class SynaptiqError(Exception):
    """Base exception for domain errors."""
    def __init__(self, message: str, code: ExitCode = ExitCode.GENERIC_ERROR) -> None:
        super().__init__(message)
        self.code = code


class InvalidNameError(SynaptiqError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Invalid name: '{name}'. Must be lowercase alphanumeric with underscores, "
            f"max {MAX_NAME_LENGTH} chars, and not a reserved name.",
            ExitCode.VALIDATION_ERROR,
        )


class ProjectNotFoundError(SynaptiqError):
    def __init__(self, path: Path) -> None:
        super().__init__(
            f"Synaptiq project not found at {path}. Run 'synaptiq init <name>' first.",
            ExitCode.NOT_FOUND,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class AppConfig:
    """Immutable application configuration."""
    project_dir: Path
    debug: bool = False
    quiet: bool = False
    use_color: bool = True
    json_output: bool = False


@dataclass(slots=True, kw_only=True)
class GlobalStats:
    sessions: int = 0
    prompts: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    history: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=90))
    _history_logged: bool = field(default=False, repr=False)
    _is_dirty: bool = field(default=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __repr__(self) -> str:
        return (
            f"GlobalStats(sessions={self.sessions}, prompts={self.prompts}, "
            f"in={self.input_tokens}, out={self.output_tokens}, "
            f"cost=${self.cost_usd:.4f})"
        )

    @classmethod
    def load(cls) -> "GlobalStats":
        if STATS_FILE.exists():
            with contextlib.suppress(json.JSONDecodeError, TypeError, KeyError):
                data = json.loads(STATS_FILE.read_text())
                history_data = data.get("history", [])
                # Reconstruct cumulative totals from last snapshot
                if history_data:
                    last = history_data[-1]
                    data["cost_usd"] = last.get("cost_usd", 0.0)
                    data["prompts"] = last.get("prompts", 0)
                    data["input_tokens"] = last.get("input_tokens", 0)
                    data["output_tokens"] = last.get("output_tokens", 0)
                data["history"] = deque(history_data, maxlen=90)
                data.pop("_history_logged", None)
                data.pop("_is_dirty", None)
                data.pop("_lock", None)
                return cls(**data)
        return cls()

    def save(self) -> None:
        with self._lock:
            if not self._is_dirty and self._history_logged:
                return

            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            if not self._history_logged:
                self.history.append({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "cost_usd": round(self.cost_usd, 6),
                    "prompts": self.prompts,
                    "input_tokens": self.input_tokens,
                    "output_tokens": self.output_tokens,
                })
                self._history_logged = True

            data = {
                k: list(v) if isinstance(v, deque) else v
                for k, v in self.__dict__.items()
                if not k.startswith("_")
            }

            tmp_file = STATS_FILE.with_suffix(".tmp")
            tmp_file.write_text(json.dumps(data, indent=2))
            os.replace(tmp_file, STATS_FILE)
            self._is_dirty = False

    def get_daily_costs(self, days: int = 7) -> list[dict[str, Any]]:
        """Return daily cost breakdown — computes deltas between snapshots."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        snapshots = []
        for entry in self.history:
            try:
                ts = datetime.fromisoformat(entry["ts"])
                if ts >= cutoff:
                    snapshots.append((ts, entry))
            except (KeyError, ValueError):
                continue
        if not snapshots:
            return []
        snapshots.sort(key=lambda x: x[0])
        daily: dict[str, dict[str, Any]] = {}
        prev = {"cost_usd": 0.0, "prompts": 0, "input_tokens": 0, "output_tokens": 0}
        for ts, entry in snapshots:
            day = ts.strftime("%Y-%m-%d")
            if day not in daily:
                daily[day] = {
                    "date": day, "cost": 0.0, "prompts": 0,
                    "input_tokens": 0, "output_tokens": 0
                }
            daily[day]["cost"] += max(0.0, entry.get("cost_usd", 0.0) - prev["cost_usd"])
            daily[day]["prompts"] += max(0, entry.get("prompts", 0) - prev["prompts"])
            daily[day]["input_tokens"] += max(0, entry.get("input_tokens", 0) - prev["input_tokens"])
            daily[day]["output_tokens"] += max(0, entry.get("output_tokens", 0) - prev["output_tokens"])
            prev = entry
        return sorted(daily.values(), key=lambda x: x["date"])


@contextlib.contextmanager
def stats_session() -> Iterator[GlobalStats]:
    """Context manager for transactional stats loading and saving."""
    stats = GlobalStats.load()
    snapshot = (
        stats.prompts, stats.sessions,
        stats.input_tokens, stats.output_tokens,
        stats.cost_usd, stats._history_logged,
    )
    try:
        yield stats
    finally:
        current = (
            stats.prompts, stats.sessions,
            stats.input_tokens, stats.output_tokens,
            stats.cost_usd, stats._history_logged,
        )
        stats._is_dirty = current != snapshot
        stats.save()


# ═══════════════════════════════════════════════════════════════════════════
# 2. CONSOLE STRATEGY (Structural Subtyping)
# ═══════════════════════════════════════════════════════════════════════════

class Color:
    RED = "\033[91m"; GREEN = "\033[92m"; YELLOW = "\033[93m"; CYAN = "\033[96m"
    MAGENTA = "\033[95m"; BOLD = "\033[1m"; DIM = "\033[2m"; RESET = "\033[0m"


@runtime_checkable
class Console(Protocol):
    def banner(self) -> None: ...
    def header(self, text: str) -> None: ...
    def success(self, text: str) -> None: ...
    def warning(self, text: str) -> None: ...
    def error(self, text: str) -> None: ...
    def bold(self, text: str) -> None: ...
    def dim(self, text: str) -> None: ...
    def markdown(self, text: str) -> None: ...
    def prompt(self, text: str, default: str = "") -> str: ...
    def confirm(self, text: str, default: bool = False) -> bool: ...
    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None: ...


def _enable_vt100_on_windows() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        os.system("")


@final
class RichConsole:
    __slots__ = ("_console", "config")
    def __init__(self, config: AppConfig) -> None:
        _enable_vt100_on_windows()
        self.config = config
        self._console = RichCore()
    def banner(self) -> None:
        self._console.print(Panel.fit(
            f"[bold cyan]Synaptiq CLI v{VERSION}[/bold cyan]\n"
            f"[dim]Multi-Agent Cognitive Framework[/dim]"
        ))
    def header(self, text: str) -> None:
        self._console.rule(f"[bold]{text}[/bold]")
    def success(self, text: str) -> None:
        self._console.print(f"[green]✓ {text}[/green]")
    def warning(self, text: str) -> None:
        self._console.print(f"[yellow]! {text}[/yellow]")
    def error(self, text: str) -> None:
        self._console.print(f"[red]✗ {text}[/red]")
    def bold(self, text: str) -> None:
        self._console.print(f"[bold]{text}[/bold]")
    def dim(self, text: str) -> None:
        self._console.print(f"[dim]{text}[/dim]")
    def markdown(self, text: str) -> None:
        self._console.print(Markdown(text))
    def prompt(self, text: str, default: str = "") -> str:
        return Prompt.ask(text, default=default)
    def confirm(self, text: str, default: bool = False) -> bool:
        return Confirm.ask(text, default=default)
    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        if not rows:
            self.warning("No rows.")
            return
        t = Table(show_header=True, header_style="bold magenta")
        for h in headers:
            t.add_column(h)
        for r in rows:
            t.add_row(*r)
        self._console.print(t)


@final
class PlainConsole:
    __slots__ = ()
    def __init__(self) -> None:
        _enable_vt100_on_windows()
    def _emit(self, text: str, end: str = "\n") -> None:
        print(text, end=end)
    def banner(self) -> None:
        self._emit(f"{Color.CYAN}{Color.BOLD}  Synaptiq CLI v{VERSION}{Color.RESET}")
    def header(self, text: str) -> None:
        self._emit(f"\n{Color.BOLD} ── {text} {Color.RESET}")
    def success(self, text: str) -> None:
        self._emit(f"{Color.GREEN} ✓ {text}{Color.RESET}")
    def warning(self, text: str) -> None:
        self._emit(f"{Color.YELLOW} ! {text}{Color.RESET}")
    def error(self, text: str) -> None:
        self._emit(f"{Color.RED} ✗ {text}{Color.RESET}")
    def bold(self, text: str) -> None:
        self._emit(f"{Color.BOLD}{text}{Color.RESET}")
    def dim(self, text: str) -> None:
        self._emit(f"{Color.DIM}{text}{Color.RESET}")
    def markdown(self, text: str) -> None:
        clean = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
        clean = re.sub(r'`(.+?)`', r'\1', clean)
        clean = re.sub(r'^#+\s*', '', clean, flags=re.MULTILINE)
        self._emit(clean)
    def prompt(self, text: str, default: str = "") -> str:
        with contextlib.suppress(EOFError, KeyboardInterrupt):
            val = input(f"{text}{' ['+default+']' if default else ''} ").strip()
            return val or default
        return default
    def confirm(self, text: str, default: bool = False) -> bool:
        with contextlib.suppress(EOFError, KeyboardInterrupt):
            ans = input(f"{text} [{'Y/n' if default else 'y/N'}] ").strip().lower()
            return ans in ("y", "yes") if ans else default
        return default
    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        if not rows:
            self.warning("No rows.")
            return
        ncols = len(headers)
        widths = [len(h) for h in headers]
        for r in rows:
            r = list(r) + [""] * (ncols - len(r)) if len(r) < ncols else list(r[:ncols])
            for i, c in enumerate(r):
                widths[i] = max(widths[i], len(c))
        def fmt(row: Sequence[str]) -> str:
            return "  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(row))
        self._emit(f"{Color.BOLD}{fmt(headers)}{Color.RESET}")
        self._emit(f"{Color.DIM}{fmt(['-' * w for w in widths])}{Color.RESET}")
        for r in rows:
            self._emit(fmt(r))


@final
class QuietConsole:
    __slots__ = ()
    def banner(self) -> None: ...
    def header(self, text: str) -> None: ...
    def success(self, text: str) -> None: ...
    def warning(self, text: str) -> None: ...
    def bold(self, text: str) -> None: ...
    def dim(self, text: str) -> None: ...
    def markdown(self, text: str) -> None: ...
    def error(self, text: str) -> None:
        print(text, file=sys.stderr)
    def prompt(self, text: str, default: str = "") -> str:
        return default
    def confirm(self, text: str, default: bool = False) -> bool:
        return default
    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None: ...


def get_console(config: AppConfig) -> Console:
    if config.quiet:
        return QuietConsole()
    if RICH_AVAILABLE and config.use_color:
        return RichConsole(config)
    return PlainConsole()


# ═══════════════════════════════════════════════════════════════════════════
# 3. UTILITIES & FRAMEWORK TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

_NAME_RE = re.compile(r"[^a-z0-9_]+")


def sanitize_name(name: str) -> str:
    if len(name) > MAX_NAME_LENGTH:
        raise InvalidNameError(name)
    cleaned = _NAME_RE.sub("_", name.lower()).strip("_")
    if not cleaned or cleaned[0].isdigit():
        raise InvalidNameError(name)
    if cleaned in RESERVED_NAMES:
        raise InvalidNameError(f"{name} (reserved name)")
    return cleaned


def setup_logging(debug: bool) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _LOGGER
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    for h in logger.handlers[:]:
        if isinstance(h, logging.FileHandler):
            logger.removeHandler(h)
            h.close()
    fh = logging.FileHandler(LOG_DIR / "synaptiq.log")
    fh.setLevel(logging.DEBUG if debug else logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(fh)
    logger.propagate = False


class ConfigManager:
    """Manage synaptiq.json configuration."""

    DEFAULT_CONFIG: dict[str, Any] = {
        "default_provider": "anthropic",
        "default_model": DEFAULT_MODEL,
        "max_tool_iterations": 10,
        "max_tokens_per_turn": 4096,
        "routing_rules": [
            {"pattern": "code|python|javascript|refactor|bug", "provider": "openai", "model": "gpt-4o"},
            {"pattern": "poem|story|creative|essay", "provider": "anthropic", "model": "claude-3-5-sonnet-20241022"}
        ]
    }

    @staticmethod
    def load(config_path: Path) -> dict[str, Any]:
        if config_path.exists():
            with contextlib.suppress(json.JSONDecodeError):
                return json.loads(config_path.read_text())
        return {}

    @staticmethod
    def save(config_path: Path, config: dict[str, Any]) -> None:
        config_path.write_text(json.dumps(config, indent=2))

    @staticmethod
    def get(config_path: Path, key: str) -> Any:
        config = ConfigManager.load(config_path)
        return config.get(key)

    @staticmethod
    def set(config_path: Path, key: str, value: str) -> None:
        config = ConfigManager.load(config_path)
        with contextlib.suppress(json.JSONDecodeError):
            value = json.loads(value)
        config[key] = value
        ConfigManager.save(config_path, config)

    @staticmethod
    def list_all(config_path: Path) -> dict[str, Any]:
        return ConfigManager.load(config_path)

    @staticmethod
    def edit(config_path: Path) -> None:
        editor = os.environ.get("EDITOR", "nano")
        subprocess.run([editor, str(config_path)], check=False)


class Templates:
    """Embedded file templates for scaffolding."""

    PYPROJECT: str = (
        '[build-system]\n'
        'requires = ["setuptools>=61.0", "wheel"]\n'
        'build-backend = "setuptools.build_meta"\n'
        '\n'
        '[project]\n'
        'name = "synaptiq"\n'
        'version = "5.1.0"\n'
        'requires-python = ">=3.11"\n'
        'dependencies = ["anthropic", "openai", "chromadb", "networkx", "rich", "pydantic>=2.0", "rank_bm25", "tiktoken"]\n'
        '\n'
        '[project.scripts]\n'
        'synaptiq = "synaptiq.__main__:main"\n'
    )

    CONFIG_JSON: str = (
        '{\n'
        '  "default_provider": "anthropic",\n'
        f'  "default_model": "{DEFAULT_MODEL}",\n'
        '  "max_tool_iterations": 10,\n'
        '  "max_tokens_per_turn": 4096,\n'
        '  "routing_rules": [\n'
        '    {"pattern": "code|python|javascript|refactor|bug", "provider": "openai", "model": "gpt-4o"},\n'
        '    {"pattern": "poem|story|creative|essay", "provider": "anthropic", "model": "claude-3-5-sonnet-20241022"}\n'
        '  ]\n'
        '}\n'
    )

    GITIGNORE: str = (
        '__pycache__/\n'
        '*.pyc\n'
        '.env\n'
        '.venv/\n'
        'synaptiq_db/\n'
        'global_db/\n'
        '*.log\n'
        'queue.json\n'
        'state.json\n'
        'session_backup.json\n'
    )

    ENV_EXAMPLE: str = (
        'ANTHROPIC_API_KEY=sk-ant-xxxxxxxx\n'
        'OPENAI_API_KEY=sk-xxxxxxxx\n'
    )

    INIT: str = (
        '"""Synaptiq: an event-driven, cognitive AI agent framework."""\n'
        'from __future__ import annotations\n'
        '__version__ = "5.1.0"\n'
        'from .core.agent import Agent\n'
        'from .core.event_bus import EventBus\n'
        'from .core.memory import MemoryStore\n'
        'from .core.orchestrator import Orchestrator, SubAgent\n'
        'from .core.provider import LLMProvider, get_provider\n'
        'from .core.registry import ToolRegistry\n'
        'from .core.session import Session\n'
        'from .core.state_manager import StateManager\n'
        '__all__ = ["Agent", "EventBus", "MemoryStore", "Orchestrator", "SubAgent", "LLMProvider", "get_provider", "ToolRegistry", "Session", "StateManager"]\n'
    )

    MAIN: str = (
        '"""CLI entry point for Synaptiq."""\n'
        'from __future__ import annotations\n'
        'import asyncio, sys\n'
        'from .ui.cli import SynaptiqCLI\n'
        'def main() -> None:\n'
        '    try:\n'
        '        asyncio.run(SynaptiqCLI().run())\n'
        '    except KeyboardInterrupt:\n'
        '        print("\\nGoodbye!")\n'
        '        sys.exit(0)\n'
        'if __name__ == "__main__": main()\n'
    )

    CORE_PROVIDER: str = '''"""LLM provider abstraction."""
from __future__ import annotations
import asyncio, logging, os, json, re, contextlib
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
logger = logging.getLogger(__name__)

async def _with_retry(coro, retries=3):
    for i in range(retries):
        try:
            return await coro()
        except Exception:
            if i == retries - 1:
                raise
            await asyncio.sleep(2 ** i)

class LLMProvider(ABC):
    @abstractmethod
    async def chat(self, messages, tools, system, max_tokens=4096) -> dict[str, Any]: ...
    @abstractmethod
    async def stream(self, messages, tools, system, max_tokens=4096) -> AsyncIterator[str]: ...

class AnthropicProvider(LLMProvider):
    def __init__(self, api_key, model="claude-3-5-sonnet-20241022"):
        import anthropic
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model = model
    async def chat(self, m, t, s, max_tokens=4096):
        async def _c():
            kw = {"model": self.model, "max_tokens": max_tokens, "system": s, "messages": m}
            if t:
                kw["tools"] = t
            r = await self.client.messages.create(**kw)
            return {
                "content": r.content,
                "stop_reason": r.stop_reason,
                "tool_calls": [b for b in r.content if getattr(b, "type", None) == "tool_use"],
                "usage": {"input_tokens": r.usage.input_tokens, "output_tokens": r.usage.output_tokens},
            }
        return await _with_retry(_c)
    async def stream(self, m, t, s, max_tokens=4096):
        kw = {"model": self.model, "max_tokens": max_tokens, "system": s, "messages": m}
        if t:
            kw["tools"] = t
        async with self.client.messages.stream(**kw) as stream:
            async for text in stream.text_stream:
                yield text

class OpenAIProvider(LLMProvider):
    def __init__(self, api_key, model="gpt-4o"):
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
    async def chat(self, m, t, s, max_tokens=4096):
        async def _c():
            messages = [{"role": "system", "content": s}] + m if s else m
            kw = {"model": self.model, "max_tokens": max_tokens, "messages": messages}
            if t:
                kw["tools"] = [{"type": "function", "function": tool} for tool in t]
            r = await self.client.chat.completions.create(**kw)
            choice = r.choices[0]
            tool_calls = None
            if choice.message.tool_calls:
                tool_calls = [
                    {"id": tc.id, "name": tc.function.name, "input": json.loads(tc.function.arguments or "{}")}
                    for tc in choice.message.tool_calls
                ]
            return {
                "content": [{"type": "text", "text": choice.message.content or ""}],
                "stop_reason": choice.finish_reason,
                "tool_calls": tool_calls,
                "usage": {"input_tokens": r.usage.prompt_tokens, "output_tokens": r.usage.completion_tokens},
            }
        return await _with_retry(_c)
    async def stream(self, m, t, s, max_tokens=4096):
        messages = [{"role": "system", "content": s}] + m if s else m
        kw = {"model": self.model, "max_tokens": max_tokens, "messages": messages, "stream": True}
        stream = await self.client.chat.completions.create(**kw)
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

class MockProvider(LLMProvider):
    def __init__(self, api_key=None, model="mock"):
        self.model = model
    async def chat(self, m, t, s, max_tokens=4096):
        last = ""
        for msg in reversed(m):
            if msg.get("role") == "user":
                content = msg.get("content")
                last = content if isinstance(content, str) else json.dumps(content)
                break
        return {
            "content": [{"type": "text", "text": f"[mock] {last[:200]}"}],
            "stop_reason": "end_turn",
            "tool_calls": None,
            "usage": {"input_tokens": 10, "output_tokens": 20},
        }
    async def stream(self, m, t, s, max_tokens=4096):
        yield "Mock streaming response."

class CascadeProvider(LLMProvider):
    def __init__(self, default_llm, api_key, rules):
        self.default_llm = default_llm
        self.api_key = api_key
        self.rules = rules
        cache_key = f"{default_llm.model}:{default_llm.__class__.__name__}"
        self._cache: dict[str, LLMProvider] = {cache_key: default_llm}
    def _get_llm_for_message(self, messages):
        if not messages:
            return self.default_llm
        last_msg_content = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                content = m.get("content")
                if isinstance(content, str):
                    last_msg_content = content
                elif isinstance(content, list):
                    last_msg_content = " ".join(
                        b.get("text", "") for b in content
                        if isinstance(b, dict) and b.get("type") == "text"
                    )
                break
        for rule in self.rules:
            pattern = rule.get("pattern", "")
            provider_name = rule.get("provider")
            model_name = rule.get("model")
            if not pattern or not provider_name or not model_name:
                continue
            if re.search(pattern, last_msg_content, re.IGNORECASE):
                cache_key = f"{model_name}:{provider_name}"
                if cache_key not in self._cache:
                    self._cache[cache_key] = get_provider(provider_name, self.api_key, model_name)
                return self._cache[cache_key]
        return self.default_llm
    async def chat(self, messages, tools, system, max_tokens=4096):
        llm = self._get_llm_for_message(messages)
        return await llm.chat(messages, tools, system, max_tokens)
    async def stream(self, messages, tools, system, max_tokens=4096):
        llm = self._get_llm_for_message(messages)
        async for chunk in llm.stream(messages, tools, system, max_tokens):
            yield chunk

def get_provider(name, key, model):
    match name.lower():
        case "anthropic":
            return AnthropicProvider(key, model)
        case "openai":
            return OpenAIProvider(key, model)
        case "ollama":
            return AnthropicProvider(key or "ollama", model)
        case "mock":
            return MockProvider(key, model)
        case _:
            raise ValueError(f"Unknown provider: {name}")
'''

    CORE_STATE_MANAGER: str = '''"""Manages persistent state variables."""
from __future__ import annotations
import json, contextlib
from pathlib import Path
from typing import Any
import logging
logger = logging.getLogger(__name__)

class StateManager:
    def __init__(self, config_path: str = "./synaptiq.json"):
        self.config_path = Path(config_path)
        self.state_file = self.config_path.parent / "state.json"
        self.state: dict[str, Any] = {}
        self._load()
    def _load(self) -> None:
        if self.state_file.exists():
            with contextlib.suppress(json.JSONDecodeError):
                self.state = json.loads(self.state_file.read_text())
        if self.config_path.exists():
            with contextlib.suppress(json.JSONDecodeError):
                cfg = json.loads(self.config_path.read_text())
                if "state" in cfg:
                    self.state.update(cfg["state"])
    def get(self, key: str) -> Any | None:
        return self.state.get(key)
    def set(self, key: str, value: Any) -> None:
        self.state[key] = value
        self._save()
    def _save(self) -> None:
        with contextlib.suppress(IOError):
            self.state_file.write_text(json.dumps(self.state, indent=2))
    def inject_into_prompt(self, prompt: str) -> str:
        return f"{prompt}\\n\\nCurrent Persistent State:\\n{json.dumps(self.state, indent=2)}" if self.state else prompt
'''

    CORE_EVENT_BUS: str = '''"""Central async event bus."""
from __future__ import annotations
import asyncio, logging, contextlib
from typing import Any, Callable, Awaitable
logger = logging.getLogger(__name__)

class EventBus:
    def __init__(self) -> None:
        self._sub: dict[str, list[Callable[[Any], Awaitable[None] | None]]] = {}
        self.context: dict[str, Any] = {}
    def subscribe(self, event: str, callback: Callable[[Any], Awaitable[None] | None]) -> None:
        self._sub.setdefault(event, []).append(callback)
    def unsubscribe(self, event: str, callback: Callable[[Any], Awaitable[None] | None]) -> None:
        if event in self._sub:
            self._sub[event] = [cb for cb in self._sub[event] if cb is not callback]
    async def publish(self, event: str, data: Any) -> None:
        for callback in list(self._sub.get(event, [])):
            with contextlib.suppress(Exception):
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
'''

    CORE_MEMORY: str = '''"""Vector DB wrapper with Tiered Storage and Time-Travel Graph."""
from __future__ import annotations
import logging, uuid, time, gzip, json, contextlib, hashlib
from pathlib import Path
from typing import Any
logger = logging.getLogger(__name__)

_E = False
_emb = None
try:
    import chromadb
    from chromadb.utils import embedding_functions
    try:
        _emb = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        _E = True
    except Exception:
        _E = False
except ImportError:
    _E = False

_BM25 = False
try:
    from rank_bm25 import BM25Okapi
    _BM25 = True
except ImportError:
    _BM25 = False

_G = False
try:
    import networkx as nx
    _G = True
except ImportError:
    _G = False

class MemoryStore:
    def __init__(self, db_path: str | Path = "./synaptiq_db"):
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        self.col_episodic = None
        self.col_semantic = None
        self._f: list[dict] = []
        self.graph = nx.DiGraph() if _G else None
        self.cold_path = self.db_path / "cold_storage.json.gz"
        self._last_prune = 0.0
        self._bm25_cache: tuple[str, Any] = ("", None)
        self.cli = None
        if _E and _emb is not None:
            with contextlib.suppress(Exception):
                self.cli = chromadb.PersistentClient(path=str(self.db_path / "chroma"))
                self.col_episodic = self.cli.get_or_create_collection("episodic_memory", embedding_function=_emb)
                self.col_semantic = self.cli.get_or_create_collection("semantic_memory", embedding_function=_emb)
    def _is_expired(self, meta: dict) -> bool:
        exp = meta.get("expires_at")
        return exp is not None and time.time() > exp
    def add_fact(self, content: str, metadata: dict | None = None, ttl_seconds: float | None = None, memory_type: str = "episodic") -> str:
        fact_id = f"mem_{uuid.uuid4().hex[:8]}"
        meta = metadata or {}
        if ttl_seconds:
            meta["expires_at"] = time.time() + ttl_seconds
        meta.update({"type": memory_type, "created_at": time.time(), "last_accessed": time.time()})
        col = self.col_episodic if memory_type == "episodic" else self.col_semantic
        if col:
            col.add(ids=[fact_id], documents=[content], metadatas=[meta])
        else:
            self._f.append({"id": fact_id, "document": content, "metadata": meta})
        return fact_id
    def prune_expired(self) -> None:
        if time.time() - self._last_prune < 300:
            return
        self._last_prune = time.time()
        for col in [self.col_episodic, self.col_semantic]:
            if col:
                res = col.get()
                ids_to_delete = [i for i, m in zip(res.get("ids", []), res.get("metadatas", [])) if self._is_expired(m)]
                if ids_to_delete:
                    col.delete(ids=ids_to_delete)
            else:
                self._f = [m for m in self._f if not self._is_expired(m.get("metadata", {}))]
    def get_fact(self, fact_id: str) -> dict | None:
        for col in (self.col_episodic, self.col_semantic):
            if not col:
                continue
            res = col.get(ids=[fact_id])
            if res and res.get("ids") and not self._is_expired(res["metadatas"][0]):
                col.update(ids=[fact_id], metadatas=[{"last_accessed": time.time()}])
                return {"id": res["ids"][0], "content": res["documents"][0], "metadata": res["metadatas"][0]}
        for m in self._f:
            if m["id"] == fact_id and not self._is_expired(m.get("metadata", {})):
                return {"id": m["id"], "content": m["document"], "metadata": m.get("metadata", {})}
        return None
    def delete_fact(self, fact_id: str) -> bool:
        for col in [self.col_episodic, self.col_semantic]:
            if col:
                col.delete(ids=[fact_id])
                return True
        self._f = [m for m in self._f if m["id"] != fact_id]
        return True
    def _get_bm25(self, docs: list[str]) -> Any:
        key = hashlib.md5("".join(docs).encode()).hexdigest()
        if key == self._bm25_cache[0]:
            return self._bm25_cache[1]
        bm25 = BM25Okapi([d.lower().split() for d in docs])
        self._bm25_cache = (key, bm25)
        return bm25
    def search(self, query: str, k: int = 3, memory_type: str = "all") -> list[dict]:
        self.prune_expired()
        cols_to_search = [col for col in [self.col_episodic, self.col_semantic]
                          if col and (memory_type == "all" or (col.name == f"{memory_type}_memory"))]
        vector_results = []
        for col in cols_to_search:
            r = col.query(query_texts=[query], n_results=k * 5)
            for i, d, m in zip(r.get("ids", [[]])[0], r.get("documents", [[]])[0], r.get("metadatas", [[]])[0]):
                vector_results.append({"id": i, "content": d, "metadata": m, "score": 1.0, "type": m.get("type", "episodic")})
        if not vector_results and not cols_to_search:
            lowered = query.lower()
            vector_results = [{"id": m["id"], "content": m["document"], "metadata": m.get("metadata", {}),
                               "score": 0.5, "type": m.get("metadata", {}).get("type", "episodic")}
                              for m in self._f if lowered in m["document"].lower()][:k * 5]
        if not vector_results:
            return []
        if _BM25 and vector_results:
            bm25 = self._get_bm25([r["content"] for r in vector_results])
            bm25_scores = bm25.get_scores(query.lower().split())
            top_score = max(bm25_scores) if bm25_scores else 0.0
            max_bm25 = top_score if top_score > 0 else 1.0
            for i, score in enumerate(bm25_scores):
                vector_results[i]["score"] += (score / max_bm25)
        for r in vector_results:
            if r["type"] == "semantic":
                r["score"] += 0.5
        vector_results.sort(key=lambda x: x["score"], reverse=True)
        for r in vector_results[:k]:
            col = self.col_episodic if r["type"] == "episodic" else self.col_semantic
            if col:
                col.update(ids=[r["id"]], metadatas=[{"last_accessed": time.time()}])
        return [{"id": r["id"], "content": r["content"]} for r in vector_results[:k]]
    def list_all(self, memory_type: str = "all") -> list[dict]:
        self.prune_expired()
        results = []
        cols_to_search = [col for col in [self.col_episodic, self.col_semantic]
                          if col and (memory_type == "all" or (col.name == f"{memory_type}_memory"))]
        for col in cols_to_search:
            r = col.get()
            results.extend([{"id": i, "content": d, "metadata": m}
                            for i, d, m in zip(r.get("ids", []), r.get("documents", []), r.get("metadatas", []))])
        if not cols_to_search:
            results = [{"id": m["id"], "content": m["document"], "metadata": m.get("metadata", {})} for m in self._f]
        return results
    def export_data(self) -> list[dict]:
        return self.list_all()
    def import_data(self, data: list[dict]) -> None:
        for item in data:
            m_type = item.get("metadata", {}).get("type", "episodic")
            self.add_fact(item.get("content", ""), item.get("metadata", {}), memory_type=m_type)
    def add_entity(self, name: str, entity_type: str = "entity", properties: dict | None = None) -> bool:
        if not self.graph:
            return False
        self.graph.add_node(name, type=entity_type, **(properties or {}))
        return True
    def add_relation(self, source: str, target: str, relation: str = "related_to",
                     properties: dict | None = None, timestamp: float | None = None) -> bool:
        if not self.graph:
            return False
        ts = timestamp or time.time()
        self.graph.add_edge(source, target, relation=relation, timestamp=ts, **(properties or {}))
        return True
    def clear_graph(self) -> None:
        if self.graph:
            self.graph.clear()
    def query_graph(self, entity: str, relation: str | None = None,
                    before_timestamp: float | None = None, after_timestamp: float | None = None) -> list[dict]:
        if not self.graph or entity not in self.graph:
            return []
        results = []
        for neighbor in nx.neighbors(self.graph, entity):
            edge_data = self.graph.get_edge_data(entity, neighbor)
            rel = edge_data.get("relation", "related_to")
            if relation and rel != relation:
                continue
            ts = edge_data.get("timestamp", 0)
            if before_timestamp and ts > before_timestamp:
                continue
            if after_timestamp and ts < after_timestamp:
                continue
            results.append({"entity": neighbor, "relation": rel, "properties": edge_data})
        return results
    def get_graph_data(self) -> dict:
        if not self.graph:
            return {"nodes": [], "edges": []}
        nodes = [{"id": n, "label": n, "group": d.get("type", "entity")} for n, d in self.graph.nodes(data=True)]
        edges = [{"from": u, "to": v, "label": d.get("relation", "related"), "arrows": "to"}
                 for u, v, d in self.graph.edges(data=True)]
        return {"nodes": nodes, "edges": edges}
    def archive_cold_memories(self, days_old: int = 7) -> int:
        if not self.col_episodic:
            return 0
        threshold = time.time() - (days_old * 86400)
        res = self.col_episodic.get()
        ids_to_archive = []
        cold_data = []
        for i, d, m in zip(res.get("ids", []), res.get("documents", []), res.get("metadatas", [])):
            if m.get("last_accessed", time.time()) < threshold:
                ids_to_archive.append(i)
                cold_data.append({"id": i, "document": d, "metadata": m})
        if not ids_to_archive:
            return 0
        existing_cold = []
        if self.cold_path.exists():
            with contextlib.suppress(json.JSONDecodeError, OSError):
                with gzip.open(self.cold_path, "rt", encoding="utf-8") as f:
                    existing_cold = json.loads(f.read())
        existing_cold.extend(cold_data)
        with gzip.open(self.cold_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps(existing_cold))
        self.col_episodic.delete(ids=ids_to_archive)
        return len(ids_to_archive)
    def thaw_memory(self, query: str) -> list[dict]:
        if not self.cold_path.exists():
            return []
        with contextlib.suppress(json.JSONDecodeError, OSError):
            with gzip.open(self.cold_path, "rt", encoding="utf-8") as f:
                cold_data = json.loads(f.read())
        thawed = []
        remaining = []
        lowered = query.lower()
        for item in cold_data:
            if lowered in item.get("document", "").lower():
                self.add_fact(item["document"], item.get("metadata", {}), memory_type="episodic")
                thawed.append(item)
            else:
                remaining.append(item)
        if thawed:
            with gzip.open(self.cold_path, "wt", encoding="utf-8") as f:
                f.write(json.dumps(remaining))
        return thawed
'''

    CORE_SESSION: str = (
        '"""Manages conversation history."""\n'
        'from __future__ import annotations\n'
        'from typing import Any\n'
        'from collections import deque\n'
        'from datetime import datetime\n'
        'import json, contextlib\n'
        'from pathlib import Path\n'
        '\n'
        'class Session:\n'
        '    def __init__(self, max_m: int = 50) -> None:\n'
        '        self.messages: deque[dict[str, Any]] = deque(maxlen=max_m)\n'
        '    def add_message(self, role: str, content: Any) -> None:\n'
        '        self.messages.append({"role": role, "content": content})\n'
        '    def get_messages(self) -> list[dict[str, Any]]:\n'
        '        return list(self.messages)\n'
        '    def clear(self) -> None:\n'
        '        self.messages.clear()\n'
        '    def undo_last_turn(self) -> bool:\n'
        '        """Remove the last complete turn (user prompt + all assistant/tool messages)."""\n'
        '        if not self.messages:\n'
        '            return False\n'
        '        removed_user = False\n'
        '        while self.messages:\n'
        '            msg = self.messages.pop()\n'
        '            if msg.get("role") == "user":\n'
        '                content = msg.get("content")\n'
        '                if isinstance(content, list) and all(\n'
        '                    isinstance(b, dict) and b.get("type") == "tool_result"\n'
        '                    for b in content\n'
        '                ):\n'
        '                    continue\n'
        '                removed_user = True\n'
        '                break\n'
        '        return removed_user\n'
        '    def save(self, path: str | Path = "session_backup.json") -> bool:\n'
        '        p = Path(path)\n'
        '        try:\n'
        '            p.write_text(json.dumps(list(self.messages), indent=2))\n'
        '            return True\n'
        '        except IOError:\n'
        '            return False\n'
        '    def load(self, path: str | Path = "session_backup.json") -> bool:\n'
        '        p = Path(path)\n'
        '        if not p.exists():\n'
        '            return False\n'
        '        with contextlib.suppress(json.JSONDecodeError, IOError):\n'
        '            data = json.loads(p.read_text())\n'
        '            if isinstance(data, list):\n'
        '                self.messages.clear()\n'
        '                self.messages.extend(data)\n'
        '                return True\n'
        '        return False\n'
        '    def export_markdown(self, path: str | Path | None = None) -> str:\n'
        '        """Export conversation as markdown."""\n'
        '        lines = ["# Synaptiq Conversation Export\\n"]\n'
        '        lines.append(f"*Exported: {datetime.now().isoformat()}*\\n")\n'
        '        for msg in self.messages:\n'
        '            role = msg.get("role", "unknown").upper()\n'
        '            content = msg.get("content", "")\n'
        '            if isinstance(content, list):\n'
        '                text_parts = []\n'
        '                for block in content:\n'
        '                    if isinstance(block, dict):\n'
        '                        if block.get("type") == "text":\n'
        '                            text_parts.append(block.get("text", ""))\n'
        '                        elif block.get("type") == "tool_result":\n'
        '                            text_parts.append("[Tool Result: " + str(block.get("content", ""))[:100] + "...]")\n'
        '                    else:\n'
        '                        text_parts.append(str(block))\n'
        '                content = " ".join(text_parts)\n'
        '            lines.append(f"## {role}\\n\\n{content}\\n")\n'
        '        md = "\\n".join(lines)\n'
        '        if path:\n'
        '            Path(path).write_text(md)\n'
        '        return md\n'
        '    def export_json(self, path: str | Path | None = None) -> str:\n'
        '        """Export conversation as JSON."""\n'
        '        data = json.dumps(list(self.messages), indent=2)\n'
        '        if path:\n'
        '            Path(path).write_text(data)\n'
        '        return data\n'
    )

    CORE_ORCHESTRATOR: str = '''"""Multi-agent orchestration."""
from __future__ import annotations
import json, logging, os, asyncio, contextlib
from collections import deque
from pathlib import Path
from typing import Any
logger = logging.getLogger(__name__)

class SubAgent:
    def __init__(self, cfg: dict, parent: Any, agents_dir: Path) -> None:
        self.name = cfg.get("name", "sub").lower()
        self.sp = cfg.get("system_prompt", "Helper.")
        self.prov = cfg.get("llm_provider", parent.config.get("default_provider", "anthropic"))
        self.mod = cfg.get("llm_model", parent.config.get("default_model", "claude-3-5-sonnet-20241022"))
        env = "ANTHROPIC_API_KEY" if self.prov == "anthropic" else "OPENAI_API_KEY"
        self._api_key = os.getenv(cfg.get("api_key_env", env))
        self.llm = parent.llm
        self.reg = parent.reg
        self.global_mem = parent.global_mem
        self.sess: deque[dict] = deque(maxlen=50)
        self._initialized = False
    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        if not self._api_key:
            raise ValueError(f"Missing API key for agent '{self.name}'")
        self._initialized = True
    async def execute(self, task: str, max_it: int = 10) -> str:
        self._ensure_initialized()
        self.sess.append({"role": "user", "content": task})
        for _ in range(max_it):
            r = await self.llm.chat(list(self.sess), self.reg.get_schemas(), self.sp)
            mc = r.get("content", [])
            tc = r.get("tool_calls") or []
            txt = "".join(b.get("text", "") for b in mc if isinstance(b, dict) and b.get("type") == "text")
            if not tc:
                self.sess.append({"role": "assistant", "content": mc})
                return txt or "[No resp]"
            self.sess.append({"role": "assistant", "content": mc})
            for c in tc:
                res = await self.reg.execute(c.get("name", ""), c.get("input", {}))
                self.sess.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": c.get("id"), "content": json.dumps(res)}]})
        return "Max tools."

class Orchestrator:
    def __init__(self, parent: Any, agents_dir: Path | None = None) -> None:
        self.parent = parent
        self.agents: dict[str, SubAgent] = {}
        self.agents_dir = agents_dir or (Path.cwd() / "synaptiq" / "agents")
        self._load()
    def _load(self) -> None:
        if not self.agents_dir.exists():
            return
        for p in self.agents_dir.glob("*.json"):
            try:
                cfg = json.loads(p.read_text())
                cfg["name"] = cfg.get("name", p.stem).lower()
                self.agents[cfg["name"]] = SubAgent(cfg, self.parent, self.agents_dir)
            except Exception as e:
                logger.warning(f"Failed to load agent from {p}: {e}")
    def list_agents(self) -> list[str]:
        return list(self.agents.keys())
    def get_agent_config(self, name: str) -> dict | None:
        p = self.agents_dir / f"{name}.json"
        if p.exists():
            with contextlib.suppress(json.JSONDecodeError):
                return json.loads(p.read_text())
        return None
    async def delegate(self, agent_name: str, task: str, _depth: int = 0) -> str:
        if _depth > 3:
            return "Error: Max A2A delegation depth reached."
        a = self.agents.get(agent_name.lower())
        if not a:
            return f"Err: Agent {agent_name} not found."
        return await a.execute(task)
    async def delegate_parallel(self, tasks: list[dict[str, str]]) -> dict[str, str]:
        coros = []
        valid_tasks = []
        for t in tasks:
            agent_name = t.get("agent_name", "").lower()
            task_desc = t.get("task_description", "")
            if agent_name in self.agents and task_desc:
                coros.append(self.delegate(agent_name, task_desc, _depth=1))
                valid_tasks.append(agent_name)
        results = await asyncio.gather(*coros, return_exceptions=True)
        return {name: str(res) for name, res in zip(valid_tasks, results)}
    def spawn_agent(self, config: dict) -> str:
        name = config.get("name", "dynamic_agent").lower()
        if name in self.agents:
            return f"Agent '{name}' already exists."
        with contextlib.suppress(Exception):
            p = self.agents_dir / f"{name}.json"
            p.write_text(json.dumps(config, indent=2))
            self.agents[name] = SubAgent(config, self.parent, self.agents_dir)
            return f"Successfully spawned agent '{name}'."
        return "Failed to spawn agent."
    def delete_agent(self, name: str) -> bool:
        name = name.lower()
        if name not in self.agents:
            return False
        p = self.agents_dir / f"{name}.json"
        if p.exists():
            p.unlink()
        del self.agents[name]
        return True
'''

    CORE_REGISTRY: str = '''"""Dynamic tool registry with circuit breaker and per-tool timeout/retry."""
from __future__ import annotations
import importlib, pkgutil, logging, json, contextlib, asyncio, time
from collections import OrderedDict
from typing import Any
logger = logging.getLogger(__name__)

try:
    from rank_bm25 import BM25Okapi
    _BM25 = True
except ImportError:
    _BM25 = False

def _parse_version(v: str) -> tuple[int, ...]:
    parts = tuple(int(x) for x in str(v).split(".") if x.isdigit())
    return parts + (0,) * (3 - len(parts)) if len(parts) < 3 else parts

class ToolRegistry:
    def __init__(self, bus: Any) -> None:
        self.bus = bus
        self.tools: dict[str, Any] = {}
        self.mock_data: dict[str, Any] = {}
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self._cache_maxsize = 256
        self._bm25_cache: tuple[str, Any] = ("", None)
        self._circuit_breaker: dict[str, dict] = {}
        self._breaker_threshold = 5
        self._breaker_reset_time = 60.0
    def register(self, cls: type) -> None:
        try:
            i = cls(self.bus)
            version = getattr(i, "version", "1")
            self.tools[f"{i.name}_v{version}"] = i
            existing = self.tools.get(i.name)
            if not existing or _parse_version(version) > _parse_version(getattr(existing, "version", "1")):
                self.tools[i.name] = i
        except Exception as e:
            logger.warning(f"Failed to register tool {cls.__name__}: {e}")
    def auto_discover(self) -> None:
        with contextlib.suppress(ImportError, AttributeError):
            from ..tools import base as bm
            import synaptiq.tools as tp
            for _, m, _ in pkgutil.iter_modules(tp.__path__):
                if m == "base":
                    continue
                with contextlib.suppress(Exception):
                    mod = importlib.import_module(f"synaptiq.tools.{m}")
                    for a in dir(mod):
                        at = getattr(mod, a)
                        if isinstance(at, type) and issubclass(at, bm.BaseTool) and at is not bm.BaseTool:
                            self.register(at)
    def _get_bm25(self, docs: list[str]) -> Any:
        import hashlib
        key = hashlib.md5("".join(docs).encode()).hexdigest()
        if key == self._bm25_cache[0]:
            return self._bm25_cache[1]
        bm25 = BM25Okapi([d.lower().split() for d in docs])
        self._bm25_cache = (key, bm25)
        return bm25
    def get_schemas(self, query: str | None = None, top_k: int = 7) -> list[dict[str, Any]]:
        all_schemas = []
        for name, t in self.tools.items():
            if "_v" not in name:
                sc = {"name": t.name, "description": t.description}
                sc["input_schema"] = (
                    t.args_model.model_json_schema()
                    if hasattr(t, "args_model")
                    else {"type": "object", "properties": {}}
                )
                sc["_doc"] = f"{t.name} {t.description}"
                all_schemas.append(sc)
        if query and _BM25 and len(all_schemas) > top_k:
            bm25 = self._get_bm25([s["_doc"] for s in all_schemas])
            scores = bm25.get_scores(query.lower().split())
            scored_schemas = list(zip(scores, all_schemas))
            scored_schemas.sort(key=lambda x: x[0], reverse=True)
            return [{k: v for k, v in s.items() if k != "_doc"} for _, s in scored_schemas[:top_k]]
        return [{k: v for k, v in s.items() if k != "_doc"} for s in all_schemas]
    def _check_circuit(self, name: str) -> bool:
        state = self._circuit_breaker.get(name)
        if not state:
            return True
        if state.get("tripped"):
            if time.time() - state.get("last_failure", 0) > self._breaker_reset_time:
                state["tripped"] = False
                state["failures"] = 0
                return True
            return False
        return True
    def _record_failure(self, name: str) -> None:
        state = self._circuit_breaker.setdefault(name, {"failures": 0, "last_failure": 0, "tripped": False})
        state["failures"] += 1
        state["last_failure"] = time.time()
        if state["failures"] >= self._breaker_threshold:
            state["tripped"] = True
            logger.warning(f"Circuit breaker tripped for tool '{name}' after {state['failures']} failures")
    def _record_success(self, name: str) -> None:
        if name in self._circuit_breaker:
            self._circuit_breaker[name]["failures"] = 0
            self._circuit_breaker[name]["tripped"] = False
    def clear_cache(self) -> int:
        count = len(self.cache)
        self.cache.clear()
        return count
    def get_cache_stats(self) -> dict[str, Any]:
        return {"entries": len(self.cache), "max_size": self._cache_maxsize}
    def get_circuit_status(self) -> dict[str, dict]:
        return dict(self._circuit_breaker)
    async def execute(self, name: str, args: dict) -> dict:
        t = self.tools.get(name)
        if not t:
            return {"error": "Tool not found"}
        
        cacheable = getattr(t, "cacheable", True)
        if cacheable:
            cache_key = f"{name}:{json.dumps(args, sort_keys=True)}"
            if cache_key in self.cache:
                self.cache.move_to_end(cache_key)
                return self.cache[cache_key]
                
        if name in self.mock_data:
            return self.mock_data[name]
            
        if not self._check_circuit(name):
            return {"error": f"Tool '{name}' circuit breaker is tripped. Try again later."}
            
        timeout = getattr(t, "timeout", 30)
        retries = getattr(t, "retries", 0)
        retry_delay = getattr(t, "retry_delay", 1.0)
        last_error = None
        
        for attempt in range(retries + 1):
            try:
                res = await asyncio.wait_for(t.execute(**args), timeout=timeout)
                if cacheable:
                    self.cache[cache_key] = res
                    if len(self.cache) > self._cache_maxsize:
                        self.cache.popitem(last=False)
                self._record_success(name)
                return res
            except asyncio.TimeoutError:
                last_error = {"error": f"Tool '{name}' timed out after {timeout}s", "type": "timeout"}
                logger.warning(last_error["error"])
            except Exception as e:
                if hasattr(e, 'errors'):
                    last_error = {"error": "Validation failed", "details": e.errors(), "type": "validation"}
                else:
                    last_error = {"error": str(e), "type": "exception"}
                logger.warning(f"Tool '{name}' attempt {attempt+1} failed: {e}")
            if attempt < retries:
                await asyncio.sleep(retry_delay * (attempt + 1))
                
        self._record_failure(name)
        return last_error if last_error else {"error": "Unknown error"}
'''

    CORE_AGENT: str = '''"""The main cognitive agent."""
from __future__ import annotations
import json, asyncio, logging, re, time, contextlib, base64
from pathlib import Path
from typing import Any, AsyncGenerator
from collections import deque
from .event_bus import EventBus
from .memory import MemoryStore
from .orchestrator import Orchestrator
from .provider import LLMProvider, get_provider, CascadeProvider
from .registry import ToolRegistry
from .session import Session
from .state_manager import StateManager
logger = logging.getLogger(__name__)

_tiktoken_enc = None

class Agent:
    def __init__(self, api_key: str, provider: str = "anthropic", model: str | None = None,
                 config_path: str = "./synaptiq.json", budget_usd: float | None = None,
                 mock_tools: dict | None = None, summarize: bool = False,
                 mcp_servers: dict | None = None, json_schema: dict | None = None,
                 fallback_model: str | None = None, fallback_provider: str | None = None,
                 approve_tools: bool = False, plan_mode: bool = False, smart_routing: bool = False,
                 stream_mode: bool = False) -> None:
        self.config = self._load_cfg(config_path)
        self.version = "5.1.0"
        provider = provider or self.config.get("default_provider", "anthropic")
        model = model or self.config.get("default_model", "claude-3-5-sonnet-20241022")
        self.llm = get_provider(provider, api_key, model)
        self.model_name = model
        self.provider_name = provider
        self.budget = budget_usd
        self.summarize = summarize
        self.json_schema = json_schema
        self.approve_tools = approve_tools
        self.plan_mode = plan_mode
        self.smart_routing = smart_routing
        self.stream_mode = stream_mode
        self.max_tokens_per_turn = int(self.config.get("max_tokens_per_turn", 4096))
        self.max_tool_iterations = int(self.config.get("max_tool_iterations", 10))
        self.state_manager = StateManager(config_path)
        self.fallback_model = fallback_model
        self.fallback_provider = fallback_provider
        self._fallback_llm = None
        self._api_key = api_key
        if self.smart_routing:
            rules = self.config.get("routing_rules", [])
            if rules:
                self.llm = CascadeProvider(self.llm, api_key, rules)
        self.bus = EventBus()
        self.bus.context["state_manager"] = self.state_manager
        self.mem = MemoryStore()
        self.bus.context["memory_store"] = self.mem
        global_db_path = Path.home() / ".synaptiq" / "global_db"
        self.global_mem = MemoryStore(db_path=str(global_db_path))
        self.bus.context["global_memory"] = self.global_mem
        self.reg = ToolRegistry(self.bus)
        if mock_tools:
            self.reg.mock_data = mock_tools
        self.sess = Session()
        self.reg.auto_discover()
        self.called_tools: set[str] = set()
        agents_dir = Path(config_path).resolve().parent / "synaptiq" / "agents"
        self.orch = Orchestrator(self, agents_dir=agents_dir)
        self.bus.context["orchestrator"] = self.orch
        self.bus.context["parent_agent"] = self
        self.usage = {"input_tokens": 0, "output_tokens": 0, "tool_calls": 0}
        self.trace: list[dict] = []
        self._consolidator_task = None
        self._pending_image: dict | None = None
        self._pending_file: str | None = None
        
        from .pricing import estimate_cost
        self._estimate_cost = estimate_cost

    def _load_cfg(self, p: str | Path) -> dict:
        p = Path(p)
        if p.exists() and p.is_file():
            with contextlib.suppress(json.JSONDecodeError, OSError):
                return json.loads(p.read_text())
        return {}
    def get_system_prompt(self) -> str:
        prompt = f"You are Synaptiq v{self.version}, an autonomous coding, reasoning, and document processing agent."
        prompt = self.state_manager.inject_into_prompt(prompt)
        if self.json_schema:
            prompt += f"\\n\\nIMPORTANT: You must respond with ONLY a valid JSON object that strictly matches this schema: {json.dumps(self.json_schema)}."
        return prompt
    def set_model(self, model: str) -> None:
        self.model_name = model
        self.llm = get_provider(self.provider_name, self._api_key, model)
        if self.smart_routing:
            rules = self.config.get("routing_rules", [])
            if rules:
                self.llm = CascadeProvider(self.llm, self._api_key, rules)
    def set_provider(self, provider: str, model: str | None = None) -> None:
        self.provider_name = provider
        self.model_name = model or self.model_name
        self.llm = get_provider(provider, self._api_key, self.model_name)
        if self.smart_routing:
            rules = self.config.get("routing_rules", [])
            if rules:
                self.llm = CascadeProvider(self.llm, self._api_key, rules)
    def set_image(self, image_path: str) -> bool:
        p = Path(image_path)
        if not p.exists():
            return False
        ext = p.suffix.lower()
        media_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp"}
        media_type = media_map.get(ext, "image/jpeg")
        data = base64.b64encode(p.read_bytes()).decode("utf-8")
        self._pending_image = {"media_type": media_type, "data": data}
        return True
    def set_file(self, file_path: str) -> bool:
        p = Path(file_path)
        if not p.exists():
            return False
        self._pending_file = p.read_text(encoding="utf-8", errors="replace")
        return True
    def _track_usage(self, r: dict) -> None:
        u = r.get("usage", {})
        self.usage["input_tokens"] += u.get("input_tokens", 0)
        self.usage["output_tokens"] += u.get("output_tokens", 0)
        self.usage["tool_calls"] += len(r.get("tool_calls") or [])
    def cost(self) -> float:
        return self._estimate_cost(self.model_name, self.usage["input_tokens"], self.usage["output_tokens"])
    async def _compress_context_if_needed(self) -> None:
        with contextlib.suppress(Exception):
            global _tiktoken_enc
            if _tiktoken_enc is None:
                import tiktoken
                _tiktoken_enc = tiktoken.get_encoding("cl100k_base")
            msgs = list(self.sess.messages)
            total_tokens = sum(len(_tiktoken_enc.encode(json.dumps(m))) for m in msgs)
            if total_tokens > 50000 and len(msgs) > 10:
                old_msgs = msgs[1:10]
                summary_prompt = "Summarize this conversation context concisely:\\n\\n" + json.dumps(old_msgs)
                resp = await self.llm.chat([{"role": "user", "content": summary_prompt}], None, "Summarize concisely.", self.max_tokens_per_turn)
                summary = "".join(b.get("text", "") for b in resp.get("content", []) if isinstance(b, dict) and b.get("type") == "text")
                maxlen = self.sess.messages.maxlen
                self.sess.messages.clear()
                self.sess.messages.append(msgs[0])
                self.sess.messages.append({"role": "system", "content": f"Episodic Memory:\\n{summary}"})
                self.sess.messages.extend(msgs[10:])
    async def _chat_with_fallback(self, target_llm: LLMProvider | None = None) -> dict:
        llm_to_use = target_llm if target_llm else self.llm
        try:
            return await llm_to_use.chat(self.sess.get_messages(), self.reg.get_schemas(), self.get_system_prompt(), self.max_tokens_per_turn)
        except Exception:
            if self.fallback_model and not target_llm:
                if not self._fallback_llm:
                    self._fallback_llm = get_provider(self.fallback_provider or "anthropic", self._api_key, self.fallback_model)
                return await self._fallback_llm.chat(self.sess.get_messages(), self.reg.get_schemas(), self.get_system_prompt(), self.max_tokens_per_turn)
            raise
    async def _execute_tool_loop(self, user_input: str, image: dict | None = None) -> AsyncGenerator[str, None]:
        content = [{"type": "text", "text": user_input}]
        if image:
            content.append({"type": "image", "source": {"type": "base64", "media_type": image.get("media_type", "image/jpeg"), "data": image["data"]}})
        self.sess.add_message("user", content if len(content) > 1 else user_input)
        self.trace.append({"event": "user_input", "data": user_input, "t": time.time()})
        consecutive_failures = 0
        for _ in range(self.max_tool_iterations):
            if self.budget and self.cost() > self.budget:
                yield "[Budget exceeded]"
                return
            await self._compress_context_if_needed()
            r = await self._chat_with_fallback(self.llm)
            self._track_usage(r)
            content_blocks = r.get("content", [])
            tc = r.get("tool_calls") or []
            t = "".join(b.get("text", "") for b in content_blocks if isinstance(b, dict) and b.get("type") == "text")
            if not tc:
                self.sess.add_message("assistant", content_blocks)
                self.trace.append({"event": "final_answer", "data": t, "t": time.time()})
                if self.json_schema:
                    with contextlib.suppress(json.JSONDecodeError):
                        t = re.sub(r'^```json\\s*', '', t.strip(), flags=re.MULTILINE)
                        t = re.sub(r'\\s*```$', '', t.strip())
                        t = json.dumps(json.loads(t))
                yield t or "[No resp]"
                return
            self.sess.add_message("assistant", content_blocks)
            tasks = []
            for call in tc:
                tool_name = call.get("name", "")
                tool_inst = self.reg.tools.get(tool_name)
                if tool_inst and hasattr(tool_inst, 'requires') and tool_inst.requires:
                    missing = [req for req in tool_inst.requires if req not in self.called_tools]
                    if missing:
                        async def _missing_dep_error(m=missing):
                            return {"error": f"Missing dependencies: {m}."}
                        tasks.append(_missing_dep_error())
                        continue
                if self.approve_tools and tool_inst and getattr(tool_inst, "requires_approval", False):
                    console = self.bus.context.get("console")
                    if console:
                        allowed = await asyncio.to_thread(
                            console.confirm,
                            f"Agent wants to run '{tool_name}' with args: {json.dumps(call.get('input', {}))[:200]}. Allow?",
                            False,
                        )
                    else:
                        allowed = False
                    if allowed:
                        tasks.append(self.reg.execute(tool_name, call.get("input", {})))
                        self.called_tools.add(tool_name)
                    else:
                        async def _denied():
                            return {"error": "Tool execution denied by user.", "type": "denied"}
                        tasks.append(_denied())
                else:
                    tasks.append(self.reg.execute(tool_name, call.get("input", {})))
                    self.called_tools.add(tool_name)
            results = await asyncio.gather(*tasks)
            for call, res in zip(tc, results):
                self.trace.append({"event": "tool_call", "tool": call.get("name"), "input": call.get("input", {}), "t": time.time()})
                self.trace.append({"event": "tool_result", "tool": call.get("name"), "result": res, "t": time.time()})
                res_str = json.dumps(res, default=str)
                self.sess.add_message("user", [{"type": "tool_result", "tool_use_id": call.get("id"), "content": res_str}])
                if isinstance(res, dict) and res.get("error"):
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        reflect_prompt = "You have failed 3 tool calls in a row. Analyze why you are failing and explicitly write out a new strategy."
                        self.sess.add_message("user", reflect_prompt)
                        reflect_resp = await self.llm.chat(self.sess.get_messages(), None, "You are a reflective AI.", self.max_tokens_per_turn)
                        reflect_text = "".join(b.get("text", "") for b in reflect_resp.get("content", []) if isinstance(b, dict) and b.get("type") == "text")
                        self.sess.add_message("assistant", reflect_text)
                        consecutive_failures = 0
                        yield f"\\n[Reflection] {reflect_text}\\n"
                else:
                    consecutive_failures = 0
        yield "[Max iters]"
    async def _stream_response(self, user_input: str, image: dict | None = None) -> AsyncGenerator[str, None]:
        content = [{"type": "text", "text": user_input}]
        if image:
            content.append({"type": "image", "source": {"type": "base64", "media_type": image.get("media_type", "image/jpeg"), "data": image["data"]}})
        self.sess.add_message("user", content if len(content) > 1 else user_input)
        self.trace.append({"event": "user_input", "data": user_input, "t": time.time()})
        full_response = []
        try:
            async for chunk in self.llm.stream(self.sess.get_messages(), None, self.get_system_prompt(), self.max_tokens_per_turn):
                full_response.append(chunk)
                yield chunk
        except Exception as e:
            yield f"\\n[Stream error: {e}]"
        resp_text = "".join(full_response)
        self.sess.add_message("assistant", [{"type": "text", "text": resp_text}])
        self.trace.append({"event": "final_answer", "data": resp_text, "t": time.time()})
        try:
            global _tiktoken_enc
            if _tiktoken_enc is None:
                import tiktoken
                _tiktoken_enc = tiktoken.get_encoding("cl100k_base")
            self.usage["input_tokens"] += len(_tiktoken_enc.encode(user_input))
            self.usage["output_tokens"] += len(_tiktoken_enc.encode(resp_text))
        except Exception:
            self.usage["input_tokens"] += len(user_input) // 4
            self.usage["output_tokens"] += len(resp_text) // 4
    async def _episodic_consolidator_loop(self) -> None:
        await asyncio.sleep(60)
        while True:
            with contextlib.suppress(Exception):
                if self.mem.col_episodic and self.mem.col_episodic.count() > 50:
                    res = self.mem.col_episodic.get(limit=5)
                    if res and res.get("ids"):
                        prompt = "Extract hard facts (Semantic memory) from these Episodic logs. Output a JSON list of strings (facts).\\n\\n" + json.dumps([d for d in res.get("documents", [])])
                        resp = await self.llm.chat([{"role": "user", "content": prompt}], None, "Output ONLY a JSON list of strings.", self.max_tokens_per_turn)
                        text = "".join(b.get("text", "") for b in resp.get("content", []) if isinstance(b, dict) and b.get("type") == "text")
                        with contextlib.suppress(json.JSONDecodeError):
                            start = text.find("[")
                            end = text.rfind("]") + 1
                            if start != -1 and end != -1:
                                facts = json.loads(text[start:end])
                                for fact in facts:
                                    self.mem.add_fact(fact, memory_type="semantic")
                                self.mem.col_episodic.delete(ids=res.get("ids", []))
            await asyncio.sleep(300)
    async def think(self, user_input: str, image: dict | None = None) -> AsyncGenerator[str, None]:
        if self._consolidator_task is None or self._consolidator_task.done():
            self._consolidator_task = asyncio.create_task(self._episodic_consolidator_loop())
        if self._pending_image:
            image = self._pending_image
            self._pending_image = None
        if self._pending_file:
            user_input = f"{user_input}\\n\\n[File Context]\\n{self._pending_file}"
            self._pending_file = None
        if self.summarize and len(self.sess.messages) > 20:
            old_msgs = list(self.sess.messages)[:15]
            self.sess.messages = deque(list(self.sess.messages)[15:], maxlen=self.sess.messages.maxlen)
            summary_resp = await self.llm.chat([{"role": "user", "content": "Summarize this: " + json.dumps(old_msgs)}], None, "Summarize briefly.", self.max_tokens_per_turn)
            summary_text = "".join(b.get("text", "") for b in summary_resp.get("content", []) if isinstance(b, dict) and b.get("type") == "text")
            self.sess.messages.insert(0, {"role": "system", "content": f"Previous context summary: {summary_text}"})
        if self.stream_mode and not self.plan_mode:
            async for chunk in self._stream_response(user_input, image):
                yield chunk
            return
        if self.plan_mode:
            plan_prompt = f"Analyze the following request and create a step-by-step plan. Output ONLY a JSON list of strings.\\n\\nRequest: {user_input}"
            self.sess.add_message("user", plan_prompt)
            self.trace.append({"event": "user_input", "data": plan_prompt, "t": time.time()})
            await self._compress_context_if_needed()
            r = await self._chat_with_fallback()
            self._track_usage(r)
            c = r.get("content", [])
            plan_text = "".join(b.get("text", "") for b in c if isinstance(b, dict) and b.get("type") == "text")
            with contextlib.suppress(json.JSONDecodeError, ValueError):
                plan_text = re.sub(r'^```json\\s*', '', plan_text.strip(), flags=re.MULTILINE)
                plan_text = re.sub(r'\\s*```$', '', plan_text.strip())
                steps = json.loads(plan_text)
                if not isinstance(steps, list):
                    raise ValueError("Plan is not a list")
                self.sess.add_message("assistant", c)
                self.trace.append({"event": "final_answer", "data": plan_text, "t": time.time()})
                yield "## Execution Plan:\\n"
                for i, step in enumerate(steps):
                    yield f"{i+1}. {step}\\n"
                yield "\\n**Executing plan...**\\n\\n"
                for i, step in enumerate(steps):
                    yield f"### Step {i+1}: {step}\\n"
                    step_prompt = f"Execute step {i+1} of the plan: {step}. Use tools if necessary."
                    async for chunk in self._execute_tool_loop(step_prompt):
                        yield chunk
                    yield "\\n"
                return
            self.sess.add_message("assistant", c)
            yield "Failed to generate structured plan. Executing normally...\\n\\n"
        async for chunk in self._execute_tool_loop(user_input, image):
            yield chunk
    async def aclose(self) -> None:
        if self._consolidator_task and not self._consolidator_task.done():
            self._consolidator_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consolidator_task
'''

    CORE_PRICING: str = (
        '"""Token cost estimation."""\n'
        'MODEL_PRICING = {"claude-3-5-sonnet-20241022": {"input": 3.0, "output": 15.0}, "gpt-4o": {"input": 2.5, "output": 10.0}, "gpt-4o-mini": {"input": 0.15, "output": 0.6}, "claude-3-5-haiku-20241022": {"input": 0.8, "output": 4.0}, "mock": {"input": 0.0, "output": 0.0}}\n'
        'def estimate_cost(m: str, i: int, o: int) -> float:\n'
        '    r = MODEL_PRICING.get(m, {"input": 1.0, "output": 2.0})\n'
        '    return (i * r["input"] + o * r["output"]) / 1_000_000\n'
    )

    TOOLS_BASE: str = (
        '"""Base tool abstract class with timeout and retry support."""\n'
        'from __future__ import annotations\n'
        'import logging\n'
        'from typing import Any, ClassVar, Type\n'
        'from pydantic import BaseModel\n'
        'logger = logging.getLogger(__name__)\n'
        '\n'
        'class BaseTool:\n'
        '    name: ClassVar[str] = "base"\n'
        '    description: ClassVar[str] = ""\n'
        '    args_model: ClassVar[Type[BaseModel]] = BaseModel\n'
        '    requires_approval: ClassVar[bool] = False\n'
        '    version: ClassVar[str] = "1"\n'
        '    cacheable: ClassVar[bool] = True\n'
        '    requires: ClassVar[tuple[str, ...]] = ()\n'
        '    timeout: ClassVar[int] = 30\n'
        '    retries: ClassVar[int] = 0\n'
        '    retry_delay: ClassVar[float] = 1.0\n'
        '    def __init__(self, bus: Any) -> None:\n'
        '        self.bus = bus\n'
        '    async def execute(self, **kw: Any) -> dict:\n'
        '        v = self.args_model(**kw)\n'
        '        return await self._run(**v.model_dump())\n'
        '    async def _run(self, **kw: Any) -> dict:\n'
        '        raise NotImplementedError\n'
    )

    TOOL_STUB: str = (
        '"""Tool: {name}"""\n'
        'from __future__ import annotations\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class {cn}Schema(BaseModel):\n'
        '    input_string: str = Field(...)\n'
        '\n'
        'class {cn}Tool(BaseTool):\n'
        '    name = "{name}"\n'
        '    description = "{desc}"\n'
        '    args_model = {cn}Schema\n'
        '    async def _run(self, **kw: Any) -> dict[str, Any]:\n'
        '        return {{"status": "success"}}\n'
    )

    TOOLS_MULTI_FILE_WRITE: str = (
        '"""Tool: multi_file_write"""\n'
        'from __future__ import annotations\n'
        'from pathlib import Path\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class MultiFileWriteSchema(BaseModel):\n'
        '    files: dict[str, str] = Field(...)\n'
        '\n'
        'class MultiFileWriteTool(BaseTool):\n'
        '    name = "multi_file_write"\n'
        '    description = "Write multiple files to the filesystem simultaneously."\n'
        '    requires_approval = True\n'
        '    version = "2"\n'
        '    timeout = 60\n'
        '    args_model = MultiFileWriteSchema\n'
        '    async def _run(self, files: dict[str, str]) -> dict[str, Any]:\n'
        '        results = []\n'
        '        base_dir = Path.cwd().resolve()\n'
        '        for path_str, content in files.items():\n'
        '            p = Path(path_str)\n'
        '            if not p.is_absolute():\n'
        '                p = (base_dir / p).resolve()\n'
        '            try:\n'
        '                p.relative_to(base_dir)\n'
        '            except ValueError:\n'
        '                results.append({"file_path": str(path_str), "status": "error", "error": "Path escapes project root"})\n'
        '                continue\n'
        '            try:\n'
        '                p.parent.mkdir(parents=True, exist_ok=True)\n'
        '                p.write_text(content, encoding="utf-8")\n'
        '                results.append({"file_path": str(p), "status": "written", "bytes": len(content)})\n'
        '            except Exception as e:\n'
        '                results.append({"file_path": str(p), "status": "error", "error": str(e)})\n'
        '        return {"status": "success", "details": results}\n'
    )

    TOOLS_EXECUTE_CODE: str = (
        '"""Tool: execute_code"""\n'
        'from __future__ import annotations\n'
        'import asyncio, sys, tempfile, os\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class ExecuteCodeSchema(BaseModel):\n'
        '    code: str = Field(...)\n'
        '    timeout: int = Field(10)\n'
        '\n'
        'class ExecuteCodeTool(BaseTool):\n'
        '    name = "execute_code"\n'
        '    description = "Execute Python code in a restricted subprocess."\n'
        '    requires_approval = True\n'
        '    cacheable = False\n'
        '    timeout = 120\n'
        '    args_model = ExecuteCodeSchema\n'
        '    async def _run(self, code: str, timeout: int = 10) -> dict[str, Any]:\n'
        '        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:\n'
        '            f.write(code)\n'
        '            temp_path = f.name\n'
        '        stdout_data, stderr_data = [], []\n'
        '        try:\n'
        '            proc = await asyncio.create_subprocess_exec(\n'
        '                sys.executable, temp_path,\n'
        '                stdin=asyncio.subprocess.DEVNULL,\n'
        '                stdout=asyncio.subprocess.PIPE,\n'
        '                stderr=asyncio.subprocess.PIPE\n'
        '            )\n'
        '            async def read_stream(stream, storage):\n'
        '                while True:\n'
        '                    line = await stream.readline()\n'
        '                    if not line:\n'
        '                        break\n'
        '                    decoded = line.decode("utf-8", errors="ignore")\n'
        '                    storage.append(decoded)\n'
        '                    await self.bus.publish("tool_stream", f"[stdout] {decoded}")\n'
        '            await asyncio.wait_for(\n'
        '                asyncio.gather(read_stream(proc.stdout, stdout_data), read_stream(proc.stderr, stderr_data)),\n'
        '                timeout=timeout\n'
        '            )\n'
        '            await proc.wait()\n'
        '            return {"stdout": "".join(stdout_data), "stderr": "".join(stderr_data), "returncode": proc.returncode}\n'
        '        except asyncio.TimeoutError:\n'
        '            if proc.returncode is None:\n'
        '                proc.kill()\n'
        '            return {"error": f"Execution timed out after {timeout}s"}\n'
        '        finally:\n'
        '            os.unlink(temp_path)\n'
    )

    TOOLS_WEB_SEARCH: str = (
        '"""Tool: web_search"""\n'
        'from __future__ import annotations\n'
        'import asyncio, urllib.request, urllib.parse, re\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class WebSearchSchema(BaseModel):\n'
        '    query: str = Field(...)\n'
        '\n'
        'class WebSearchTool(BaseTool):\n'
        '    name = "web_search"\n'
        '    description = "Search the web using DuckDuckGo."\n'
        '    cacheable = False\n'
        '    timeout = 15\n'
        '    retries = 2\n'
        '    retry_delay = 2.0\n'
        '    args_model = WebSearchSchema\n'
        '    async def _run(self, query: str) -> dict[str, Any]:\n'
        '        def _sync_search():\n'
        '            url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"\n'
        '            req = urllib.request.Request(url, headers={"User-Agent": "Synaptiq-Agent/1.0"})\n'
        '            with urllib.request.urlopen(req, timeout=10) as resp:\n'
        '                return resp.read().decode("utf-8", errors="ignore")\n'
        '        try:\n'
        '            html = await asyncio.to_thread(_sync_search)\n'
        '            results = []\n'
        '            links = re.findall(r\'<a rel="nofollow" class="result__a" href="([^"]+)">(.*?)</a>\', html)\n'
        '            snippets = re.findall(r\'<a class="result__snippet" href="[^"]+">(.*?)</a>\', html, re.DOTALL)\n'
        '            for i in range(min(len(links), 5)):\n'
        '                link = links[i][0]\n'
        '                if "uddg=" in link:\n'
        '                    link = urllib.parse.unquote(link.split("uddg=")[1].split("&")[0])\n'
        '                title = re.sub(r"<[^>]+>", "", links[i][1])\n'
        '                snippet = re.sub(r"<[^>]+>", "", snippets[i]) if i < len(snippets) else ""\n'
        '                results.append({"title": title, "link": link, "snippet": snippet})\n'
        '            return {"status": "success", "results": results}\n'
        '        except Exception as e:\n'
        '            return {"error": f"Search failed: {e}"}\n'
    )

    TOOLS_READ_FILE: str = (
        '"""Tool: read_file"""\n'
        'from __future__ import annotations\n'
        'from pathlib import Path\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class ReadFileSchema(BaseModel):\n'
        '    file_path: str = Field(...)\n'
        '    start_line: int = Field(0, ge=0)\n'
        '    end_line: int = Field(0, ge=0)\n'
        '\n'
        'class ReadFileTool(BaseTool):\n'
        '    name = "read_file"\n'
        '    description = "Read a file from the filesystem with optional line range."\n'
        '    cacheable = False\n'
        '    timeout = 10\n'
        '    args_model = ReadFileSchema\n'
        '    async def _run(self, file_path: str, start_line: int = 0, end_line: int = 0) -> dict[str, Any]:\n'
        '        p = Path(file_path)\n'
        '        if not p.is_absolute():\n'
        '            p = Path.cwd().resolve() / p\n'
        '        if not p.exists():\n'
        '            return {"error": f"File not found: {file_path}"}\n'
        '        if not p.is_file():\n'
        '            return {"error": f"Not a file: {file_path}"}\n'
        '        try:\n'
        '            content = p.read_text(encoding="utf-8", errors="replace")\n'
        '            lines = content.split("\\n")\n'
        '            if start_line > 0 or end_line > 0:\n'
        '                start = start_line - 1 if start_line > 0 else 0\n'
        '                end = end_line if end_line > 0 else len(lines)\n'
        '                lines = lines[start:end]\n'
        '                content = "\\n".join(lines)\n'
        '            return {"file_path": str(p), "content": content, "lines": len(lines), "size_bytes": len(content.encode("utf-8"))}\n'
        '        except Exception as e:\n'
        '            return {"error": str(e)}\n'
    )

    TOOLS_GLOB: str = (
        '"""Tool: glob"""\n'
        'from __future__ import annotations\n'
        'from pathlib import Path\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        '\n'
        'class GlobSchema(BaseModel):\n'
        '    pattern: str = Field(...)\n'
        '    path: str = Field(".", description="Base directory to search from")\n'
        '\n'
        'class GlobTool(BaseTool):\n'
        '    name = "glob"\n'
        '    description = "Find files matching a glob pattern."\n'
        '    cacheable = False\n'
        '    timeout = 10\n'
        '    args_model = GlobSchema\n'
        '    async def _run(self, pattern: str, path: str = ".") -> dict[str, Any]:\n'
        '        base = Path(path).resolve()\n'
        '        if not base.exists():\n'
        '            return {"error": f"Directory not found: {path}"}\n'
        '        matches = sorted(base.glob(pattern))\n'
        '        files = []\n'
        '        for m in matches:\n'
        '            if m.is_file():\n'
        '                try:\n'
        '                    rel = m.relative_to(Path.cwd().resolve())\n'
        '                    files.append(str(rel))\n'
        '                except ValueError:\n'
        '                    files.append(str(m))\n'
        '        return {"pattern": pattern, "path": str(base), "matches": files[:100], "count": len(files)}\n'
    )

    TOOLS_GREP: str = (
        '"""Tool: grep"""\n'
        'from __future__ import annotations\n'
        'from pathlib import Path\n'
        'from typing import Any\n'
        'from pydantic import BaseModel, Field\n'
        'from .base import BaseTool\n'
        'import re, asyncio\n'
        '\n'
        'class GrepSchema(BaseModel):\n'
        '    pattern: str = Field(...)\n'
        '    path: str = Field(".", description="Directory to search in")\n'
        '    file_pattern: str = Field("*.py", description="File glob pattern")\n'
        '    case_insensitive: bool = Field(False)\n'
        '    max_results: int = Field(50, ge=1, le=500)\n'
        '\n'
        'class GrepTool(BaseTool):\n'
        '    name = "grep"\n'
        '    description = "Search file contents using regex pattern. Returns matching lines with context."\n'
        '    cacheable = False\n'
        '    timeout = 15\n'
        '    args_model = GrepSchema\n'
        '    async def _run(self, pattern: str, path: str = ".", file_pattern: str = "*.py",\n'
        '                   case_insensitive: bool = False, max_results: int = 50) -> dict[str, Any]:\n'
        '        def _sync_search():\n'
        '            base = Path(path).resolve()\n'
        '            if not base.exists():\n'
        '                return {"error": f"Directory not found: {path}"}\n'
        '            flags = re.IGNORECASE if case_insensitive else 0\n'
        '            try:\n'
        '                regex = re.compile(pattern, flags)\n'
        '            except re.error as e:\n'
        '                return {"error": f"Invalid regex: {e}"}\n'
        '            results = []\n'
        '            for f in base.rglob(file_pattern):\n'
        '                if not f.is_file():\n'
        '                    continue\n'
        '                try:\n'
        '                    content = f.read_text(encoding="utf-8", errors="replace")\n'
        '                    for i, line in enumerate(content.split("\\n"), 1):\n'
        '                        if regex.search(line):\n'
        '                            try:\n'
        '                                rel = f.relative_to(Path.cwd().resolve())\n'
        '                                file_str = str(rel)\n'
        '                            except ValueError:\n'
        '                                file_str = str(f)\n'
        '                            results.append({"file": file_str, "line": i, "content": line[:200]})\n'
        '                            if len(results) >= max_results:\n'
        '                                return {"pattern": pattern, "matches": results, "count": len(results), "truncated": True}\n'
        '                except Exception:\n'
        '                    continue\n'
        '            return {"pattern": pattern, "matches": results, "count": len(results), "truncated": False}\n'
        '        return await asyncio.to_thread(_sync_search)\n'
    )

    UI_CLI: str = (
        '"""REPL."""\n'
        'from __future__ import annotations\n'
        'import asyncio\n'
        'from ..core.agent import Agent\n'
        '\n'
        'class SynaptiqCLI:\n'
        '    async def run(self) -> None:\n'
        '        a = Agent("key")\n'
        '        while True:\n'
        '            i = input("You > ")\n'
        '            if i == "exit":\n'
        '                break\n'
        '            async for c in a.think(i):\n'
        '                print(c, end="", flush=True)\n'
        '            print()\n'
    )

    TEST_BASIC: str = (
        '"""Smoke test."""\n'
        'from synaptiq.core.session import Session\n'
        '\n'
        'def test_sess() -> None:\n'
        '    s = Session()\n'
        '    s.add_message("u", "h")\n'
        '    assert s.messages[0]["content"] == "h"\n'
        '\n'
        'def test_undo() -> None:\n'
        '    s = Session()\n'
        '    s.add_message("user", "hello")\n'
        '    s.add_message("assistant", "hi")\n'
        '    assert len(s.messages) == 2\n'
        '    assert s.undo_last_turn() is True\n'
        '    assert len(s.messages) == 0\n'
    )


@dataclass(frozen=True, slots=True)
class Scaffold:
    DIRECTORIES: tuple[str, ...] = (
        "synaptiq/core", "synaptiq/tools", "synaptiq/agents",
        "synaptiq/ui", "tests", "web", "workflows",
    )
    FILES: dict[str, str] = field(default_factory=lambda: {
        "pyproject.toml": Templates.PYPROJECT,
        "synaptiq.json": Templates.CONFIG_JSON,
        ".gitignore": Templates.GITIGNORE,
        ".env.example": Templates.ENV_EXAMPLE,
        "synaptiq/__init__.py": Templates.INIT,
        "synaptiq/__main__.py": Templates.MAIN,
        "synaptiq/core/__init__.py": "",
        "synaptiq/tools/__init__.py": "",
        "synaptiq/ui/__init__.py": "",
        "tests/__init__.py": "",
        "synaptiq/core/provider.py": Templates.CORE_PROVIDER,
        "synaptiq/core/state_manager.py": Templates.CORE_STATE_MANAGER,
        "synaptiq/core/event_bus.py": Templates.CORE_EVENT_BUS,
        "synaptiq/core/memory.py": Templates.CORE_MEMORY,
        "synaptiq/core/session.py": Templates.CORE_SESSION,
        "synaptiq/core/orchestrator.py": Templates.CORE_ORCHESTRATOR,
        "synaptiq/core/registry.py": Templates.CORE_REGISTRY,
        "synaptiq/core/agent.py": Templates.CORE_AGENT,
        "synaptiq/core/pricing.py": Templates.CORE_PRICING,
        "synaptiq/tools/base.py": Templates.TOOLS_BASE,
        "synaptiq/tools/multi_file_write.py": Templates.TOOLS_MULTI_FILE_WRITE,
        "synaptiq/tools/execute_code.py": Templates.TOOLS_EXECUTE_CODE,
        "synaptiq/tools/web_search.py": Templates.TOOLS_WEB_SEARCH,
        "synaptiq/tools/read_file.py": Templates.TOOLS_READ_FILE,
        "synaptiq/tools/glob.py": Templates.TOOLS_GLOB,
        "synaptiq/tools/grep.py": Templates.TOOLS_GREP,
        "synaptiq/ui/cli.py": Templates.UI_CLI,
        "tests/test_basic.py": Templates.TEST_BASIC,
    })


# ═══════════════════════════════════════════════════════════════════════════
# 4. ASYNC LIFECYCLE & REPL
# ═══════════════════════════════════════════════════════════════════════════

@runtime_checkable
class AgentProtocol(Protocol):
    usage: dict[str, int]
    sess: "Session"
    reg: "ToolRegistry"
    orch: "Orchestrator"
    trace: list[dict]
    model_name: str
    stream_mode: bool
    async def _episodic_consolidator_loop(self) -> None: ...
    async def think(self, prompt: str, image: dict | None = None) -> AsyncIterator[str]: ...
    async def aclose(self) -> None: ...
    def set_model(self, model: str) -> None: ...
    def set_provider(self, provider: str, model: str | None = None) -> None: ...
    def set_image(self, path: str) -> bool: ...
    def set_file(self, path: str) -> bool: ...
    def cost(self) -> float: ...


@final
class SynaptiqCompleter(Completer):
    __slots__ = ("slash_commands", "_path_completer")

    def __init__(self) -> None:
        self.slash_commands = [
            "/help", "/stats", "/reset", "/tools",
            "/agents", "/stream", "/save", "/load", "/exit",
            "/clear", "/memory", "/trace", "/model", "/cost",
            "/export", "/undo", "/image", "/file", "/multiline",
            "/cache", "/provider", "/plan", "/budget",
        ]
        self._path_completer = PathCompleter()

    def get_completions(self, document: Any, complete_event: Any) -> Iterator[Completion]:
        text = document.text_before_cursor
        parts = text.split(None, 1)
        if not parts:
            return
        cmd = parts[0]
        if not cmd.startswith("/"):
            return
        if len(parts) == 1:
            for c in self.slash_commands:
                if c.startswith(cmd):
                    yield Completion(c, start_position=-len(text))
            return
        arg_text = parts[1]
        if cmd in ("/image", "/file", "/load", "/save"):
            doc = Document(text=arg_text, cursor_position=len(arg_text))
            yield from self._path_completer.get_completions(doc, complete_event)


@contextlib.asynccontextmanager
async def managed_agent_lifecycle(agent: AgentProtocol) -> AsyncIterator[AgentProtocol]:
    try:
        yield agent
    finally:
        with contextlib.suppress(Exception):
            await agent.aclose()


def _install_signal_handlers() -> None:
    def _handler(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt
    with contextlib.suppress(ValueError, OSError):
        signal.signal(signal.SIGINT, _handler)
    with contextlib.suppress(ValueError, OSError):
        signal.signal(signal.SIGTERM, _handler)


async def run_repl(console: Console, agent: AgentProtocol) -> None:
    console.banner()
    console.dim("Type '/help' for commands, 'exit' to quit.\n")

    stream_mode = False
    multiline_mode = False

    if PROMPT_TOOLKIT_AVAILABLE:
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        history = FileHistory(str(HISTORY_FILE))
        session = PromptSession(completer=SynaptiqCompleter(), history=history)
        prompt_fn = session.prompt_async
    else:
        async def prompt_fn() -> str:
            return await asyncio.to_thread(input, "You > ")

    prev_input_tokens = 0
    prev_output_tokens = 0
    prev_cost = 0.0

    with stats_session() as stats:
        async with managed_agent_lifecycle(agent):
            try:
                while True:
                    try:
                        if multiline_mode:
                            console.dim("Multiline mode (type '---' on empty line to submit):")
                            lines = []
                            while True:
                                line = await prompt_fn()
                                if line.strip() == "---":
                                    break
                                lines.append(line)
                            user_input = "\n".join(lines)
                        else:
                            user_input = (await prompt_fn()).strip()
                    except (EOFError, KeyboardInterrupt):
                        break

                    if not user_input:
                        continue
                    if user_input.lower() in ("exit", "quit"):
                        break

                    if user_input.startswith("/"):
                        parts = user_input.split(None, 1)
                        cmd = parts[0].lower()
                        arg = parts[1] if len(parts) > 1 else ""

                        match cmd:
                            case "/help":
                                console.dim(
                                    "  /help              Show this help\n"
                                    "  /stats             Show token usage\n"
                                    "  /reset | /clear    Reset session\n"
                                    "  /tools             List available tools\n"
                                    "  /agents            List configured agents\n"
                                    "  /memory            Show memory stats\n"
                                    "  /trace             Show recent execution events\n"
                                    "  /stream            Toggle stream mode\n"
                                    "  /save              Save session to file\n"
                                    "  /load              Load session from file\n"
                                    "  /model <name>      Switch model\n"
                                    "  /provider <name>   Switch provider\n"
                                    "  /cost              Show detailed cost breakdown\n"
                                    "  /export [md|json]  Export conversation\n"
                                    "  /undo              Undo last turn\n"
                                    "  /image <path>      Attach image to next prompt\n"
                                    "  /file <path>       Attach file as context\n"
                                    "  /multiline         Toggle multiline input\n"
                                    "  /cache [clear]     Tool cache management\n"
                                    "  /plan              Toggle plan mode\n"
                                    "  /budget            Show budget info\n"
                                    "  /exit              Exit REPL"
                                )
                            case "/stats":
                                cost = 0.0
                                with contextlib.suppress(Exception):
                                    cost = agent.cost()
                                console.dim(
                                    f"Tokens: in={agent.usage['input_tokens']} "
                                    f"out={agent.usage['output_tokens']} "
                                    f"tools={agent.usage.get('tool_calls', 0)} "
                                    f"cost=${cost:.4f} "
                                    f"model={getattr(agent, 'model_name', 'unknown')}"
                                )
                            case "/cost":
                                cost = 0.0
                                with contextlib.suppress(Exception):
                                    cost = agent.cost()
                                console.table(
                                    ["Metric", "Value"],
                                    [
                                        ["Input tokens", str(agent.usage.get("input_tokens", 0))],
                                        ["Output tokens", str(agent.usage.get("output_tokens", 0))],
                                        ["Tool calls", str(agent.usage.get("tool_calls", 0))],
                                        ["Cost (USD)", f"${cost:.6f}"],
                                        ["Model", getattr(agent, "model_name", "unknown")],
                                    ]
                                )
                            case "/reset" | "/clear":
                                agent.sess.clear()
                                agent.usage = {"input_tokens": 0, "output_tokens": 0, "tool_calls": 0}
                                agent.trace.clear()
                                prev_input_tokens = 0
                                prev_output_tokens = 0
                                prev_cost = 0.0
                                console.success("Session reset.")
                            case "/stream":
                                stream_mode = not stream_mode
                                agent.stream_mode = stream_mode
                                console.success(f"Stream mode: {'ON' if stream_mode else 'OFF'}.")
                            case "/multiline":
                                multiline_mode = not multiline_mode
                                console.success(f"Multiline mode: {'ON' if multiline_mode else 'OFF'}.")
                            case "/model":
                                if not arg:
                                    console.dim(f"Current model: {getattr(agent, 'model_name', 'unknown')}")
                                else:
                                    try:
                                        agent.set_model(arg.strip())
                                        console.success(f"Model switched to: {arg.strip()}")
                                    except Exception as e:
                                        console.error(f"Failed to switch model: {e}")
                            case "/provider":
                                if not arg:
                                    console.dim(f"Current provider: {getattr(agent, 'provider_name', 'unknown')}")
                                else:
                                    parts = arg.split()
                                    provider = parts[0]
                                    model = parts[1] if len(parts) > 1 else None
                                    try:
                                        agent.set_provider(provider, model)
                                        console.success(f"Provider switched to: {provider}")
                                    except Exception as e:
                                        console.error(f"Failed to switch provider: {e}")
                            case "/plan":
                                agent.plan_mode = not agent.plan_mode
                                console.success(f"Plan mode: {'ON' if agent.plan_mode else 'OFF'}.")
                            case "/budget":
                                budget = getattr(agent, "budget", None)
                                if budget:
                                    cost = 0.0
                                    with contextlib.suppress(Exception):
                                        cost = agent.cost()
                                    remaining = max(0.0, budget - cost)
                                    console.dim(f"Budget: ${budget:.2f} | Spent: ${cost:.4f} | Remaining: ${remaining:.4f}")
                                else:
                                    console.dim("No budget set.")
                            case "/save":
                                saved = agent.sess.save()
                                console.success("Session saved to session_backup.json." if saved else "Failed to save session.")
                            case "/load":
                                loaded = agent.sess.load()
                                console.success("Session loaded from session_backup.json." if loaded else "No saved session found.")
                            case "/tools":
                                tools_info = []
                                for name, t in agent.reg.tools.items():
                                    if "_v" not in name:
                                        tools_info.append([t.name, t.description[:60]])
                                if tools_info:
                                    console.table(["Tool", "Description"], tools_info)
                                else:
                                    console.dim("No tools registered.")
                            case "/agents":
                                agents = agent.orch.list_agents()
                                if agents:
                                    console.table(["Agent"], [[a] for a in agents])
                                else:
                                    console.dim("No agents configured.")
                            case "/memory":
                                with contextlib.suppress(Exception):
                                    mem = agent.reg.bus.context.get("memory_store")
                                    if mem:
                                        ep_count = mem.col_episodic.count() if mem.col_episodic else len(mem._f)
                                        sem_count = mem.col_semantic.count() if mem.col_semantic else 0
                                        console.table(
                                            ["Memory Type", "Count"],
                                            [["Episodic", str(ep_count)], ["Semantic", str(sem_count)]]
                                        )
                                    else:
                                        console.dim("Memory store not available.")
                            case "/trace":
                                if not agent.trace:
                                    console.dim("No trace events yet.")
                                else:
                                    console.dim("Last 5 trace events:")
                                    for t in agent.trace[-5:]:
                                        try:
                                            print(json.dumps(t, indent=2, default=str))
                                        except Exception:
                                            console.dim(f"  - {t.get('event', 'unknown')}")
                            case "/export":
                                fmt = arg.strip().lower() if arg else "md"
                                if fmt == "json":
                                    data = agent.sess.export_json()
                                    export_path = f"conversation_export_{int(time.time())}.json"
                                    Path(export_path).write_text(data)
                                    console.success(f"Exported to {export_path}")
                                else:
                                    md = agent.sess.export_markdown()
                                    export_path = f"conversation_export_{int(time.time())}.md"
                                    Path(export_path).write_text(md)
                                    console.success(f"Exported to {export_path}")
                            case "/undo":
                                if agent.sess.undo_last_turn():
                                    console.success("Last turn removed.")
                                else:
                                    console.warning("No turns to undo.")
                            case "/image":
                                if not arg:
                                    console.dim("Usage: /image <path>")
                                elif agent.set_image(arg.strip()):
                                    console.success(f"Image loaded: {arg.strip()}")
                                else:
                                    console.error(f"Failed to load image: {arg.strip()}")
                            case "/file":
                                if not arg:
                                    console.dim("Usage: /file <path>")
                                elif agent.set_file(arg.strip()):
                                    console.success(f"File loaded: {arg.strip()}")
                                else:
                                    console.error(f"Failed to load file: {arg.strip()}")
                            case "/cache":
                                if arg.strip().lower() == "clear":
                                    count = agent.reg.clear_cache()
                                    console.success(f"Cleared {count} cache entries.")
                                else:
                                    cache_stats = agent.reg.get_cache_stats()
                                    console.dim(f"Cache entries: {cache_stats['entries']}/{cache_stats['max_size']}")
                            case _:
                                console.error(f"Unknown command: {cmd}")
                        continue

                    print(f"{Color.MAGENTA}Assistant > {Color.RESET}", end="", flush=True)
                    full_response = []
                    try:
                        async with contextlib.aclosing(agent.think(user_input)) as gen:
                            async for chunk in gen:
                                print(chunk, end="", flush=True)
                                full_response.append(chunk)
                    except Exception as e:
                        print(f"\n[Error] {e}", end="")
                    print()

                    curr_input = agent.usage.get("input_tokens", 0)
                    curr_output = agent.usage.get("output_tokens", 0)
                    curr_cost = 0.0
                    with contextlib.suppress(Exception):
                        curr_cost = agent.cost()

                    stats.prompts += 1
                    stats.input_tokens += max(0, curr_input - prev_input_tokens)
                    stats.output_tokens += max(0, curr_output - prev_output_tokens)
                    stats.cost_usd += max(0.0, curr_cost - prev_cost)

                    prev_input_tokens = curr_input
                    prev_output_tokens = curr_output
                    prev_cost = curr_cost
            finally:
                stats.sessions += 1


async def run_one_shot(agent: AgentProtocol, prompt: str) -> None:
    with stats_session() as stats:
        async with managed_agent_lifecycle(agent):
            async with contextlib.aclosing(agent.think(prompt)) as gen:
                async for chunk in gen:
                    print(chunk, end="", flush=True)
            print()

            stats.prompts += 1
            stats.input_tokens += agent.usage.get("input_tokens", 0)
            stats.output_tokens += agent.usage.get("output_tokens", 0)
            with contextlib.suppress(Exception):
                stats.cost_usd += agent.cost()
            stats.sessions += 1


# ═══════════════════════════════════════════════════════════════════════════
# 5. DECLARATIVE CLI FRAMEWORK
# ═══════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class Arg:
    name: str
    kwargs: dict[str, Any] = field(default_factory=dict)


@final
class CLI:
    __slots__ = ("parser", "subparsers", "_commands")
    def __init__(self, desc: str) -> None:
        self.parser = argparse.ArgumentParser(prog="synaptiq-cli", description=desc)
        self.parser.add_argument("--dir", default=".", help="Project directory")
        self.parser.add_argument("--no-color", action="store_true")
        self.parser.add_argument("--quiet", action="store_true")
        self.parser.add_argument("--debug", action="store_true")
        self.parser.add_argument("--json", action="store_true", help="Machine-readable JSON output")
        self.parser.add_argument("-V", "--version", action="version",
                                 version=f"synaptiq-cli {VERSION} (framework {FRAMEWORK_VERSION})")
        self.subparsers = self.parser.add_subparsers(dest="command", metavar="<command>")
        self._commands: dict[str, Callable[..., Any]] = {}

    def command(self, name: str, args: Sequence[Arg] = ()) -> Callable:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._commands[name] = func
            sub = self.subparsers.add_parser(name, help=func.__doc__)
            for arg in args:
                sub.add_argument(arg.name, **arg.kwargs)
            return func
        return decorator


# ═══════════════════════════════════════════════════════════════════════════
# 6. IMPERATIVE SHELL: APP & COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

@final
class App:
    __slots__ = ("config", "console", "cli")
    def __init__(self) -> None:
        self.config = AppConfig(project_dir=Path.cwd())
        self.console: Console = get_console(self.config)
        self.cli = CLI(f"Synaptiq CLI v{VERSION}")
        register_commands(self)

    def run(self, argv: Sequence[str] | None = None) -> ExitCode:
        args = self.cli.parser.parse_args(argv)

        self.config = AppConfig(
            project_dir=Path(args.dir).resolve(),
            debug=args.debug,
            quiet=args.quiet,
            use_color=not args.no_color,
            json_output=getattr(args, "json", False),
        )
        self.console = get_console(self.config)
        setup_logging(self.config.debug)

        if not args.command:
            self.cli.parser.print_help()
            return ExitCode.SUCCESS

        cmd_func = self.cli._commands.get(args.command)
        if not cmd_func:
            self.console.error(f"Unknown command: {args.command}")
            return ExitCode.NOT_FOUND

        try:
            cmd_func(self, args)
            return ExitCode.SUCCESS
        except SynaptiqError as e:
            self.console.error(str(e))
            if self.config.debug:
                traceback.print_exc()
            return e.code
        except KeyboardInterrupt:
            self.console.warning("\nInterrupted.")
            return ExitCode.INTERRUPTED
        except Exception as e:
            self.console.error(f"Unexpected error: {e}")
            if self.config.debug:
                traceback.print_exc()
            return ExitCode.GENERIC_ERROR


def register_commands(app: App) -> None:
    @app.cli.command("init", args=[
        Arg("name", {"help": "Project name"}),
        Arg("--force", {"action": "store_true", "help": "Overwrite existing directory"}),
    ])
    def init(app: App, args: argparse.Namespace) -> None:
        """Initialize a new Synaptiq project."""
        safe_name = sanitize_name(args.name)
        target = app.config.project_dir / safe_name
        if target.exists() and any(target.iterdir()) and not args.force:
            raise SynaptiqError(
                f"Directory '{target}' is not empty. Use --force to overwrite.",
                ExitCode.ALREADY_EXISTS,
            )

        target.mkdir(parents=True, exist_ok=True)
        scaffold = Scaffold()
        for d in scaffold.DIRECTORIES:
            (target / d).mkdir(parents=True, exist_ok=True)
        for rel_path, content in scaffold.FILES.items():
            (target / rel_path).parent.mkdir(parents=True, exist_ok=True)
            (target / rel_path).write_text(content, encoding="utf-8")

        app.console.success(f"Project initialised at {target}")
        app.console.dim(f"  Framework: Synaptiq v{FRAMEWORK_VERSION}")
        app.console.dim(f"  Next steps: cd {safe_name} && python -m synaptiq")

    @app.cli.command("chat", args=[
        Arg("--mock", {"action": "store_true", "help": "Use mock provider (no API key needed)"}),
        Arg("--provider", {"default": None, "help": "Override LLM provider (anthropic/openai/ollama/mock)"}),
        Arg("--model", {"default": None, "help": "Override LLM model name"}),
        Arg("--prompt", {"default": None, "help": "Execute a single prompt and exit (non-interactive)"}),
        Arg("--budget", {"type": float, "default": None, "help": "Set cost budget in USD"}),
        Arg("--fallback-model", {"default": None, "help": "Fallback model if primary fails"}),
        Arg("--fallback-provider", {"default": None, "help": "Fallback provider"}),
        Arg("--approve-tools", {"action": "store_true", "help": "Require approval before tool execution"}),
        Arg("--plan", {"action": "store_true", "help": "Enable plan mode (generate step-by-step plan first)"}),
        Arg("--smart-routing", {"action": "store_true", "help": "Enable smart routing to different models based on rules"}),
        Arg("--stream", {"action": "store_true", "help": "Enable streaming mode by default"}),
        Arg("--summarize", {"action": "store_true", "help": "Enable context summarization"}),
    ])
    def chat(app: App, args: argparse.Namespace) -> None:
        """Start an interactive chat REPL or execute a single prompt."""
        if not (app.config.project_dir / "synaptiq").exists():
            raise ProjectNotFoundError(app.config.project_dir)

        provider = args.provider or "anthropic"
        if provider not in VALID_PROVIDERS:
            raise SynaptiqError(
                f"Invalid provider '{provider}'. Valid: {', '.join(sorted(VALID_PROVIDERS))}",
                ExitCode.VALIDATION_ERROR,
            )

        if args.mock or provider == "mock":
            provider = "mock"
            api_key = "mock"
            args.model = "mock"
        else:
            env_var = "ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY"
            api_key = os.getenv(env_var, "")
            if not api_key:
                raise SynaptiqError(
                    f"No API key found. Set {env_var} environment variable or use --mock.",
                    ExitCode.VALIDATION_ERROR,
                )

        project_str = str(app.config.project_dir)
        sys.path.insert(0, project_str)
        try:
            from synaptiq.core.agent import Agent
        except ImportError as e:
            missing_dep = "anthropic" in str(e).lower() or "openai" in str(e).lower() or "pydantic" in str(e).lower()
            if missing_dep:
                raise SynaptiqError(
                    f"Missing third-party dependencies: {e}. Run `pip install anthropic openai pydantic` inside your project.",
                    ExitCode.GENERIC_ERROR,
                ) from e
            raise SynaptiqError(
                f"Failed to import Agent: {e}. Ensure the project is properly initialised.",
                ExitCode.GENERIC_ERROR,
            ) from e

        agent: AgentProtocol = Agent(
            api_key=api_key,
            provider=provider,
            model=args.model,
            config_path=str(app.config.project_dir / "synaptiq.json"),
            budget_usd=args.budget,
            fallback_model=args.fallback_model,
            fallback_provider=args.fallback_provider,
            approve_tools=args.approve_tools,
            plan_mode=args.plan,
            smart_routing=args.smart_routing,
            stream_mode=args.stream,
            summarize=args.summarize,
        )
        
        # Inject console into bus for approval prompts
        agent.bus.context["console"] = app.console

        _install_signal_handlers()

        if args.prompt:
            async def execute() -> None:
                await run_one_shot(agent, args.prompt)
        else:
            async def execute() -> None:
                await run_repl(app.console, agent)

        try:
            asyncio.run(execute())
        except ExceptionGroup as eg:
            raise SynaptiqError(str(eg.exceptions[0])) from eg
        finally:
            with contextlib.suppress(ValueError):
                sys.path.remove(project_str)

    @app.cli.command("list", args=[])
    def list_projects(app: App, args: argparse.Namespace) -> None:
        """List Synaptiq projects in the current directory."""
        base = app.config.project_dir
        projects = []
        for child in sorted(base.iterdir()):
            if child.is_dir() and (child / "synaptiq" / "__init__.py").exists():
                config_file = child / "synaptiq.json"
                cfg = {}
                if config_file.exists():
                    with contextlib.suppress(json.JSONDecodeError):
                        cfg = json.loads(config_file.read_text())
                projects.append({
                    "name": child.name,
                    "provider": cfg.get("default_provider", "unknown"),
                    "model": cfg.get("default_model", "unknown"),
                })

        if app.config.json_output:
            print(json.dumps(projects, indent=2))
            return

        if not projects:
            app.console.dim("No Synaptiq projects found.")
            return
        app.console.header("Synaptiq Projects")
        app.console.table(
            ["Name", "Provider", "Model"],
            [[p["name"], p["provider"], p["model"]] for p in projects]
        )

    @app.cli.command("info", args=[])
    def info(app: App, args: argparse.Namespace) -> None:
        """Show information about the current project."""
        project_path = app.config.project_dir / "synaptiq"
        if not project_path.exists():
            raise ProjectNotFoundError(app.config.project_dir)

        config_file = app.config.project_dir / "synaptiq.json"
        cfg = {}
        if config_file.exists():
            with contextlib.suppress(json.JSONDecodeError):
                cfg = json.loads(config_file.read_text())

        tools_dir = project_path / "tools"
        tool_count = len([f for f in tools_dir.glob("*.py") if f.name != "__init__.py"]) if tools_dir.exists() else 0

        agents_dir = project_path / "agents"
        agent_count = len(list(agents_dir.glob("*.json"))) if agents_dir.exists() else 0

        if app.config.json_output:
            info_data = {
                "name": app.config.project_dir.name,
                "path": str(app.config.project_dir),
                "provider": cfg.get("default_provider", "anthropic"),
                "model": cfg.get("default_model", DEFAULT_MODEL),
                "framework_version": FRAMEWORK_VERSION,
                "cli_version": VERSION,
                "tools_count": tool_count,
                "agents_count": agent_count,
            }
            print(json.dumps(info_data, indent=2))
            return

        app.console.header(f"Project: {app.config.project_dir.name}")
        app.console.dim(f"  Path:       {app.config.project_dir}")
        app.console.dim(f"  Provider:   {cfg.get('default_provider', 'anthropic')}")
        app.console.dim(f"  Model:      {cfg.get('default_model', DEFAULT_MODEL)}")
        app.console.dim(f"  Framework:  v{FRAMEWORK_VERSION}")
        app.console.dim(f"  CLI:        v{VERSION}")
        app.console.dim(f"  Tools:      {tool_count} files")
        app.console.dim(f"  Agents:     {agent_count} configured")

    @app.cli.command("config", args=[
        Arg("action", {"choices": ["get", "set", "list", "edit"], "help": "Config action"}),
        Arg("key", {"nargs": "?", "help": "Config key"}),
        Arg("value", {"nargs": "?", "help": "Config value (for set)"}),
    ])
    def config_cmd(app: App, args: argparse.Namespace) -> None:
        """Manage project configuration."""
        config_path = app.config.project_dir / "synaptiq.json"
        if not config_path.exists():
            raise ProjectNotFoundError(app.config.project_dir)

        match args.action:
            case "get":
                if not args.key:
                    app.console.error("Key required: config get <key>")
                    return
                value = ConfigManager.get(config_path, args.key)
                if value is not None:
                    if app.config.json_output:
                        print(json.dumps({"key": args.key, "value": value}))
                    else:
                        app.console.dim(f"{args.key} = {json.dumps(value)}")
                else:
                    app.console.warning(f"Key '{args.key}' not found.")
            case "set":
                if not args.key or args.value is None:
                    app.console.error("Key and value required: config set <key> <value>")
                    return
                ConfigManager.set(config_path, args.key, args.value)
                app.console.success(f"Set {args.key} = {args.value}")
            case "list":
                cfg = ConfigManager.list_all(config_path)
                if app.config.json_output:
                    print(json.dumps(cfg, indent=2))
                else:
                    app.console.header("Configuration")
                    rows = [[k, json.dumps(v)] for k, v in cfg.items()]
                    app.console.table(["Key", "Value"], rows)
            case "edit":
                ConfigManager.edit(config_path)
                app.console.success("Config editor closed.")

    @app.cli.command("doctor", args=[])
    def doctor(app: App, args: argparse.Namespace) -> None:
        """Run health checks on the environment and project."""
        results: list[tuple[str, str, str]] = []  # (check, status, detail)

        py_ok = sys.version_info >= (3, 11)
        results.append(("Python", "✓" if py_ok else "✗", f"{sys.version.split()[0]} (need ≥3.11)"))

        has_anthropic = bool(os.getenv("ANTHROPIC_API_KEY"))
        results.append(("ANTHROPIC_API_KEY", "✓" if has_anthropic else "!", "Set" if has_anthropic else "Not set"))

        has_openai = bool(os.getenv("OPENAI_API_KEY"))
        results.append(("OPENAI_API_KEY", "✓" if has_openai else "!", "Set" if has_openai else "Not set"))

        for dep in ["anthropic", "openai", "chromadb", "networkx", "rich", "pydantic", "tiktoken", "rank_bm25"]:
            try:
                __import__(dep)
                results.append((dep, "✓", "Installed"))
            except ImportError:
                results.append((dep, "!", "Not installed"))

        project_path = app.config.project_dir / "synaptiq"
        if project_path.exists():
            results.append(("Project", "✓", str(app.config.project_dir)))
        else:
            results.append(("Project", "✗", "Not found"))

        cache_ok = CACHE_DIR.exists() and os.access(CACHE_DIR, os.W_OK)
        results.append(("Cache dir", "✓" if cache_ok else "✗", str(CACHE_DIR)))

        try:
            usage = shutil.disk_usage(str(Path.home()))
            free_gb = usage.free / (1024 ** 3)
            results.append(("Disk space", "✓" if free_gb > 1 else "!", f"{free_gb:.1f} GB free"))
        except Exception:
            pass

        if STATS_FILE.exists():
            stats = GlobalStats.load()
            results.append(("Stats", "✓", f"{stats.prompts} prompts, ${stats.cost_usd:.4f} spent"))
        else:
            results.append(("Stats", "!", "No stats file yet"))

        if app.config.json_output:
            data = [{"check": c, "status": s, "detail": d} for c, s, d in results]
            print(json.dumps(data, indent=2))
        else:
            app.console.header("Health Check")
            app.console.table(["Check", "Status", "Detail"], results)

        errors = sum(1 for _, s, _ in results if s == "✗")
        warnings = sum(1 for _, s, _ in results if s == "!")
        if errors == 0 and warnings == 0:
            app.console.success("All checks passed!")
        elif errors == 0:
            app.console.warning(f"All critical checks passed. {warnings} warning(s).")
        else:
            app.console.error(f"{errors} error(s), {warnings} warning(s).")

    @app.cli.command("cost", args=[
        Arg("--days", {"type": int, "default": 7, "help": "Number of days to show"}),
    ])
    def cost_cmd(app: App, args: argparse.Namespace) -> None:
        """Show cost report from global stats."""
        stats = GlobalStats.load()

        if app.config.json_output:
            data = {
                "total_cost": round(stats.cost_usd, 6),
                "total_prompts": stats.prompts,
                "total_input_tokens": stats.input_tokens,
                "total_output_tokens": stats.output_tokens,
                "total_sessions": stats.sessions,
                "daily": stats.get_daily_costs(args.days),
            }
            print(json.dumps(data, indent=2))
            return

        app.console.header(f"Cost Report (last {args.days} days)")
        app.console.dim(f"  Total cost:      ${stats.cost_usd:.4f}")
        app.console.dim(f"  Total prompts:   {stats.prompts}")
        app.console.dim(f"  Input tokens:    {stats.input_tokens:,}")
        app.console.dim(f"  Output tokens:   {stats.output_tokens:,}")
        app.console.dim(f"  Total sessions:  {stats.sessions}")

        daily = stats.get_daily_costs(args.days)
        if daily:
            app.console.header("Daily Breakdown")
            rows = []
            for d in daily:
                rows.append([
                    d["date"],
                    f"${d['cost']:.4f}",
                    str(d["prompts"]),
                    f"{d['input_tokens']:,}",
                    f"{d['output_tokens']:,}",
                ])
            app.console.table(["Date", "Cost", "Prompts", "In Tokens", "Out Tokens"], rows)

    @app.cli.command("tool", args=[
        Arg("action", {"choices": ["list", "create", "test"], "help": "Tool action"}),
        Arg("name", {"nargs": "?", "help": "Tool name"}),
    ])
    def tool_cmd(app: App, args: argparse.Namespace) -> None:
        """Manage tools."""
        project_path = app.config.project_dir / "synaptiq"
        if not project_path.exists():
            raise ProjectNotFoundError(app.config.project_dir)

        tools_dir = project_path / "tools"
        tools_dir.mkdir(parents=True, exist_ok=True)

        match args.action:
            case "list":
                tools = [f.stem for f in tools_dir.glob("*.py") if f.name != "__init__.py" and f.name != "base.py"]
                if tools:
                    app.console.header("Available Tools")
                    app.console.table(["Name"], [[t] for t in sorted(tools)])
                else:
                    app.console.dim("No custom tools found.")
            case "create":
                if not args.name:
                    app.console.error("Tool name required: tool create <name>")
                    return
                tool_name = sanitize_name(args.name)
                class_name = "".join(p.capitalize() for p in tool_name.split("_"))
                file_path = tools_dir / f"{tool_name}.py"
                if file_path.exists():
                    raise SynaptiqError(f"Tool '{tool_name}' already exists.", ExitCode.ALREADY_EXISTS)
                content = Templates.TOOL_STUB.format(name=tool_name, cn=class_name, desc=f"Tool: {tool_name}")
                file_path.write_text(content)
                app.console.success(f"Created tool: {file_path}")
            case "test":
                if not args.name:
                    app.console.error("Tool name required: tool test <name>")
                    return
                app.console.dim(f"Testing tool '{args.name}' requires a running agent. Use the REPL /tools command.")

    @app.cli.command("agent", args=[
        Arg("action", {"choices": ["list", "create", "delete"], "help": "Agent action"}),
        Arg("name", {"nargs": "?", "help": "Agent name"}),
    ])
    def agent_cmd(app: App, args: argparse.Namespace) -> None:
        """Manage sub-agents."""
        project_path = app.config.project_dir / "synaptiq"
        if not project_path.exists():
            raise ProjectNotFoundError(app.config.project_dir)

        agents_dir = project_path / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)

        match args.action:
            case "list":
                agents = [f.stem for f in agents_dir.glob("*.json")]
                if agents:
                    app.console.header("Configured Agents")
                    app.console.table(["Name"], [[a] for a in sorted(agents)])
                else:
                    app.console.dim("No agents configured.")
            case "create":
                if not args.name:
                    app.console.error("Agent name required: agent create <name>")
                    return
                agent_name = sanitize_name(args.name)
                file_path = agents_dir / f"{agent_name}.json"
                if file_path.exists():
                    raise SynaptiqError(f"Agent '{agent_name}' already exists.", ExitCode.ALREADY_EXISTS)
                template = {
                    "name": agent_name,
                    "system_prompt": f"You are {agent_name}, a specialized assistant.",
                    "llm_provider": "anthropic",
                    "llm_model": "claude-3-5-sonnet-20241022",
                    "api_key_env": "ANTHROPIC_API_KEY"
                }
                file_path.write_text(json.dumps(template, indent=2))
                app.console.success(f"Created agent: {file_path}")
                app.console.dim(f"  Edit {file_path} to customize.")
            case "delete":
                if not args.name:
                    app.console.error("Agent name required: agent delete <name>")
                    return
                agent_name = sanitize_name(args.name)
                file_path = agents_dir / f"{agent_name}.json"
                if not file_path.exists():
                    raise SynaptiqError(f"Agent '{agent_name}' not found.", ExitCode.NOT_FOUND)
                file_path.unlink()
                app.console.success(f"Deleted agent: {agent_name}")

    @app.cli.command("memory", args=[
        Arg("action", {"choices": ["search", "list", "clear", "export", "import"], "help": "Memory action"}),
        Arg("query", {"nargs": "?", "help": "Search query or file path"}),
        Arg("--type", {"default": "all", "choices": ["all", "episodic", "semantic"], "help": "Memory type"}),
    ])
    def memory_cmd(app: App, args: argparse.Namespace) -> None:
        """Manage agent memory."""
        project_path = app.config.project_dir / "synaptiq"
        if not project_path.exists():
            raise ProjectNotFoundError(app.config.project_dir)

        db_path = app.config.project_dir / "synaptiq_db"
        sys.path.insert(0, str(app.config.project_dir))
        try:
            from synaptiq.core.memory import MemoryStore
            mem = MemoryStore(db_path=str(db_path))

            match args.action:
                case "search":
                    if not args.query:
                        app.console.error("Query required: memory search <query>")
                        return
                    results = mem.search(args.query, k=10, memory_type=args.type)
                    if results:
                        app.console.header(f"Search Results ({len(results)})")
                        for i, r in enumerate(results, 1):
                            app.console.dim(f"  {i}. [{r.get('id', '?')}] {r.get('content', '')[:100]}...")
                    else:
                        app.console.dim("No results found.")
                case "list":
                    items = mem.list_all(memory_type=args.type)
                    if items:
                        app.console.header(f"Memory Items ({len(items)})")
                        if app.config.json_output:
                            print(json.dumps(items, indent=2))
                        else:
                            for i, item in enumerate(items[:50], 1):
                                app.console.dim(f"  {i}. [{item.get('id', '?')}] {item.get('content', '')[:80]}...")
                            if len(items) > 50:
                                app.console.dim(f"  ... and {len(items) - 50} more")
                    else:
                        app.console.dim("No memories found.")
                case "clear":
                    if app.console.confirm("Clear all memories? This cannot be undone.", default=False):
                        for col in [mem.col_episodic, mem.col_semantic]:
                            if col:
                                col.delete(ids=col.get()["ids"])
                        mem._f.clear()
                        app.console.success("Memory cleared.")
                    else:
                        app.console.dim("Cancelled.")
                case "export":
                    data = mem.export_data()
                    export_path = args.query or f"memory_export_{int(time.time())}.json"
                    Path(export_path).write_text(json.dumps(data, indent=2))
                    app.console.success(f"Exported {len(data)} memories to {export_path}")
                case "import":
                    if not args.query:
                        app.console.error("File path required: memory import <file>")
                        return
                    p = Path(args.query)
                    if not p.exists():
                        app.console.error(f"File not found: {p}")
                        return
                    data = json.loads(p.read_text())
                    mem.import_data(data)
                    app.console.success(f"Imported {len(data)} memories from {p}")
        finally:
            with contextlib.suppress(ValueError):
                sys.path.remove(str(app.config.project_dir))


# ═══════════════════════════════════════════════════════════════════════════
# 7. ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    app = App()
    return int(app.run())


if __name__ == "__main__":
    sys.exit(main())