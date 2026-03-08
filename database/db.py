import os
import random
import json
import math
import uuid
import re
import copy
import sqlite3
import threading
import queue
import time as time_module
import atexit
import tempfile
from decimal import Decimal
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime, date, time, timedelta, timezone
from typing import Optional, List, Dict, Any, Callable, Tuple, Set
import pandas as pd
from config import (
    NEAR_MISS_THRESHOLD,
    BONUS_PROB_MULTIPLIER,
    TEASER_DELAY_DEFAULT_HOURS,
    MAX_TEASERS_SHOWN,
    BONUS_XP_REWARD,
    FRONTIER_SORT_WEIGHTS,
)

try:
    import psycopg2
    from psycopg2 import pool as psycopg2_pool
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - optional dependency for Postgres deployments
    psycopg2 = None
    psycopg2_pool = None
    RealDictCursor = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency in constrained environments
    load_dotenv = None

if load_dotenv is not None:
    # Load project-level .env regardless of current working directory.
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

DB_PATH = os.getenv('DATABASE_PATH', 'data/dashboard.db')
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)
_LOCAL_CACHE_SETTING = os.getenv("LOCAL_CACHE_PRIMARY", "auto").strip().lower()
_DEFAULT_LOCAL_CACHE_DB_PATH = str(Path(tempfile.gettempdir()) / "renaissance-man" / "postgres_cache.db")
USE_LOCAL_CACHE_PRIMARY = USE_POSTGRES and (
    _LOCAL_CACHE_SETTING in {"1", "true", "yes", "on"}
    or (_LOCAL_CACHE_SETTING in {"", "auto"} and not os.getenv("RENDER"))
)
LOCAL_CACHE_DB_PATH = os.path.expandvars(
    os.path.expanduser(
        os.getenv("LOCAL_CACHE_DB_PATH", _DEFAULT_LOCAL_CACHE_DB_PATH).strip()
    )
)
POSTGRES_BOOTSTRAP_VERSION = "2026-03-08-perf-v1"
POSTGRES_BOOTSTRAP_KEY = "postgres_bootstrap_version"
DOMAIN_XP_PER_LEVEL = 500
GLOBAL_XP_PER_LEVEL = 2000
DAG_DEFAULT_USER_ID = 'default'
DEFAULT_DAG_GRAPH_ID = "discipline"
BODY_DAG_GRAPH_ID = "body"
MIND_DAG_GRAPH_ID = "mind"
_DAG_SIGNAL_NODE_CACHE: Optional[Dict[str, List[str]]] = None

RENAISSANCE_THRESHOLDS = {
    "run_5k_time_1_seconds": 1800,
    "run_5k_time_2_seconds": 1500,
    "run_3k_time_1_seconds": 900,
    "run_1k_time_1_seconds": 240,
    "run_10k_time_aggressive_seconds": 3300,
    "half_compete_time_seconds": 7200,
    "marathon_compete_time_seconds": 16200,
}

_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()
_READ_CACHE_LOCK = threading.Lock()
_READ_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_READ_CACHE_VERSION = 0
_WRITE_BEHIND_QUEUE: "queue.Queue[List[Tuple[str, Any]]]" = queue.Queue()
_WRITE_BEHIND_THREAD = None
_WRITE_BEHIND_LOCK = threading.Lock()
_WRITE_BEHIND_ATEXIT_REGISTERED = False
_LOCAL_CACHE_READY = False
_LOCAL_CACHE_READY_LOCK = threading.Lock()
_CACHE_TAGS_BY_PREFIX: Dict[str, Set[str]] = {
    "get_sleep_logs": {"sleep_logs"},
    "get_exercise_summary": {"exercise_resistance", "exercise_cardio"},
    "get_weight_logs": {"weight_logs"},
    "get_health_summary": {"weight_logs", "daily_health_logs"},
    "get_recent_daily_health_logs": {"daily_health_logs"},
    "get_weight_trend": {"weight_logs"},
    "get_exercise_trend": {"exercise_resistance", "exercise_cardio"},
    "get_active_projects": {"projects", "project_sessions"},
    "get_project_summary": {"projects", "project_sessions"},
    "get_all_tasks": {"tasks"},
    "get_finance_summary": {
        "weekly_finance",
        "finance_snapshots",
        "income_sources",
        "assets",
        "liabilities",
        "finance_fortnights",
    },
    "get_net_worth": {"assets", "liabilities"},
    "get_identity_levels": {"identity_levels", "xp_logs"},
    "get_overall_level": {"identity_levels", "xp_logs"},
    "discover_correlations": {"sleep_logs", "exercise_resistance", "exercise_cardio"},
    "get_daily_quests": {"daily_quests"},
    "get_routine_templates": {"routine_templates"},
    "get_routine_items": {"routine_items"},
    "is_routine_submitted": {"routine_submissions"},
    "get_today_routine_progress": {"daily_routine_progress"},
}


def _cache_tags_for_key(key: Tuple[Any, ...]) -> Set[str]:
    if not key:
        return set()
    prefix = str(key[0])
    return set(_CACHE_TAGS_BY_PREFIX.get(prefix, set()))


def _normalize_table_name(name: str) -> str:
    if not name:
        return ""
    table = name.strip().strip('"').split(".")[-1]
    return table.strip('"').lower()


def _extract_mutated_tables(query: str) -> Set[str]:
    cleaned = re.sub(r"/\*.*?\*/", " ", query or "", flags=re.S)
    cleaned = re.sub(r"--.*?$", " ", cleaned, flags=re.M).strip()
    if not cleaned:
        return set()

    ident = r'(?:\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_$]*)'
    table_pat = rf'({ident}(?:\.{ident})?)'
    tables: Set[str] = set()

    patterns = [
        rf"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+{table_pat}",
        rf"\bUPDATE\s+(?:ONLY\s+)?{table_pat}",
        rf"\bDELETE\s+FROM\s+{table_pat}",
        rf"\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?{table_pat}",
        rf"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?{table_pat}",
        rf"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{table_pat}",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            normalized = _normalize_table_name(match.group(1))
            if normalized:
                tables.add(normalized)

    truncate_match = re.search(
        rf"\bTRUNCATE\s+(?:TABLE\s+)?(.+?)(?:\s+RESTART|\s+CONTINUE|\s+CASCADE|\s+RESTRICT|;|$)",
        cleaned,
        flags=re.IGNORECASE | re.S,
    )
    if truncate_match:
        raw = truncate_match.group(1)
        for part in raw.split(","):
            table = _normalize_table_name(part.strip())
            if table:
                tables.add(table)

    return tables


def _clone_cached_value(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return value.copy(deep=True)
    if isinstance(value, (dict, list, tuple, set)):
        return copy.deepcopy(value)
    return value


def _invalidate_cached_reads(tables: Optional[Set[str]] = None):
    global _READ_CACHE_VERSION
    keys_to_refresh: List[Tuple[Any, ...]] = []
    with _READ_CACHE_LOCK:
        if not tables or "*" in tables:
            _READ_CACHE_VERSION += 1
            _READ_CACHE.clear()
            return

        normalized = {_normalize_table_name(t) for t in tables if t}
        if not normalized:
            return

        for key, entry in _READ_CACHE.items():
            tags = set(entry.get("tags", set()))
            if not tags or (tags & normalized):
                entry["stale"] = True
                if not entry.get("refreshing"):
                    entry["refreshing"] = True
                    keys_to_refresh.append(key)

    for key in keys_to_refresh:
        threading.Thread(
            target=_refresh_cached_key,
            args=(key,),
            daemon=True,
        ).start()


def _refresh_cached_key(key: Tuple[Any, ...]):
    with _READ_CACHE_LOCK:
        entry = _READ_CACHE.get(key)
        if not entry:
            return
        loader = entry.get("loader")
        version = entry.get("version")

    if loader is None:
        with _READ_CACHE_LOCK:
            current = _READ_CACHE.get(key)
            if current and current.get("version") == version:
                current["refreshing"] = False
                current["stale"] = False
        return

    try:
        value = loader()
    except Exception:
        with _READ_CACHE_LOCK:
            current = _READ_CACHE.get(key)
            if current and current.get("version") == version:
                current["refreshing"] = False
        return

    with _READ_CACHE_LOCK:
        current = _READ_CACHE.get(key)
        if current and current.get("version") == version:
            current["value"] = _clone_cached_value(value)
            current["stale"] = False
            current["refreshing"] = False


def _cached_read(key: Tuple[Any, ...], loader: Callable[[], Any]) -> Any:
    with _READ_CACHE_LOCK:
        entry = _READ_CACHE.get(key)
        if entry and entry.get("version") == _READ_CACHE_VERSION:
            if entry.get("stale"):
                if not entry.get("refreshing"):
                    entry["refreshing"] = True
                    threading.Thread(
                        target=_refresh_cached_key,
                        args=(key,),
                        daemon=True,
                    ).start()
            return _clone_cached_value(entry.get("value"))

    value = loader()
    with _READ_CACHE_LOCK:
        _READ_CACHE[key] = {
            "version": _READ_CACHE_VERSION,
            "value": _clone_cached_value(value),
            "loader": loader,
            "tags": _cache_tags_for_key(key),
            "stale": False,
            "refreshing": False,
        }
    return _clone_cached_value(value)


def _upsert_cached_dataframe_row(
    prefix: str,
    row: Dict[str, Any],
    *,
    date_column: str = "date",
    sort_ascending: bool = False,
    limit_key_index: Optional[int] = None,
    cutoff_key_index: Optional[int] = None,
):
    if not row or date_column not in row:
        return

    target_date = str(row[date_column])
    with _READ_CACHE_LOCK:
        for key, entry in _READ_CACHE.items():
            if not key or key[0] != prefix:
                continue
            value = entry.get("value")
            if not isinstance(value, pd.DataFrame):
                continue

            df = value.copy(deep=True)
            if date_column not in df.columns:
                continue

            existing_row = {}
            if not df.empty:
                existing = df[df[date_column].astype(str) == target_date]
                if not existing.empty:
                    existing_row = existing.iloc[0].to_dict()
                df = df[df[date_column].astype(str) != target_date]

            merged_row = existing_row
            merged_row.update(row)
            for col in df.columns:
                merged_row.setdefault(col, None)

            df = pd.concat([df, pd.DataFrame([merged_row])], ignore_index=True)

            if cutoff_key_index is not None and len(key) > cutoff_key_index:
                cutoff = str(key[cutoff_key_index])
                df = df[df[date_column].astype(str) >= cutoff]

            df = df.sort_values(by=date_column, ascending=sort_ascending)

            if limit_key_index is not None and len(key) > limit_key_index:
                try:
                    limit = int(key[limit_key_index])
                    df = df.head(limit)
                except Exception:
                    pass

            entry["value"] = df.reset_index(drop=True)
            entry["stale"] = False
            entry["refreshing"] = False


def _upsert_cached_list_row(
    prefix: str,
    row: Dict[str, Any],
    *,
    date_column: str = "date",
    sort_desc: bool = True,
    limit_key_index: Optional[int] = 1,
):
    if not row or date_column not in row:
        return

    target_date = str(row[date_column])
    with _READ_CACHE_LOCK:
        for key, entry in _READ_CACHE.items():
            if not key or key[0] != prefix:
                continue
            value = entry.get("value")
            if not isinstance(value, list):
                continue

            rows: List[Dict[str, Any]] = []
            replaced = False
            for item in value:
                if isinstance(item, dict) and str(item.get(date_column, "")) == target_date:
                    if not replaced:
                        merged = dict(item)
                        merged.update(row)
                        rows.append(merged)
                        replaced = True
                    continue
                rows.append(dict(item) if isinstance(item, dict) else item)

            if not replaced:
                rows.append(dict(row))

            rows = sorted(
                rows,
                key=lambda r: str((r or {}).get(date_column, "")),
                reverse=bool(sort_desc),
            )

            if limit_key_index is not None and len(key) > limit_key_index:
                try:
                    limit = int(key[limit_key_index])
                    rows = rows[:limit]
                except Exception:
                    pass

            entry["value"] = rows
            entry["stale"] = False
            entry["refreshing"] = False


def _patch_cached_health_summary(
    *,
    logged_date: str,
    latest_weight: Optional[float] = None,
):
    with _READ_CACHE_LOCK:
        for key, entry in _READ_CACHE.items():
            if not key or key[0] != "get_health_summary":
                continue
            value = entry.get("value")
            if not isinstance(value, dict):
                continue
            updated = dict(value)
            if latest_weight is not None:
                existing_date = str(updated.get("latest_weight_date") or "")
                if not existing_date or logged_date >= existing_date:
                    updated["latest_weight"] = latest_weight
                    updated["latest_weight_date"] = logged_date
            entry["value"] = updated
            entry["stale"] = False
            entry["refreshing"] = False


def _patch_cached_exercise_summary(
    *,
    logged_date: str,
    resistance_deltas: Optional[Dict[str, float]] = None,
    cardio_minutes: float = 0.0,
    cardio_distance: float = 0.0,
):
    resistance_deltas = dict(resistance_deltas or {})
    with _READ_CACHE_LOCK:
        for key, entry in _READ_CACHE.items():
            if not key or key[0] != "get_exercise_summary":
                continue
            value = entry.get("value")
            if not isinstance(value, dict):
                continue

            cutoff = str(key[2]) if len(key) > 2 else ""
            if cutoff and logged_date < cutoff:
                continue

            updated = dict(value)
            resistance = dict(updated.get("resistance") or {})
            for ex_type, delta in resistance_deltas.items():
                resistance[ex_type] = float(resistance.get(ex_type) or 0) + float(delta or 0)

            updated["resistance"] = resistance
            updated["cardio_minutes"] = float(updated.get("cardio_minutes") or 0) + float(cardio_minutes or 0)
            updated["cardio_distance"] = float(updated.get("cardio_distance") or 0) + float(cardio_distance or 0)
            entry["value"] = updated
            entry["stale"] = False
            entry["refreshing"] = False


def _is_mutating_sql(query: str) -> bool:
    token = (query or "").lstrip().split(None, 1)
    if not token:
        return False
    first = token[0].upper()
    return first in {
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "REINDEX",
        "VACUUM",
        "ANALYZE",
        "GRANT",
        "REVOKE",
    }


def _get_postgres_pool():
    global _PG_POOL
    if psycopg2_pool is None:
        raise RuntimeError("DATABASE_URL is set but psycopg2 pool is unavailable")
    if _PG_POOL is not None:
        return _PG_POOL
    with _PG_POOL_LOCK:
        if _PG_POOL is None:
            min_conn = max(1, int(os.getenv("PG_POOL_MIN_CONN", "1")))
            default_max = max(4, int(os.getenv("WEB_CONCURRENCY", "1")) * 4)
            max_conn = max(min_conn, int(os.getenv("PG_POOL_MAX_CONN", str(default_max))))
            connect_timeout = int(os.getenv("PG_CONNECT_TIMEOUT", "10"))
            connect_retries = max(1, int(os.getenv("PG_CONNECT_RETRIES", "6")))
            retry_delay = max(1, int(os.getenv("PG_CONNECT_RETRY_DELAY", "5")))
            last_exc = None
            for attempt in range(1, connect_retries + 1):
                try:
                    _PG_POOL = psycopg2_pool.ThreadedConnectionPool(
                        min_conn,
                        max_conn,
                        DATABASE_URL,
                        connect_timeout=connect_timeout,
                        application_name="renaissance-man",
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt >= connect_retries:
                        raise
                    wait_seconds = retry_delay * attempt
                    print(
                        f"Postgres connection attempt {attempt}/{connect_retries} failed: {exc}. "
                        f"Retrying in {wait_seconds}s..."
                    )
                    time_module.sleep(wait_seconds)
            if _PG_POOL is None and last_exc is not None:
                raise last_exc
    return _PG_POOL


def _adapt_query_for_postgres(query: str) -> str:
    q = query
    q = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", q, flags=re.IGNORECASE)
    if "ON CONFLICT" not in q.upper() and "INSERT INTO" in q.upper() and "OR IGNORE" in query.upper():
        q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    q = q.replace("?", "%s")
    # Escape literal percent signs for psycopg2 paramstyle, while preserving %s placeholders.
    q = re.sub(r"%(?!s)", "%%", q)
    return q


class _PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def fetchone(self):
        row = self._cursor.fetchone()
        try:
            self._cursor.close()
        except Exception:
            pass
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        try:
            self._cursor.close()
        except Exception:
            pass
        return rows

    @property
    def lastrowid(self):
        try:
            self._cursor.execute("SELECT LASTVAL() AS id")
            row = self._cursor.fetchone()
            if isinstance(row, dict):
                return row.get("id")
            if row:
                return row[0]
        except Exception:
            return None
        return None

    def __iter__(self):
        return iter(self._cursor)


class _PostgresConnection:
    def __init__(self):
        self._pool = _get_postgres_pool()
        self._conn = self._pool.getconn()
        self._closed = False
        self._has_pending_write = False
        self._mutated_tables: Set[str] = set()

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def execute(self, query: str, params=None):
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        if params is None:
            # Keep raw SQL intact (e.g., PL/pgSQL bodies that use '%' in RAISE format strings).
            cur.execute(query)
        else:
            pg_query = _adapt_query_for_postgres(query)
            cur.execute(pg_query, params)
        first = (query or "").lstrip().split(None, 1)
        command = first[0].upper() if first else ""
        if command == "COMMIT":
            if self._has_pending_write:
                _invalidate_cached_reads(set(self._mutated_tables) or {"*"})
            self._has_pending_write = False
            self._mutated_tables.clear()
        elif command == "ROLLBACK":
            self._has_pending_write = False
            self._mutated_tables.clear()
        elif _is_mutating_sql(query):
            self._has_pending_write = True
            tables = _extract_mutated_tables(query)
            if tables:
                self._mutated_tables.update(tables)
            else:
                self._mutated_tables.add("*")
        return _PostgresCursor(cur)

    def executescript(self, script: str):
        # Used for SQLite bootstrap; Postgres path doesn't rely on this.
        for statement in script.split(";"):
            stmt = statement.strip()
            if stmt:
                self.execute(stmt)

    def commit(self):
        self._conn.commit()
        if self._has_pending_write:
            _invalidate_cached_reads(set(self._mutated_tables) or {"*"})
        self._has_pending_write = False
        self._mutated_tables.clear()

    def rollback(self):
        self._conn.rollback()
        self._has_pending_write = False
        self._mutated_tables.clear()

    def close(self):
        if self._closed:
            return
        try:
            # Avoid returning connections with open transactions to the pool.
            self._conn.rollback()
        except Exception:
            pass
        self._pool.putconn(self._conn)
        self._closed = True

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class _SQLiteConnection:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._has_pending_write = False
        self._mutated_tables: Set[str] = set()

    def execute(self, query: str, params=None):
        if params is None:
            cur = self._conn.execute(query)
        else:
            cur = self._conn.execute(query, params)
        first = (query or "").lstrip().split(None, 1)
        command = first[0].upper() if first else ""
        if command == "COMMIT":
            if self._has_pending_write:
                _invalidate_cached_reads(set(self._mutated_tables) or {"*"})
            self._has_pending_write = False
            self._mutated_tables.clear()
        elif command == "ROLLBACK":
            self._has_pending_write = False
            self._mutated_tables.clear()
        elif _is_mutating_sql(query):
            self._has_pending_write = True
            tables = _extract_mutated_tables(query)
            if tables:
                self._mutated_tables.update(tables)
            else:
                self._mutated_tables.add("*")
        return cur

    def commit(self):
        self._conn.commit()
        if self._has_pending_write:
            _invalidate_cached_reads(set(self._mutated_tables) or {"*"})
        self._has_pending_write = False
        self._mutated_tables.clear()

    def rollback(self):
        self._conn.rollback()
        self._has_pending_write = False
        self._mutated_tables.clear()

    def close(self):
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _SQLiteWriteBehindConnection(_SQLiteConnection):
    def __init__(self, db_path: Path):
        super().__init__(db_path)
        self._mirror_ops: List[Tuple[str, Any]] = []

    def execute(self, query: str, params=None):
        cur = super().execute(query, params)
        first = (query or "").lstrip().split(None, 1)
        command = first[0].upper() if first else ""
        if _is_mutating_sql(query) and command not in {"COMMIT", "ROLLBACK"}:
            safe_params = tuple(params) if isinstance(params, list) else params
            self._mirror_ops.append((query, safe_params))
        return cur

    def commit(self):
        super().commit()
        if self._mirror_ops:
            _enqueue_write_behind(self._mirror_ops)
            self._mirror_ops = []

    def rollback(self):
        super().rollback()
        self._mirror_ops = []


def _quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _start_write_behind_worker():
    global _WRITE_BEHIND_THREAD, _WRITE_BEHIND_ATEXIT_REGISTERED
    with _WRITE_BEHIND_LOCK:
        if _WRITE_BEHIND_THREAD and _WRITE_BEHIND_THREAD.is_alive():
            return
        _WRITE_BEHIND_THREAD = threading.Thread(
            target=_write_behind_worker_loop,
            name="postgres-write-behind",
            daemon=True,
        )
        _WRITE_BEHIND_THREAD.start()
        if not _WRITE_BEHIND_ATEXIT_REGISTERED:
            atexit.register(_flush_write_behind_queue)
            _WRITE_BEHIND_ATEXIT_REGISTERED = True


def _enqueue_write_behind(ops: List[Tuple[str, Any]]):
    if not USE_LOCAL_CACHE_PRIMARY:
        return
    if not ops:
        return
    _WRITE_BEHIND_QUEUE.put(list(ops))


def _write_behind_worker_loop():
    while True:
        ops = _WRITE_BEHIND_QUEUE.get()
        if ops is None:
            _WRITE_BEHIND_QUEUE.task_done()
            return

        delay_seconds = 0.25
        while True:
            conn = None
            try:
                conn = _PostgresConnection()
                for query, params in ops:
                    conn.execute(query, params)
                conn.commit()
                break
            except Exception as exc:
                if conn is not None:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                print(f"[write-behind] Postgres sync failed, retrying in {delay_seconds:.2f}s: {exc}")
                time_module.sleep(delay_seconds)
                delay_seconds = min(delay_seconds * 2, 10.0)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        _WRITE_BEHIND_QUEUE.task_done()


def _flush_write_behind_queue(timeout_seconds: float = 8.0):
    # Use a local import to avoid any global name shadowing edge cases.
    import time as _time

    deadline = _time.time() + max(0.0, float(timeout_seconds))
    while _time.time() < deadline:
        if _WRITE_BEHIND_QUEUE.empty():
            return
        _time.sleep(0.05)


def _prepare_local_cache_schema(local_conn):
    schema_path = Path(__file__).parent / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    local_conn.executescript(schema_sql)
    _ensure_sleep_log_columns(local_conn)
    _ensure_daily_health_columns(local_conn)
    _ensure_exercise_resistance_columns(local_conn)
    _ensure_identity_level_integrity(local_conn)
    _deactivate_orphan_routine_items(local_conn)
    local_conn.execute(
        """
        UPDATE xp_logs
        SET domain = 'health'
        WHERE domain = 'habits'
        """
    )
    local_conn.commit()


def _copy_postgres_to_local_cache(local_conn, pg_conn):
    def _to_sqlite_value(value: Any):
        if value is None or isinstance(value, (str, int, float, bytes)):
            return value
        if isinstance(value, Decimal):
            try:
                return float(value)
            except Exception:
                return str(value)
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()
        if isinstance(value, timedelta):
            return value.total_seconds()
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, (dict, list, tuple, set)):
            try:
                return json.dumps(value)
            except Exception:
                return str(value)
        return str(value)

    tables = [
        row["name"]
        for row in local_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]

    local_conn.execute("PRAGMA foreign_keys = OFF")
    for table in tables:
        cols = [
            row["name"]
            for row in local_conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
        ]
        if not cols:
            continue

        quoted_table = _quote_ident(table)
        quoted_cols = ", ".join(_quote_ident(c) for c in cols)
        placeholders = ", ".join("?" for _ in cols)

        try:
            pg_cols_rows = pg_conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ?
                """,
                (table,),
            ).fetchall()
            pg_cols = {str(row["column_name"]) for row in pg_cols_rows}
            common_cols = [col for col in cols if col in pg_cols]
            if not common_cols:
                print(f"[cache-hydrate] skipping '{table}': no shared columns found in Postgres")
                continue

            quoted_common_cols = ", ".join(_quote_ident(c) for c in common_cols)
            rows = pg_conn.execute(
                f"SELECT {quoted_common_cols} FROM {quoted_table}"
            ).fetchall()
        except Exception as exc:
            # Table might not exist in Postgres or may have incompatible structure.
            print(f"[cache-hydrate] failed for '{table}': {exc}")
            continue

        local_conn.execute(f"DELETE FROM {quoted_table}")
        common_set = set(common_cols)
        for row in rows:
            values = tuple(
                _to_sqlite_value(row.get(col)) if col in common_set else None
                for col in cols
            )
            local_conn.execute(
                f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})",
                values,
            )

        if "id" in cols:
            try:
                max_id = local_conn.execute(
                    f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {quoted_table}"
                ).fetchone()["max_id"]
                local_conn.execute(
                    "INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES(?, ?)",
                    (table, int(max_id or 0)),
                )
            except Exception:
                pass

    local_conn.execute("PRAGMA foreign_keys = ON")
    local_conn.commit()


def _ensure_local_cache_ready(force_refresh: bool = False):
    global _LOCAL_CACHE_READY
    if not USE_LOCAL_CACHE_PRIMARY:
        return
    if _LOCAL_CACHE_READY and not force_refresh:
        return

    with _LOCAL_CACHE_READY_LOCK:
        if _LOCAL_CACHE_READY and not force_refresh:
            return

        cache_path = Path(LOCAL_CACHE_DB_PATH)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if cache_path.exists():
            cache_path.unlink()

        local_conn = _SQLiteConnection(cache_path)
        pg_conn = _PostgresConnection()
        try:
            _prepare_local_cache_schema(local_conn)
            _copy_postgres_to_local_cache(local_conn, pg_conn)
        finally:
            local_conn.close()
            pg_conn.close()

        _start_write_behind_worker()
        _invalidate_cached_reads({"*"})
        _LOCAL_CACHE_READY = True


def get_connection():
    """Get database connection."""
    if USE_LOCAL_CACHE_PRIMARY:
        _ensure_local_cache_ready()
        return _SQLiteWriteBehindConnection(Path(LOCAL_CACHE_DB_PATH))

    if USE_POSTGRES:
        if psycopg2 is None:
            raise RuntimeError("DATABASE_URL is set but psycopg2 is not installed")
        return _PostgresConnection()

    return _SQLiteConnection(Path(DB_PATH))


def _dag_tables_available() -> bool:
    """Return True when core DAG tables exist."""
    conn = get_connection()
    if USE_POSTGRES and not USE_LOCAL_CACHE_PRIMARY:
        rows = conn.execute(
            """
            SELECT table_name AS name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('dag_nodes', 'dag_edges', 'dag_node_prereqs', 'dag_user_node_state')
            """
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('dag_nodes', 'dag_edges', 'dag_node_prereqs', 'dag_user_node_state')"
        ).fetchall()
    conn.close()
    return len(rows) == 4


def _query_dataframe(query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    conn = get_connection()
    rows = conn.execute(query, params or ()).fetchall()
    conn.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(row) for row in rows])


def _ensure_postgres_performance_indexes(conn):
    index_sql = [
        "CREATE INDEX IF NOT EXISTS idx_sleep_logs_date ON sleep_logs(date)",
        "CREATE INDEX IF NOT EXISTS idx_weight_logs_date ON weight_logs(date)",
        "CREATE INDEX IF NOT EXISTS idx_exercise_resistance_date ON exercise_resistance(date)",
        "CREATE INDEX IF NOT EXISTS idx_exercise_cardio_date ON exercise_cardio(date)",
        "CREATE INDEX IF NOT EXISTS idx_project_sessions_date ON project_sessions(date)",
        "CREATE INDEX IF NOT EXISTS idx_project_sessions_project_date ON project_sessions(project_id, date)",
        "CREATE INDEX IF NOT EXISTS idx_projects_status_tier_touched ON projects(status, tier, last_touched)",
        "CREATE INDEX IF NOT EXISTS idx_xp_logs_domain_activity ON xp_logs(domain, activity)",
        "CREATE INDEX IF NOT EXISTS idx_xp_logs_date ON xp_logs(date)",
        "CREATE INDEX IF NOT EXISTS idx_weekly_finance_week_end ON weekly_finance(week_end_date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_quests_date_completed ON daily_quests(date, completed)",
        "CREATE INDEX IF NOT EXISTS idx_tasks_completed_order ON tasks(completed, order_index)",
    ]
    for stmt in index_sql:
        conn.execute(stmt)


def _sync_postgres_sequences(conn):
    """Align Postgres sequences with current MAX(id) values to prevent duplicate-key inserts."""
    conn.execute(
        """
        DO $$
        DECLARE
            rec record;
            max_val bigint;
        BEGIN
            FOR rec IN
                SELECT
                    cols.table_schema AS schema_name,
                    cols.table_name,
                    cols.column_name,
                    pg_get_serial_sequence(
                        format('%I.%I', cols.table_schema, cols.table_name),
                        cols.column_name
                    ) AS seq_name
                FROM information_schema.columns cols
                WHERE cols.table_schema = 'public'
                  AND pg_get_serial_sequence(
                        format('%I.%I', cols.table_schema, cols.table_name),
                        cols.column_name
                    ) IS NOT NULL
            LOOP
                EXECUTE format(
                    'SELECT COALESCE(MAX(%I), 0) FROM %I.%I',
                    rec.column_name,
                    rec.schema_name,
                    rec.table_name
                )
                INTO max_val;

                EXECUTE format(
                    'SELECT setval(%L, %s, false)',
                    rec.seq_name,
                    max_val + 1
                );
            END LOOP;
        END
        $$;
        """
    )


def _bootstrap_postgres_if_needed(conn) -> bool:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    row = conn.execute(
        "SELECT value FROM app_meta WHERE key = ?",
        (POSTGRES_BOOTSTRAP_KEY,),
    ).fetchone()
    current = row["value"] if row else None
    if current == POSTGRES_BOOTSTRAP_VERSION:
        return False

    _install_postgres_compat_functions(conn)
    _ensure_postgres_performance_indexes(conn)
    conn.execute(
        """
        INSERT INTO app_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """,
        (POSTGRES_BOOTSTRAP_KEY, POSTGRES_BOOTSTRAP_VERSION),
    )
    return True


def _install_postgres_compat_functions(conn):
    conn.execute("DROP FUNCTION IF EXISTS public.date(text)")
    conn.execute("DROP FUNCTION IF EXISTS public.date(text,text)")
    conn.execute("DROP FUNCTION IF EXISTS public.datetime(text)")
    conn.execute("DROP FUNCTION IF EXISTS public.datetime(timestamp without time zone)")
    conn.execute("DROP FUNCTION IF EXISTS public.datetime(timestamp with time zone)")
    conn.execute("DROP FUNCTION IF EXISTS public.strftime(text,text)")
    conn.execute("DROP FUNCTION IF EXISTS public.julianday(text)")
    conn.execute("DROP FUNCTION IF EXISTS public.julianday(timestamp without time zone)")

    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.julianday(input_text text)
        RETURNS double precision
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT EXTRACT(EPOCH FROM (
            CASE
              WHEN lower(input_text) = 'now' THEN now()::timestamp
              ELSE input_text::timestamp
            END
          )) / 86400.0
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.julianday(input_ts timestamp WITHOUT TIME ZONE)
        RETURNS double precision
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT EXTRACT(EPOCH FROM input_ts) / 86400.0
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.date(input_text text)
        RETURNS text
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT to_char(
            CASE
            WHEN lower(input_text) = 'now' THEN current_date
            ELSE input_text::date
            END,
            'YYYY-MM-DD'
          )
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.date(input_text text, modifier text)
        RETURNS text
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT to_char(
            (
              CASE
                WHEN lower(input_text) = 'now' THEN now()::timestamp
                ELSE input_text::timestamp
              END
              +
              CASE
                WHEN modifier IS NULL OR trim(modifier) = '' THEN interval '0 days'
                ELSE modifier::interval
              END
            )::date,
            'YYYY-MM-DD'
          )
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.datetime(input_text text)
        RETURNS timestamp WITHOUT TIME ZONE
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT input_text::timestamp
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.datetime(input_ts timestamp WITHOUT TIME ZONE)
        RETURNS timestamp WITHOUT TIME ZONE
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT input_ts
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.datetime(input_ts timestamp WITH TIME ZONE)
        RETURNS timestamp WITHOUT TIME ZONE
        LANGUAGE SQL
        IMMUTABLE
        AS $$
          SELECT input_ts::timestamp
        $$;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION public.strftime(fmt text, ts text)
        RETURNS text
        LANGUAGE plpgsql
        IMMUTABLE
        AS $$
        BEGIN
          IF fmt = '%Y-%m' THEN
            RETURN to_char(ts::timestamp, 'YYYY-MM');
          ELSIF fmt = '%Y-%W' THEN
            RETURN to_char(ts::timestamp, 'IYYY-IW');
          ELSE
            RAISE EXCEPTION 'Unsupported strftime format: %', fmt;
          END IF;
        END;
        $$;
        """
    )


def init_db():
    """Initialize database with schema."""
    if USE_POSTGRES:
        conn = _PostgresConnection()
        bootstrap_updated = _bootstrap_postgres_if_needed(conn)
        _ensure_identity_level_integrity(conn)
        _deactivate_orphan_routine_items(conn)
        _sync_postgres_sequences(conn)
        conn.execute(
            """
            UPDATE xp_logs
            SET domain = 'health'
            WHERE domain = 'habits'
            """
        )
        conn.commit()
        conn.close()
        if USE_LOCAL_CACHE_PRIMARY:
            _ensure_local_cache_ready(force_refresh=True)
        if bootstrap_updated and os.getenv("RECALCULATE_IDENTITY_ON_BOOT", "0") == "1":
            recalculate_identity_levels_from_logs()
        if USE_LOCAL_CACHE_PRIMARY:
            print(f"Postgres initialized and local cache hydrated at {LOCAL_CACHE_DB_PATH}")
        else:
            print("Postgres database initialized from existing schema (cache-primary disabled)")
        return

    schema_path = Path(__file__).parent / 'schema.sql'
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
    
    conn = get_connection()
    conn.executescript(schema_sql)
    _ensure_sleep_log_columns(conn)
    _ensure_daily_health_columns(conn)
    _ensure_exercise_resistance_columns(conn)
    _ensure_identity_level_integrity(conn)
    _deactivate_orphan_routine_items(conn)
    # Backfill legacy routine XP domain into a tracked identity domain.
    conn.execute("""
        UPDATE xp_logs
        SET domain = 'health'
        WHERE domain = 'habits'
    """)
    conn.commit()
    conn.close()
    recalculate_identity_levels_from_logs()
    print(f"Database initialized at {DB_PATH}")


def _deactivate_orphan_routine_items(conn):
    """Disable active subtasks whose parent is missing/inactive in the same template."""
    conn.execute(
        """
        UPDATE routine_items
        SET active = 0
        WHERE active = 1
          AND parent_item_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM routine_items parent
              WHERE parent.id = routine_items.parent_item_id
                AND parent.active = 1
                AND parent.template_id = routine_items.template_id
          )
        """
    )

def _ensure_identity_level_integrity(conn):
    """Ensure one identity row per domain and enforce uniqueness."""
    default_domains = [
        ('sleep', 1, 'Novice Sleeper'),
        ('health', 1, 'Beginner'),
        ('projects', 1, 'Dabbler'),
        ('finance', 1, 'Awareness'),
    ]

    for domain, level, level_name in default_domains:
        exists = conn.execute(
            "SELECT id FROM identity_levels WHERE domain = ? LIMIT 1",
            (domain,),
        ).fetchone()
        if not exists:
            conn.execute(
                """
                INSERT INTO identity_levels (domain, level, level_name, xp)
                VALUES (?, ?, ?, 0)
                """,
                (domain, level, level_name),
            )

    rows = conn.execute("""
        SELECT id, domain
        FROM identity_levels
        WHERE domain IS NOT NULL
        ORDER BY domain, id
    """).fetchall()
    keep_by_domain = {}
    for row in rows:
        domain = row['domain']
        if domain not in keep_by_domain:
            keep_by_domain[domain] = row['id']
            continue
        conn.execute("DELETE FROM identity_levels WHERE id = ?", (row['id'],))

    conn.execute("DELETE FROM identity_levels WHERE domain IS NULL")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_levels_domain ON identity_levels(domain)"
    )

def _ensure_sleep_log_columns(conn):
    """Add new sleep tracking columns for existing databases."""
    columns = {
        row['name'] for row in conn.execute("PRAGMA table_info(sleep_logs)").fetchall()
    }

    if 'wakings_count' not in columns:
        conn.execute("ALTER TABLE sleep_logs ADD COLUMN wakings_count INTEGER DEFAULT 0")
    if 'sleep_onset' not in columns:
        conn.execute("ALTER TABLE sleep_logs ADD COLUMN sleep_onset TEXT DEFAULT 'normal'")
    if 'wake_method' not in columns:
        conn.execute("ALTER TABLE sleep_logs ADD COLUMN wake_method TEXT DEFAULT 'on_time'")
    if 'rested_level' not in columns:
        conn.execute("ALTER TABLE sleep_logs ADD COLUMN rested_level INTEGER DEFAULT 3")

def _ensure_daily_health_columns(conn):
    """Add new daily health columns for existing databases."""
    columns = {
        row['name'] for row in conn.execute("PRAGMA table_info(daily_health_logs)").fetchall()
    }
    if 'high_protein' not in columns:
        conn.execute("ALTER TABLE daily_health_logs ADD COLUMN high_protein INTEGER DEFAULT 0")
    if 'high_carbs' not in columns:
        conn.execute("ALTER TABLE daily_health_logs ADD COLUMN high_carbs INTEGER DEFAULT 0")
    if 'high_fat' not in columns:
        conn.execute("ALTER TABLE daily_health_logs ADD COLUMN high_fat INTEGER DEFAULT 0")


def _ensure_exercise_resistance_columns(conn):
    """Add resistance workout grouping columns for existing databases."""
    columns = {row['name'] for row in conn.execute("PRAGMA table_info(exercise_resistance)").fetchall()}
    if 'workout_id' not in columns:
        conn.execute("ALTER TABLE exercise_resistance ADD COLUMN workout_id TEXT")
    if 'workout_duration_min' not in columns:
        conn.execute("ALTER TABLE exercise_resistance ADD COLUMN workout_duration_min INTEGER")


def _ensure_dag_node_columns(conn):
    """Add DAG node graph metadata columns for existing databases."""
    table_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'dag_nodes'"
    ).fetchone()
    if not table_exists:
        return

    columns = {row["name"] for row in conn.execute("PRAGMA table_info(dag_nodes)").fetchall()}
    if "branch_key" not in columns:
        conn.execute("ALTER TABLE dag_nodes ADD COLUMN branch_key TEXT")
    if "pos_x" not in columns:
        conn.execute("ALTER TABLE dag_nodes ADD COLUMN pos_x REAL")
    if "pos_y" not in columns:
        conn.execute("ALTER TABLE dag_nodes ADD COLUMN pos_y REAL")


def _ensure_dag_user_state_columns(conn):
    """Add DAG user-state metadata columns for existing databases."""
    table_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'dag_user_node_state'"
    ).fetchone()
    if not table_exists:
        return

    columns = {row["name"] for row in conn.execute("PRAGMA table_info(dag_user_node_state)").fetchall()}
    if "available_since_ts" not in columns:
        conn.execute("ALTER TABLE dag_user_node_state ADD COLUMN available_since_ts TEXT")


def _ensure_dag_graph_columns(conn):
    """Backfill graph_id columns on legacy DAG tables and normalize rows."""
    for table_name in ("dag_nodes", "dag_edges", "dag_node_prereqs", "dag_user_node_state", "dag_user_teasers"):
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        if not exists:
            continue
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if "graph_id" not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN graph_id TEXT")
        conn.execute(
            f"UPDATE {table_name} SET graph_id = COALESCE(NULLIF(TRIM(graph_id), ''), ?)",
            (DEFAULT_DAG_GRAPH_ID,),
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dag_nodes_graph ON dag_nodes(graph_id, tier)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dag_edges_graph ON dag_edges(graph_id, parent_id, child_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dag_prereqs_graph ON dag_node_prereqs(graph_id, node_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dag_user_state_graph ON dag_user_node_state(user_id, graph_id, node_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dag_teasers_graph ON dag_user_teasers(user_id, graph_id, node_id)")

# ============================================================================
# SLEEP OPERATIONS
# ============================================================================

def _calculate_sleep_quality(duration_min: int, wakings_count: int, sleep_onset: str,
                             wake_method: str, rested_level: int, energy: int,
                             wake_mood: int) -> float:
    """Calculate computed sleep quality from measurable metrics."""
    # Duration score (0-100):
    # - Full score from 7.5h onward
    # - No penalty for sleeping over 8h
    # - Penalize only when below 7.5h
    if duration_min >= 450:
        duration_score = 100
    else:
        duration_score = max(0, 100 - (450 - duration_min) / 6)

    continuity_score_map = {0: 100, 1: 80, 2: 60, 3: 40}
    continuity_score = continuity_score_map.get(wakings_count, 20)

    onset_score_map = {'easy': 100, 'normal': 70, 'difficult': 40}
    onset_score = onset_score_map.get((sleep_onset or '').lower(), 70)

    timing_score_map = {'before_630': 90, 'on_time': 100, 'slept_in': 50}
    timing_score = timing_score_map.get((wake_method or '').lower(), 100)

    restoration_score = max(0, min(100, (rested_level - 1) * 25))
    energy_score = max(0, min(100, (energy - 1) * 25))
    mood_score = max(0, min(100, (wake_mood - 1) * 25))

    composite = (
        0.25 * duration_score +
        0.15 * continuity_score +
        0.15 * onset_score +
        0.15 * timing_score +
        0.15 * restoration_score +
        0.10 * energy_score +
        0.05 * mood_score
    )
    return round(composite, 1)

def log_sleep(date: str, bedtime: str, wake_time: str, wake_mood: int,
              energy: int, wakings_count: int = 0, sleep_onset: str = "normal",
              wake_method: str = "on_time", rested_level: int = 3,
              notes: str = ""):
    """Log sleep data."""
    conn = get_connection()
    
    # Calculate duration
    bed_dt = datetime.strptime(f"{date} {bedtime}", "%Y-%m-%d %H:%M")
    wake_dt = datetime.strptime(f"{date} {wake_time}", "%Y-%m-%d %H:%M")
    
    # Handle bedtime after midnight
    if wake_dt < bed_dt:
        from datetime import timedelta
        wake_dt += timedelta(days=1)
    
    duration_min = int((wake_dt - bed_dt).total_seconds() / 60)
    
    # Get targets
    cursor = conn.execute("""
        SELECT key, value
        FROM app_config
        WHERE key IN ('target_bedtime', 'target_wake_time')
    """)
    config = {row['key']: row['value'] for row in cursor.fetchall()}

    sleep_quality = _calculate_sleep_quality(
        duration_min, wakings_count, sleep_onset, wake_method, rested_level, energy, wake_mood
    )

    conn.execute("""
        INSERT INTO sleep_logs 
        (date, bedtime, wake_time, target_bedtime, target_wake_time, 
        duration_minutes, wake_mood, energy, sleep_quality,
        wakings_count, sleep_onset, wake_method, rested_level, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date) DO UPDATE SET
            bedtime = EXCLUDED.bedtime,
            wake_time = EXCLUDED.wake_time,
            target_bedtime = EXCLUDED.target_bedtime,
            target_wake_time = EXCLUDED.target_wake_time,
            duration_minutes = EXCLUDED.duration_minutes,
            wake_mood = EXCLUDED.wake_mood,
            energy = EXCLUDED.energy,
            sleep_quality = EXCLUDED.sleep_quality,
            wakings_count = EXCLUDED.wakings_count,
            sleep_onset = EXCLUDED.sleep_onset,
            wake_method = EXCLUDED.wake_method,
            rested_level = EXCLUDED.rested_level,
            notes = EXCLUDED.notes
    """, (
        date,
        bedtime,
        wake_time,
        config.get('target_bedtime'),
        config.get('target_wake_time'),
        duration_min,
        wake_mood,
        energy,
        sleep_quality,
        wakings_count,
        sleep_onset,
        wake_method,
        rested_level,
        notes
    ))
    
    conn.commit()
    _upsert_cached_dataframe_row(
        "get_sleep_logs",
        {
            "date": date,
            "bedtime": bedtime,
            "wake_time": wake_time,
            "target_bedtime": config.get("target_bedtime"),
            "target_wake_time": config.get("target_wake_time"),
            "duration_minutes": duration_min,
            "wake_mood": wake_mood,
            "energy": energy,
            "sleep_quality": sleep_quality,
            "wakings_count": wakings_count,
            "sleep_onset": sleep_onset,
            "wake_method": wake_method,
            "rested_level": rested_level,
            "notes": notes,
        },
        date_column="date",
        sort_ascending=False,
        limit_key_index=1,
    )
    conn.close()
    for signal in [
        'sleep_logs_count',
        'avg_sleep_quality',
        'avg_sleep_duration_hours',
        'avg_sleep_energy',
        'sleep_quality_days_70',
        'wake_on_time_count',
        'cross_domain_logged_days',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)
    return duration_min

def get_sleep_logs(days: int = 30) -> pd.DataFrame:
    """Get recent sleep logs."""
    key = ("get_sleep_logs", int(days))
    return _cached_read(
        key,
        lambda: _query_dataframe(
            """
            SELECT *
            FROM sleep_logs
            ORDER BY date DESC
            LIMIT ?
            """,
            (int(days),),
        ),
    )

def get_sleep_score(date: str) -> Dict[str, float]:
    """Calculate sleep score for a given date."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM sleep_logs WHERE date = ?", (date,)).fetchone()
    conn.close()
    
    if not row:
        return None
    
    # Duration score (0-100):
    # - Full score from 7.5h onward
    # - No penalty for sleeping over 8h
    # - Penalize only when below 7.5h
    duration = row['duration_minutes']
    if duration >= 450:
        duration_score = 100
    else:
        duration_score = max(0, 100 - (450 - duration) / 6)  # -1 per 6 min under 7.5h

    continuity_score_map = {0: 100, 1: 80, 2: 60, 3: 40}
    continuity_score = continuity_score_map.get(row['wakings_count'] or 0, 20)

    onset_score_map = {'easy': 100, 'normal': 70, 'difficult': 40}
    onset_score = onset_score_map.get((row['sleep_onset'] or 'normal').lower(), 70)

    timing_score_map = {'before_630': 90, 'on_time': 100, 'slept_in': 50}
    timing_score = timing_score_map.get((row['wake_method'] or 'on_time').lower(), 100)

    restoration_score = (max(1, min(5, row['rested_level'] or 3)) - 1) * 25
    energy_score = (max(1, min(5, row['energy'] or 3)) - 1) * 25
    mood_score = (max(1, min(5, row['wake_mood'] or 3)) - 1) * 25

    composite = (
        0.25 * duration_score +
        0.15 * continuity_score +
        0.15 * onset_score +
        0.15 * timing_score +
        0.15 * restoration_score +
        0.10 * energy_score +
        0.05 * mood_score
    )

    return {
        'composite': round(composite, 1),
        'duration_score': round(duration_score, 1),
        'continuity_score': round(continuity_score, 1),
        'onset_score': round(onset_score, 1),
        'timing_score': round(timing_score, 1),
        'restoration_score': round(restoration_score, 1),
        'energy_score': round(energy_score, 1),
        'mood_score': round(mood_score, 1)
    }

# ============================================================================
# HEALTH OPERATIONS
# ============================================================================

def log_resistance(date: str, exercise_type: str, reps: int, sets: int = 1,
                   intensity: int = 3, notes: str = ""):
    """Log resistance training."""
    workout_id = str(uuid.uuid4())
    conn = get_connection()
    conn.execute("""
        INSERT INTO exercise_resistance (date, workout_id, workout_duration_min, exercise_type, reps, sets, intensity, notes)
        VALUES (?, ?, NULL, ?, ?, ?, ?, ?)
    """, (date, workout_id, exercise_type, reps, sets, intensity, notes))
    conn.commit()
    _patch_cached_exercise_summary(
        logged_date=date,
        resistance_deltas={str(exercise_type): float(reps or 0) * float(sets or 0)},
    )
    conn.close()
    for signal in [
        'resistance_reps',
        'active_exercise_days',
        'cross_domain_logged_days',
        'workouts_in_7d',
        'pushups_session',
        'pullups_session',
        'squats_session',
        'plank_seconds_session',
        'hybrid_conditioning_1_sessions',
        'centurion_1_sessions',
        'centurion_2_sessions',
        'iron_centurion_sessions',
        'spartan_endurance_sessions',
        'titan_protocol_sessions',
        'iron_or_titan_sessions',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)


def log_resistance_workout(
    date: str,
    exercises: List[Dict[str, Any]],
    workout_duration_min: Optional[int] = None,
    intensity: int = 3,
    notes: str = "",
):
    """Log one resistance workout containing multiple exercises."""
    cleaned: List[Dict[str, Any]] = []
    for ex in exercises or []:
        ex_type = str(ex.get("exercise_type") or "").strip()
        reps = int(ex.get("reps") or 0)
        sets = int(ex.get("sets") or 0)
        if ex_type and reps > 0 and sets > 0:
            cleaned.append({"exercise_type": ex_type, "reps": reps, "sets": sets})
    if not cleaned:
        raise ValueError("No valid resistance exercises provided.")

    workout_id = str(uuid.uuid4())
    conn = get_connection()
    for ex in cleaned:
        conn.execute(
            """
            INSERT INTO exercise_resistance (date, workout_id, workout_duration_min, exercise_type, reps, sets, intensity, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date,
                workout_id,
                int(workout_duration_min) if workout_duration_min else None,
                ex["exercise_type"],
                ex["reps"],
                ex["sets"],
                int(intensity or 3),
                notes or "",
            ),
        )
    conn.commit()
    resistance_deltas: Dict[str, float] = {}
    for ex in cleaned:
        ex_type = str(ex["exercise_type"])
        resistance_deltas[ex_type] = resistance_deltas.get(ex_type, 0.0) + (
            float(ex["reps"]) * float(ex["sets"])
        )
    _patch_cached_exercise_summary(
        logged_date=date,
        resistance_deltas=resistance_deltas,
    )
    conn.close()

    for signal in [
        'resistance_reps',
        'active_exercise_days',
        'cross_domain_logged_days',
        'workouts_in_7d',
        'pushups_session',
        'pullups_session',
        'squats_session',
        'plank_seconds_session',
        'hybrid_conditioning_1_sessions',
        'centurion_1_sessions',
        'centurion_2_sessions',
        'iron_centurion_sessions',
        'spartan_endurance_sessions',
        'titan_protocol_sessions',
        'iron_or_titan_sessions',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def log_cardio(date: str, type: str, duration_min: int, distance_km: float = None,
               is_intense: bool = False, notes: str = ""):
    """Log cardio exercise."""
    conn = get_connection()
    
    avg_pace = None
    if distance_km and distance_km > 0:
        avg_pace = duration_min / distance_km  # min per km
    
    conn.execute("""
        INSERT INTO exercise_cardio (date, type, duration_min, distance_km, avg_pace, is_intense, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (date, type, duration_min, distance_km, avg_pace, int(is_intense), notes))
    conn.commit()
    _patch_cached_exercise_summary(
        logged_date=date,
        cardio_minutes=float(duration_min or 0),
        cardio_distance=float(distance_km or 0),
    )
    conn.close()
    for signal in [
        'cardio_minutes',
        'active_exercise_days',
        'intense_cardio_days',
        'recovered_sleep_after_intense_ratio',
        'cross_domain_logged_days',
        'workouts_in_7d',
        'run_distance_week',
        'single_run',
        'run_5k_best_seconds',
        'run_3k_best_seconds',
        'run_1k_best_seconds',
        'run_10k_best_seconds',
        'hybrid_conditioning_1_sessions',
        'spartan_endurance_sessions',
        'titan_protocol_sessions',
        'run_5k_within_48h_of_marathon',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def get_exercise_summary(days: int = 7, reference_date: Optional[str] = None) -> Dict[str, Any]:
    """Get exercise summary for last N days."""
    base_date = date.fromisoformat(reference_date or get_brisbane_date())
    days = int(days)
    cutoff = (base_date - timedelta(days=days)).isoformat()
    key = ("get_exercise_summary", days, cutoff)

    def _load():
        conn = get_connection()
        resistance = conn.execute(
            """
            SELECT exercise_type, SUM(reps * sets) as total_reps
            FROM exercise_resistance
            WHERE date >= ?
            GROUP BY exercise_type
            """,
            (cutoff,),
        ).fetchall()
        cardio = conn.execute(
            """
            SELECT SUM(duration_min) as total_minutes, SUM(distance_km) as total_distance
            FROM exercise_cardio
            WHERE date >= ?
            """,
            (cutoff,),
        ).fetchone()
        conn.close()
        return {
            'resistance': {row['exercise_type']: row['total_reps'] for row in resistance},
            'cardio_minutes': cardio['total_minutes'] or 0,
            'cardio_distance': cardio['total_distance'] or 0
        }

    return _cached_read(key, _load)

def log_weight(date: str, weight_kg: float, notes: str = ""):
    """Log weight measurement."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO weight_logs (date, weight_kg, notes)
        VALUES (?, ?, ?)
        ON CONFLICT (date) DO UPDATE SET
            weight_kg = EXCLUDED.weight_kg,
            notes = EXCLUDED.notes
    """, (date, weight_kg, notes))
    conn.commit()
    _upsert_cached_dataframe_row(
        "get_weight_logs",
        {"date": date, "weight_kg": weight_kg, "notes": notes},
        date_column="date",
        sort_ascending=False,
        limit_key_index=1,
    )
    _upsert_cached_dataframe_row(
        "get_weight_trend",
        {"date": date, "weight_kg": weight_kg, "notes": notes},
        date_column="date",
        sort_ascending=True,
        limit_key_index=None,
        cutoff_key_index=2,
    )
    _patch_cached_health_summary(logged_date=date, latest_weight=float(weight_kg))
    conn.close()

def get_latest_weight() -> Optional[float]:
    """Get most recent weight measurement."""
    conn = get_connection()
    result = conn.execute("""
        SELECT weight_kg FROM weight_logs 
        ORDER BY date DESC LIMIT 1
    """).fetchone()
    conn.close()
    return result['weight_kg'] if result else None

def get_latest_weight_entry() -> Optional[Dict[str, Any]]:
    """Get most recent weight entry including date."""
    conn = get_connection()
    result = conn.execute("""
        SELECT date, weight_kg
        FROM weight_logs
        ORDER BY date DESC
        LIMIT 1
    """).fetchone()
    conn.close()
    return dict(result) if result else None

def get_weight_logs(days: int = 30) -> pd.DataFrame:
    """Get recent weight logs."""
    key = ("get_weight_logs", int(days))
    return _cached_read(
        key,
        lambda: _query_dataframe(
            """
            SELECT *
            FROM weight_logs
            ORDER BY date DESC
            LIMIT ?
            """,
            (int(days),),
        ),
    )


def log_daily_health(date: str, steps: int = None, calories: int = None,
                     water_liters: float = None, notes: str = "",
                     high_protein: bool = False, high_carbs: bool = False,
                     high_fat: bool = False):
    """Log daily health metrics (steps, calories, water)."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO daily_health_logs 
        (date, steps, calories, water_liters, notes, high_protein, high_carbs, high_fat)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date) DO UPDATE SET
            steps = EXCLUDED.steps,
            calories = EXCLUDED.calories,
            water_liters = EXCLUDED.water_liters,
            notes = EXCLUDED.notes,
            high_protein = EXCLUDED.high_protein,
            high_carbs = EXCLUDED.high_carbs,
            high_fat = EXCLUDED.high_fat
    """, (
        date, steps, calories, water_liters, notes,
        int(bool(high_protein)), int(bool(high_carbs)), int(bool(high_fat))
    ))
    conn.commit()
    _upsert_cached_list_row(
        "get_recent_daily_health_logs",
        {
            "date": date,
            "steps": steps,
            "calories": calories,
            "water_liters": water_liters,
            "notes": notes,
            "high_protein": int(bool(high_protein)),
            "high_carbs": int(bool(high_carbs)),
            "high_fat": int(bool(high_fat)),
        },
        date_column="date",
        sort_desc=True,
        limit_key_index=1,
    )
    conn.close()
    for signal in [
        'daily_health_logs_count',
        'water_days_2l',
        'calorie_logged_days',
        'steps_days_8k',
        'cross_domain_logged_days',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)


def get_health_summary(reference_date: Optional[str] = None) -> dict:
    """Get health summary metrics."""
    base_date = date.fromisoformat(reference_date or get_brisbane_date())
    cutoff = (base_date - timedelta(days=7)).isoformat()
    key = ("get_health_summary", cutoff)

    def _load():
        conn = get_connection()
        latest_weight = conn.execute(
            """
            SELECT weight_kg, date
            FROM weight_logs
            ORDER BY date DESC LIMIT 1
            """
        ).fetchone()
        avg_calories = conn.execute(
            """
            SELECT AVG(calories) as avg_cal
            FROM daily_health_logs
            WHERE date >= ?
              AND calories IS NOT NULL
            """,
            (cutoff,),
        ).fetchone()
        conn.close()
        return {
            'latest_weight': latest_weight['weight_kg'] if latest_weight else None,
            'latest_weight_date': latest_weight['date'] if latest_weight else None,
            'avg_calories': avg_calories['avg_cal'] if avg_calories and avg_calories['avg_cal'] else 0
        }

    return _cached_read(key, _load)


def get_recent_daily_health_logs(limit: int = 5) -> List[Dict[str, Any]]:
    """Get recent daily health entries ordered newest-first."""
    limit = int(limit)
    key = ("get_recent_daily_health_logs", limit)

    def _load():
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT *
            FROM daily_health_logs
            ORDER BY date DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    return _cached_read(key, _load)


def get_weight_trend(days: int = 90, reference_date: Optional[str] = None) -> pd.DataFrame:
    """Get recent daily weight values ordered ascending for charting."""
    base_date = date.fromisoformat(reference_date or get_brisbane_date())
    days = int(days)
    cutoff = (base_date - timedelta(days=days - 1)).isoformat()
    key = ("get_weight_trend", days, cutoff)
    return _cached_read(
        key,
        lambda: _query_dataframe(
            """
            SELECT date, weight_kg
            FROM weight_logs
            WHERE date >= ?
            ORDER BY date ASC
            """,
            (cutoff,),
        ),
    )

def get_exercise_trend(days: int = 30, reference_date: Optional[str] = None) -> pd.DataFrame:
    """Get daily resistance reps by type with cardio minutes for charting."""
    base_date = date.fromisoformat(reference_date or get_brisbane_date())
    days = int(days)
    cutoff = (base_date - timedelta(days=days - 1)).isoformat()
    key = ("get_exercise_trend", days, cutoff)
    return _cached_read(
        key,
        lambda: _query_dataframe(
            """
            WITH daily_res AS (
                SELECT date, exercise_type, SUM(reps * sets) AS total_reps
                FROM exercise_resistance
                WHERE date >= ?
                GROUP BY date, exercise_type
            ),
            daily_cardio AS (
                SELECT date, SUM(duration_min) AS cardio_min
                FROM exercise_cardio
                WHERE date >= ?
                GROUP BY date
            )
            SELECT
                r.date,
                r.exercise_type,
                r.total_reps,
                COALESCE(c.cardio_min, 0) AS cardio_min
            FROM daily_res r
            LEFT JOIN daily_cardio c ON c.date = r.date
            UNION ALL
            SELECT
                c.date,
                NULL AS exercise_type,
                0 AS total_reps,
                c.cardio_min
            FROM daily_cardio c
            WHERE NOT EXISTS (
                SELECT 1
                FROM daily_res r2
                WHERE r2.date = c.date
            )
            ORDER BY date ASC
            """,
            (cutoff, cutoff),
        ),
    )

# ============================================================================
# JOURNAL OPERATIONS
# ============================================================================

def get_or_create_journal_entry(entry_date: str) -> Dict[str, Any]:
    """Get a journal entry by date, creating a blank one if missing."""
    conn = get_connection()
    row = conn.execute("""
        SELECT *
        FROM journal_entries
        WHERE date = ?
        LIMIT 1
    """, (entry_date,)).fetchone()
    if not row:
        conn.execute("""
            INSERT INTO journal_entries (date, content, submitted)
            VALUES (?, '', 0)
        """, (entry_date,))
        conn.commit()
        row = conn.execute("""
            SELECT *
            FROM journal_entries
            WHERE date = ?
            LIMIT 1
        """, (entry_date,)).fetchone()
    conn.close()
    return dict(row)

def upsert_journal_entry(entry_date: str, content: str):
    """Save journal content for a date."""
    conn = get_connection()
    existing = conn.execute("""
        SELECT id
        FROM journal_entries
        WHERE date = ?
        LIMIT 1
    """, (entry_date,)).fetchone()
    if existing:
        conn.execute("""
            UPDATE journal_entries
            SET content = ?, updated_at = CURRENT_TIMESTAMP
            WHERE date = ?
        """, (content, entry_date))
    else:
        conn.execute("""
            INSERT INTO journal_entries (date, content, submitted)
            VALUES (?, ?, 0)
        """, (entry_date, content))
    conn.commit()
    conn.close()

def get_previous_journal_entries(before_date: str, limit: int = 90) -> List[Dict[str, Any]]:
    """Get previous journal entries before a date (latest first)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT date, content, submitted
        FROM journal_entries
        WHERE date < ?
        ORDER BY date DESC
        LIMIT ?
    """, (before_date, limit)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def submit_journal_entries_before(current_date: str):
    """Auto-submit all unsent entries before the current day."""
    conn = get_connection()
    conn.execute("""
        UPDATE journal_entries
        SET submitted = 1, updated_at = CURRENT_TIMESTAMP
        WHERE date < ? AND submitted = 0
    """, (current_date,))
    conn.commit()
    conn.close()

def get_daily_snapshot(entry_date: str) -> Dict[str, Any]:
    """Collect daily cross-domain metrics for the journal sidebar cards."""
    conn = get_connection()

    sleep_row = conn.execute("""
        SELECT duration_minutes, sleep_quality, energy, notes
        FROM sleep_logs
        WHERE date = ?
        LIMIT 1
    """, (entry_date,)).fetchone()

    resistance_row = conn.execute("""
        SELECT SUM(reps * sets) AS total_reps
        FROM exercise_resistance
        WHERE date = ?
    """, (entry_date,)).fetchone()
    cardio_row = conn.execute("""
        SELECT SUM(duration_min) AS cardio_min
        FROM exercise_cardio
        WHERE date = ?
    """, (entry_date,)).fetchone()
    health_row = conn.execute("""
        SELECT steps, calories, water_liters, high_protein, high_carbs, high_fat, notes
        FROM daily_health_logs
        WHERE date = ?
        LIMIT 1
    """, (entry_date,)).fetchone()
    project_row = conn.execute("""
        SELECT SUM(duration_min) AS total_min, COUNT(*) AS sessions
        FROM project_sessions
        WHERE date = ?
    """, (entry_date,)).fetchone()
    exercise_notes_rows = conn.execute("""
        SELECT note
        FROM (
            SELECT notes AS note, created_at
            FROM exercise_resistance
            WHERE date = ? AND TRIM(COALESCE(notes, '')) <> ''
            UNION ALL
            SELECT notes AS note, created_at
            FROM exercise_cardio
            WHERE date = ? AND TRIM(COALESCE(notes, '')) <> ''
        )
        ORDER BY created_at DESC
        LIMIT 3
    """, (entry_date, entry_date)).fetchall()
    project_notes_rows = conn.execute("""
        SELECT micro_output
        FROM project_sessions
        WHERE date = ? AND TRIM(COALESCE(micro_output, '')) <> ''
        ORDER BY id DESC
        LIMIT 3
    """, (entry_date,)).fetchall()

    conn.close()

    return {
        'sleep': dict(sleep_row) if sleep_row else None,
        'exercise': {
            'resistance_reps': int((resistance_row['total_reps'] or 0) if resistance_row else 0),
            'cardio_min': int((cardio_row['cardio_min'] or 0) if cardio_row else 0),
            'notes': [row['note'] for row in exercise_notes_rows] if exercise_notes_rows else [],
        },
        'diet': dict(health_row) if health_row else None,
        'projects': {
            'minutes': int((project_row['total_min'] or 0) if project_row else 0),
            'sessions': int((project_row['sessions'] or 0) if project_row else 0),
            'notes': [row['micro_output'] for row in project_notes_rows] if project_notes_rows else [],
        },
    }

# ============================================================================
# PROJECT OPERATIONS
# ============================================================================

def create_project(name: str, category: str, tier: int, importance: int,
                   thesis: str = "", status: str = "active") -> int:
    """Create a new project with tier."""
    conn = get_connection()
    today = get_brisbane_date()
    cursor = conn.execute("""
        INSERT INTO projects (name, category, tier, priority, importance, status, thesis, last_touched)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
    """, (name, category, tier, tier, importance, status, thesis, today))
    project_id = cursor.fetchone()['id']
    conn.commit()
    conn.close()
    return project_id

def log_project_session(project_id: int, date: str, duration_min: int,
                        micro_output: str = "", completed: bool = False):
    """Log a project work session."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO project_sessions (project_id, date, duration_min, micro_output, completed)
        VALUES (?, ?, ?, ?, ?)
    """, (project_id, date, duration_min, micro_output, int(completed)))
    
    # Update last_touched
    conn.execute("""
        UPDATE projects SET last_touched = ? WHERE id = ?
    """, (date, project_id))
    
    conn.commit()
    conn.close()
    for signal in [
        'project_sessions_count',
        'project_minutes',
        'next_day_project_after_evening_ratio',
        'cross_domain_logged_days',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def update_project(project_id: int, name: str = None, category: str = None, 
                   tier: int = None, importance: int = None, thesis: str = None, 
                   status: str = None):
    """Update project details."""
    conn = get_connection()
    
    # Build dynamic update query
    updates = []
    params = []
    
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if category is not None:
        updates.append("category = ?")
        params.append(category)
    if tier is not None:
        updates.append("tier = ?")
        params.append(tier)
        updates.append("priority = ?")  # Keep in sync
        params.append(tier)
    if importance is not None:
        updates.append("importance = ?")
        params.append(importance)
    if thesis is not None:
        updates.append("thesis = ?")
        params.append(thesis)
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    
    if updates:
        params.append(project_id)
        query = f"UPDATE projects SET {', '.join(updates)} WHERE id = ?"
        conn.execute(query, params)
        conn.commit()
    
    conn.close()

def archive_project(project_id: int):
    """Archive a project (soft delete)."""
    conn = get_connection()
    conn.execute("UPDATE projects SET status = 'archived' WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()

def get_active_projects() -> List[Dict]:
    """Get all active projects with session counts, grouped by tier."""
    key = ("get_active_projects",)

    def _load():
        conn = get_connection()
        projects = conn.execute(
            """
            SELECT p.*, 
                   COUNT(s.id) as session_count,
                   SUM(s.duration_min) as total_minutes,
                   julianday('now') - julianday(p.last_touched) as days_since_touched
            FROM projects p
            LEFT JOIN project_sessions s ON p.id = s.project_id
            WHERE p.status = 'active'
            GROUP BY p.id
            ORDER BY p.tier, p.last_touched DESC
            """
        ).fetchall()
        conn.close()
        return [dict(row) for row in projects]

    return _cached_read(key, _load)

def get_project_summary() -> Dict[str, Any]:
    """Get summary stats for projects."""
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    week_start = monday.isoformat()
    key = ("get_project_summary", week_start)

    def _load():
        conn = get_connection()
        active_count = conn.execute(
            "SELECT COUNT(*) as count FROM projects WHERE status = 'active'"
        ).fetchone()
        time_this_week = conn.execute(
            """
            SELECT SUM(duration_min) as total
            FROM project_sessions
            WHERE date >= ?
            """,
            (week_start,),
        ).fetchone()
        touched_this_week = conn.execute(
            """
            SELECT COUNT(DISTINCT project_id) as count
            FROM project_sessions
            WHERE date >= ?
            """,
            (week_start,),
        ).fetchone()
        tier1_count = conn.execute(
            """
            SELECT COUNT(*) as count
            FROM projects
            WHERE status = 'active' AND tier = 1
            """
        ).fetchone()
        conn.close()
        return {
            'active_projects': active_count['count'],
            'time_this_week': time_this_week['total'] or 0,
            'touched_this_week': touched_this_week['count'],
            'tier1_projects': tier1_count['count']
        }

    return _cached_read(key, _load)

def get_all_tasks() -> List[Dict]:
    """Get all tasks ordered by index."""
    key = ("get_all_tasks",)

    def _load():
        conn = get_connection()
        tasks = conn.execute(
            """
            SELECT *
            FROM tasks
            ORDER BY completed, order_index
            """
        ).fetchall()
        conn.close()
        return [dict(row) for row in tasks]

    return _cached_read(key, _load)

def add_task(text: str) -> int:
    """Add a new task."""
    conn = get_connection()
    
    # Get next order index
    max_order = conn.execute("SELECT MAX(order_index) as max_order FROM tasks").fetchone()
    next_order = (max_order['max_order'] or 0) + 1
    
    cursor = conn.execute("""
        INSERT INTO tasks (text, order_index)
        VALUES (?, ?)
        RETURNING id
    """, (text, next_order))
    task_id = cursor.fetchone()['id']
    conn.commit()
    conn.close()
    return task_id

def toggle_task(task_id: int):
    """Toggle task completion status."""
    conn = get_connection()
    conn.execute("""
        UPDATE tasks 
        SET completed = CASE WHEN completed = 0 THEN 1 ELSE 0 END
        WHERE id = ?
    """, (task_id,))
    conn.commit()
    conn.close()

def set_task_completed(task_id: int, completed: bool):
    """Set task completion status explicitly."""
    conn = get_connection()
    conn.execute("""
        UPDATE tasks
        SET completed = ?
        WHERE id = ?
    """, (1 if completed else 0, task_id))
    conn.commit()
    conn.close()

def delete_task(task_id: int):
    """Delete a task."""
    conn = get_connection()
    conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()

def clear_completed_tasks():
    """Remove completed tasks (used at day rollover)."""
    conn = get_connection()
    conn.execute("DELETE FROM tasks WHERE completed = 1")
    conn.commit()
    conn.close()

# ============================================================================
# FINANCE OPERATIONS
# ============================================================================

def log_weekly_finance(
    week_start_date: str,
    week_end_date: str,
    income: float,
    essentials: float,
    discretionary: float,
    notes: str = ""
):
    """Insert a weekly finance entry."""
    savings = income - essentials - discretionary

    conn = get_connection()
    conn.execute("""
        INSERT INTO weekly_finance (
            week_start_date, week_end_date, income, essentials, discretionary, savings, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (week_start_date, week_end_date, income, essentials, discretionary, savings, notes))
    conn.commit()
    conn.close()
    for signal in [
        'weekly_finance_entries_count',
        'weekly_non_negative_savings_count',
        'discretionary_ratio_weeks_35',
        'cross_domain_logged_days',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def log_finance_decision(date: str, decision: str, expected_outcome: str, confidence: int):
    """Log a financial decision."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO finance_decisions (date, decision, expected_outcome, confidence)
        VALUES (?, ?, ?, ?)
    """, (date, decision, expected_outcome, confidence))
    conn.commit()
    conn.close()

def get_weekly_finance_summary() -> Dict[str, Any]:
    """Get weekly finance summary statistics for finance page cards."""
    conn = get_connection()

    avg_weekly = conn.execute("""
        SELECT AVG(savings) AS avg_weekly_savings
        FROM (
            SELECT savings
            FROM weekly_finance
            ORDER BY week_end_date DESC
            LIMIT 4
        )
    """).fetchone()

    month_total = conn.execute("""
        SELECT SUM(savings) AS total_this_month
        FROM weekly_finance
        WHERE strftime('%Y-%m', week_end_date) = strftime('%Y-%m', 'now')
    """).fetchone()

    best_week = conn.execute("""
        SELECT MAX(savings) AS best_week_savings
        FROM weekly_finance
    """).fetchone()

    weeks_count = conn.execute("""
        SELECT COUNT(*) AS weeks_logged
        FROM weekly_finance
    """).fetchone()

    conn.close()

    return {
        'avg_weekly_savings': round(avg_weekly['avg_weekly_savings'] or 0, 2),
        'total_this_month': round(month_total['total_this_month'] or 0, 2),
        'best_week_savings': round(best_week['best_week_savings'] or 0, 2),
        'weeks_logged': weeks_count['weeks_logged'] or 0
    }

def get_all_weekly_entries(limit: int = 20) -> List[Dict]:
    """Get recent weekly finance entries."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT *
        FROM weekly_finance
        ORDER BY week_end_date DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def update_weekly_finance(
    entry_id: int,
    income: float,
    essentials: float,
    discretionary: float,
    notes: str = ""
):
    """Update a weekly finance entry and recompute savings."""
    savings = income - essentials - discretionary
    conn = get_connection()
    conn.execute("""
        UPDATE weekly_finance
        SET income = ?, essentials = ?, discretionary = ?, savings = ?, notes = ?
        WHERE id = ?
    """, (income, essentials, discretionary, savings, notes, entry_id))
    conn.commit()
    conn.close()
    for signal in [
        'weekly_finance_entries_count',
        'weekly_non_negative_savings_count',
        'discretionary_ratio_weeks_35',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def delete_weekly_finance(entry_id: int):
    """Delete a weekly finance entry and recalculate finance XP."""
    conn = get_connection()

    entry = conn.execute("""
        SELECT week_end_date
        FROM weekly_finance
        WHERE id = ?
    """, (entry_id,)).fetchone()

    if not entry:
        conn.close()
        return

    conn.execute("""
        DELETE FROM xp_logs
        WHERE date = ? AND domain = 'finance' AND activity = 'weekly_log'
    """, (entry['week_end_date'],))

    conn.execute("DELETE FROM weekly_finance WHERE id = ?", (entry_id,))

    total_xp = conn.execute("""
        SELECT SUM(xp_gained) AS total
        FROM xp_logs
        WHERE domain = 'finance'
    """).fetchone()

    conn.execute("""
        UPDATE identity_levels
        SET xp = ?, updated_at = datetime('now')
        WHERE domain = 'finance'
    """, (total_xp['total'] or 0,))

    conn.commit()
    conn.close()
    for signal in [
        'weekly_finance_entries_count',
        'weekly_non_negative_savings_count',
        'discretionary_ratio_weeks_35',
    ]:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)

def get_finance_summary(reference_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Backward-compatible finance summary for pages that still use legacy keys.
    Values are now derived from weekly_finance.
    """
    base_date = date.fromisoformat(reference_date or get_brisbane_date())
    cutoff_3mo = (base_date - timedelta(days=90)).isoformat()
    key = ("get_finance_summary", cutoff_3mo)

    def _load():
        conn = get_connection()
        recent = conn.execute(
            """
            SELECT income, savings
            FROM weekly_finance
            ORDER BY week_end_date DESC
            LIMIT 1
            """
        ).fetchone()
        avg = conn.execute(
            """
            SELECT AVG(savings) AS avg_savings
            FROM (
                SELECT savings
                FROM weekly_finance
                ORDER BY week_end_date DESC
                LIMIT 4
            )
            """
        ).fetchone()
        total_3mo = conn.execute(
            """
            SELECT SUM(savings) AS total_savings
            FROM weekly_finance
            WHERE week_end_date >= ?
            """,
            (cutoff_3mo,),
        ).fetchone()
        conn.close()

        if not recent:
            return {
                'savings_rate': 0,
                'avg_savings': 0,
                'total_savings_3mo': 0,
                'non_salary_income_3mo': 0
            }
        savings_rate = (recent['savings'] / recent['income'] * 100) if recent['income'] else 0
        return {
            'savings_rate': round(savings_rate, 1),
            'avg_savings': round(avg['avg_savings'] or 0, 2),
            'total_savings_3mo': round(total_3mo['total_savings'] or 0, 2),
            'non_salary_income_3mo': 0
        }

    return _cached_read(key, _load)

def add_asset(name: str, category: str, current_value: float, date_updated: str, notes: str = ""):
    """Add or update an asset."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO assets (name, category, current_value, date_updated, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (name, category, current_value, date_updated, notes))
    conn.commit()
    conn.close()

def update_asset(asset_id: int, name: str, category: str, current_value: float, date_updated: str, notes: str = ""):
    """Update an existing asset."""
    conn = get_connection()
    conn.execute("""
        UPDATE assets 
        SET name = ?, category = ?, current_value = ?, date_updated = ?, notes = ?
        WHERE id = ?
    """, (name, category, current_value, date_updated, notes, asset_id))
    conn.commit()
    conn.close()

def delete_asset(asset_id: int):
    """Delete an asset."""
    conn = get_connection()
    conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
    conn.commit()
    conn.close()

def get_all_assets() -> List[Dict]:
    """Get all assets."""
    conn = get_connection()
    assets = conn.execute("""
        SELECT * FROM assets 
        ORDER BY category, name
    """).fetchall()
    conn.close()
    return [dict(row) for row in assets]

def add_liability(name: str, category: str, current_value: float, date_updated: str, notes: str = ""):
    """Add or update a liability."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO liabilities (name, category, current_value, date_updated, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (name, category, current_value, date_updated, notes))
    conn.commit()
    conn.close()

def update_liability(liability_id: int, name: str, category: str, current_value: float, date_updated: str, notes: str = ""):
    """Update an existing liability."""
    conn = get_connection()
    conn.execute("""
        UPDATE liabilities 
        SET name = ?, category = ?, current_value = ?, date_updated = ?, notes = ?
        WHERE id = ?
    """, (name, category, current_value, date_updated, notes, liability_id))
    conn.commit()
    conn.close()

def delete_liability(liability_id: int):
    """Delete a liability."""
    conn = get_connection()
    conn.execute("DELETE FROM liabilities WHERE id = ?", (liability_id,))
    conn.commit()
    conn.close()

def get_all_liabilities() -> List[Dict]:
    """Get all liabilities."""
    conn = get_connection()
    liabilities = conn.execute("""
        SELECT * FROM liabilities 
        ORDER BY category, name
    """).fetchall()
    conn.close()
    return [dict(row) for row in liabilities]

def get_net_worth() -> Dict[str, float]:
    """Calculate current net worth."""
    key = ("get_net_worth",)

    def _load():
        conn = get_connection()
        total_assets = conn.execute("SELECT SUM(current_value) as total FROM assets").fetchone()
        total_liabilities = conn.execute("SELECT SUM(current_value) as total FROM liabilities").fetchone()
        conn.close()
        assets = total_assets['total'] or 0
        liabilities = total_liabilities['total'] or 0
        return {
            'total_assets': assets,
            'total_liabilities': liabilities,
            'net_worth': assets - liabilities
        }

    return _cached_read(key, _load)


# ============================================================================
# XP AND IDENTITY OPERATIONS
# ============================================================================

def log_xp(date: str, domain: str, activity: str, xp_gained: int, 
           multiplier: float = 1.0, notes: str = ""):
    """Log XP gain."""
    conn = get_connection()
    final_xp = int(xp_gained * multiplier)
    
    conn.execute("""
        INSERT INTO xp_logs (date, domain, activity, xp_gained, multiplier, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (date, domain, activity, final_xp, multiplier, notes))
    
    # Update identity level XP
    conn.execute("""
        UPDATE identity_levels 
        SET xp = xp + ?, updated_at = datetime('now')
        WHERE domain = ?
    """, (final_xp, domain))
    
    conn.commit()
    conn.close()
    return final_xp

def get_identity_levels() -> List[Dict]:
    """Get current identity levels for all domains."""
    key = ("get_identity_levels",)
    def _load():
        conn = get_connection()
        levels = conn.execute("SELECT * FROM identity_levels ORDER BY domain").fetchall()
        conn.close()
        return [dict(row) for row in levels]

    return _cached_read(key, _load)

def update_identity_level(domain: str, new_level: int, new_name: str):
    """Update identity level when threshold is reached."""
    conn = get_connection()
    conn.execute("""
        UPDATE identity_levels 
        SET level = ?, level_name = ?, updated_at = datetime('now')
        WHERE domain = ?
    """, (new_level, new_name, domain))
    conn.commit()
    conn.close()

def get_overall_level() -> Dict[str, Any]:
    """Get overall level across all domains."""
    key = ("get_overall_level",)

    def _load():
        conn = get_connection()
        total_xp = conn.execute(
            """
            SELECT SUM(domain_xp) as total
            FROM (
                SELECT domain, MAX(xp) AS domain_xp
                FROM identity_levels
                GROUP BY domain
            )
            """
        ).fetchone()
        conn.close()

        total = total_xp['total'] or 0
        level = (total // GLOBAL_XP_PER_LEVEL) + 1
        xp_in_level = total % GLOBAL_XP_PER_LEVEL
        xp_needed = GLOBAL_XP_PER_LEVEL
        percentage = (xp_in_level / xp_needed * 100) if xp_needed > 0 else 0

        reached_500_count = level // 100
        reached_100_count = min(level // 10, 9)
        total_rewards_value = reached_100_count * 100 + reached_500_count * 500

        if level < 10:
            next_milestone_level = 10
        elif level < 90:
            next_milestone_level = ((level // 10) + 1) * 10
        elif level < 100:
            next_milestone_level = 100
        else:
            next_milestone_level = ((level // 100) + 1) * 100

        next_milestone_reward = 500 if next_milestone_level % 100 == 0 else 100
        xp_to_next_milestone = max(0, (next_milestone_level - level) * GLOBAL_XP_PER_LEVEL - xp_in_level)

        if level >= 100:
            last_milestone_level = (level // 100) * 100
            last_milestone_reward = 500
        elif level >= 10:
            last_milestone_level = (level // 10) * 10
            last_milestone_reward = 100
        else:
            last_milestone_level = None
            last_milestone_reward = None

        return {
            'level': level,
            'name': f"Level {level}",
            'total_xp': total,
            'current_threshold': total - xp_in_level,
            'next_threshold': xp_needed,
            'next_level': level + 1,
            'xp_in_level': xp_in_level,
            'xp_per_level': GLOBAL_XP_PER_LEVEL,
            'percentage': percentage,
            'max_level': False,
            'milestones': {
                'last_level': last_milestone_level,
                'last_reward': last_milestone_reward,
                'next_level': next_milestone_level,
                'next_reward': next_milestone_reward,
                'xp_to_next': xp_to_next_milestone,
                'count_100': reached_100_count,
                'count_500': reached_500_count,
                'total_rewards_value': total_rewards_value
            }
        }

    return _cached_read(key, _load)

# ============================================================================
# QUEST OPERATIONS
# ============================================================================

def generate_daily_quests(date: str) -> List[Dict]:
    """Generate 3 daily quests (exactly easy + medium + hard) for a date."""
    quest_pool = {
        'easy': [
            {'text': 'Hydration Check: Drink at least 2L of water', 'type': 'health', 'xp_reward': 40},
            {'text': 'Move Break: Take a 20-minute walk', 'type': 'exercise', 'xp_reward': 45},
            {'text': 'Quick Lift: Complete 30 total push-ups', 'type': 'exercise', 'xp_reward': 50},
            {'text': 'Focus Sprint: 30-minute project session', 'type': 'projects', 'xp_reward': 50},
            {'text': 'Money Check-in: Log one spending decision', 'type': 'finance', 'xp_reward': 40},
            {'text': 'Sleep Hygiene: Complete your bedtime routine', 'type': 'sleep', 'xp_reward': 45},
        ],
        'medium': [
            {'text': 'Cardio Builder: 30-minute cardio session', 'type': 'exercise', 'xp_reward': 85},
            {'text': 'Strength Builder: 80 total resistance reps', 'type': 'exercise', 'xp_reward': 90},
            {'text': 'Deep Work: 90-minute focused project session', 'type': 'projects', 'xp_reward': 100},
            {'text': 'Nutrition Discipline: Track calories today', 'type': 'health', 'xp_reward': 80},
            {'text': 'Sleep Consistency: Lights out within your target window', 'type': 'sleep', 'xp_reward': 90},
            {'text': "Budget Control: Spend within today's planned budget", 'type': 'finance', 'xp_reward': 95},
        ],
        'hard': [
            {'text': 'Century Club: Complete 100 push-ups today', 'type': 'exercise', 'xp_reward': 150},
            {'text': 'Cardio Endurance: 60-minute cardio session', 'type': 'exercise', 'xp_reward': 170},
            {'text': 'Deep Work Marathon: 3-hour project session', 'type': 'projects', 'xp_reward': 200},
            {'text': 'Leg Day Challenge: 200 squat reps total', 'type': 'exercise', 'xp_reward': 180},
            {'text': 'Perfect Tracking: Log steps, calories, and water today', 'type': 'health', 'xp_reward': 160},
            {'text': 'No-Impulse Day: Zero unplanned purchases', 'type': 'finance', 'xp_reward': 170},
        ],
    }

    # Deterministic randomization per date: random across days, stable within same day.
    rng = random.Random(date)
    preferred_types = _get_frontier_preferred_quest_types(DAG_DEFAULT_USER_ID)
    quests = []
    for difficulty in ['easy', 'medium', 'hard']:
        candidates = quest_pool[difficulty]
        if preferred_types:
            targeted = [q for q in candidates if q['type'] in preferred_types]
            if targeted:
                candidates = targeted
        selected = rng.choice(candidates).copy()
        selected['difficulty'] = difficulty
        quests.append(selected)
    
    conn = get_connection()
    
    # Check if quests already exist for this date
    existing = conn.execute("SELECT COUNT(*) as count FROM daily_quests WHERE date = ?", (date,)).fetchone()
    
    if existing['count'] == 0:
        for quest in quests:
            conn.execute("""
                INSERT INTO daily_quests (date, quest_text, quest_type, difficulty, xp_reward)
                VALUES (?, ?, ?, ?, ?)
            """, (date, quest['text'], quest['type'], quest['difficulty'], quest['xp_reward']))
        conn.commit()
    
    # Return quests for this date
    result = conn.execute("SELECT * FROM daily_quests WHERE date = ?", (date,)).fetchall()
    conn.close()
    
    return [dict(row) for row in result]

def get_daily_quests(date: str) -> List[Dict]:
    """Get quests for a specific date."""
    key = ("get_daily_quests", date)

    def _load():
        conn = get_connection()
        result = conn.execute(
            """
            SELECT * FROM daily_quests
            WHERE date = ?
            ORDER BY id
            """,
            (date,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in result]

    return _cached_read(key, _load)

def _quest_domain(quest_type: str) -> str:
    """Map quest type to XP domain."""
    domain_map = {
        'sleep': 'sleep',
        'exercise': 'health',
        'health': 'health',
        'projects': 'projects',
        'finance': 'finance',
    }
    return domain_map.get((quest_type or '').lower(), 'health')

def _recalculate_domain_identity(domain: str):
    """Recalculate domain XP/level from xp_logs (supports both gain and rollback)."""
    conn = get_connection()

    total_row = conn.execute("""
        SELECT SUM(xp_gained) as total
        FROM xp_logs
        WHERE domain = ?
    """, (domain,)).fetchone()
    total_xp = int(total_row['total'] or 0)

    identity = conn.execute("""
        SELECT id FROM identity_levels WHERE domain = ?
    """, (domain,)).fetchone()

    if not identity:
        conn.close()
        return

    new_level = (total_xp // DOMAIN_XP_PER_LEVEL) + 1
    new_name = f"Level {new_level}"

    conn.execute("""
        UPDATE identity_levels
        SET xp = ?, level = ?, level_name = ?, updated_at = datetime('now')
        WHERE domain = ?
    """, (total_xp, new_level, new_name, domain))
    conn.commit()
    conn.close()

def recalculate_identity_levels_from_logs():
    """Rebuild identity XP/levels from xp_logs for all tracked domains."""
    conn = get_connection()
    domains = [row['domain'] for row in conn.execute(
        "SELECT DISTINCT domain FROM identity_levels ORDER BY domain"
    ).fetchall()]
    conn.close()

    for domain in domains:
        _recalculate_domain_identity(domain)

def complete_quest(quest_id: int):
    """Mark a quest as completed and award XP once."""
    conn = get_connection()
    quest = conn.execute("""
        SELECT id, date, quest_text, quest_type, xp_reward, completed
        FROM daily_quests
        WHERE id = ?
    """, (quest_id,)).fetchone()

    if not quest:
        conn.close()
        return 0

    if quest['completed']:
        conn.close()
        return 0

    conn.execute("UPDATE daily_quests SET completed = 1 WHERE id = ?", (quest_id,))
    conn.commit()
    conn.close()

    domain = _quest_domain(quest['quest_type'])
    activity = f"daily_quest_{quest_id}"

    # Award XP only once for this quest/activity.
    conn = get_connection()
    existing = conn.execute("""
        SELECT id FROM xp_logs
        WHERE domain = ? AND activity = ?
        LIMIT 1
    """, (domain, activity)).fetchone()
    conn.close()

    if existing:
        return 0

    gained = log_xp(
        quest['date'],
        domain,
        activity,
        int(quest['xp_reward'] or 0),
        1.0,
        notes=quest['quest_text'] or ""
    )
    _recalculate_domain_identity(domain)
    dag_eval_for_signal(DAG_DEFAULT_USER_ID, 'quest_completions_count')
    return gained

def uncomplete_quest(quest_id: int):
    """Uncheck a quest and rollback XP previously awarded for it."""
    conn = get_connection()
    quest = conn.execute("""
        SELECT id, quest_type, completed
        FROM daily_quests
        WHERE id = ?
    """, (quest_id,)).fetchone()

    if not quest:
        conn.close()
        return 0

    if not quest['completed']:
        conn.close()
        return 0

    domain = _quest_domain(quest['quest_type'])
    activity = f"daily_quest_{quest_id}"

    gained_row = conn.execute("""
        SELECT SUM(xp_gained) as total
        FROM xp_logs
        WHERE domain = ? AND activity = ?
    """, (domain, activity)).fetchone()
    removed_xp = int(gained_row['total'] or 0)

    conn.execute("UPDATE daily_quests SET completed = 0 WHERE id = ?", (quest_id,))
    conn.execute("DELETE FROM xp_logs WHERE domain = ? AND activity = ?", (domain, activity))
    conn.commit()
    conn.close()

    _recalculate_domain_identity(domain)
    dag_eval_for_signal(DAG_DEFAULT_USER_ID, 'quest_completions_count')
    return removed_xp

# ============================================================================
# PATTERN DISCOVERY
# ============================================================================

def discover_correlations() -> List[Dict]:
    """Discover correlations between different metrics."""
    key = ("discover_correlations",)

    def _load():
        query = """
            SELECT 
                s.date,
                s.duration_minutes,
                s.energy,
                s.sleep_quality,
                (SELECT SUM(duration_min) FROM exercise_cardio e WHERE e.date = s.date) as cardio_min,
                (SELECT SUM(reps * sets) FROM exercise_resistance r WHERE r.date = s.date AND r.exercise_type = 'pushups') as pushups
            FROM sleep_logs s
            WHERE s.date >= date('now', '-30 days')
            ORDER BY s.date DESC
        """
        df = _query_dataframe(query)
        patterns = []
        if len(df) > 10 and df['cardio_min'].notna().sum() > 5:
            corr = df['cardio_min'].corr(df['energy'])
            if abs(corr) > 0.3:
                patterns.append({
                    'type': 'correlation',
                    'description': f"Cardio exercise and next-day energy: {corr:.2f} correlation",
                    'metric1': 'cardio_minutes',
                    'metric2': 'energy',
                    'correlation_value': corr,
                    'significance': 'moderate' if abs(corr) > 0.5 else 'weak'
                })
        return patterns

    return _cached_read(key, _load)

# =============================================================================
# ROUTINES OPERATIONS
# =============================================================================

def get_routine_templates() -> List[Dict]:
    """Get all active routine templates."""
    key = ("get_routine_templates",)

    def _load():
        conn = get_connection()
        templates = conn.execute(
            """
            SELECT * FROM routine_templates
            WHERE active = 1
            ORDER BY time_available
            """
        ).fetchall()
        conn.close()
        return [dict(row) for row in templates]

    return _cached_read(key, _load)

def get_routine_items(template_id: int) -> List[Dict]:
    """Get all items for a routine template."""
    template_id = int(template_id)
    key = ("get_routine_items", template_id)

    def _load():
        conn = get_connection()
        items = conn.execute(
            """
            SELECT ri.*
            FROM routine_items ri
            LEFT JOIN routine_items parent
              ON parent.id = ri.parent_item_id
             AND parent.active = 1
             AND parent.template_id = ri.template_id
            WHERE ri.template_id = ?
              AND ri.active = 1
              AND (ri.parent_item_id IS NULL OR parent.id IS NOT NULL)
            ORDER BY order_index
            """,
            (template_id,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in items]

    return _cached_read(key, _load)

def update_routine_item_progress(date: str, template_id: int, item_id: int, completed: bool):
    """Update progress for a routine item."""
    conn = get_connection()
    
    
    conn.execute("""
        INSERT INTO daily_routine_progress (date, template_id, item_id, completed)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (date, template_id, item_id) DO UPDATE SET
            completed = EXCLUDED.completed
    """, (date, template_id, item_id, int(completed)))
    
    conn.commit()
    conn.close()

def is_routine_submitted(date: str, template_id: int) -> bool:
    """Check if a routine has been submitted for the day."""
    template_id = int(template_id)
    key = ("is_routine_submitted", date, template_id)

    def _load():
        conn = get_connection()
        result = conn.execute(
            """
            SELECT id FROM routine_submissions
            WHERE date = ? AND template_id = ?
            """,
            (date, template_id),
        ).fetchone()
        conn.close()
        return result is not None

    return _cached_read(key, _load)

def submit_routine(date: str, template_id: int) -> int:
    """Mark routine as submitted (XP awarded at midnight). Returns total XP that will be awarded."""
    conn = get_connection()
    total_xp, all_completed = _calculate_routine_totals(conn, date, template_id)
    
    # Record submission (but don't award XP yet)
    conn.execute("""
        INSERT INTO routine_submissions 
        (date, template_id, submitted_at, all_completed, total_xp)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (date, template_id) DO UPDATE SET
            submitted_at = EXCLUDED.submitted_at,
            all_completed = EXCLUDED.all_completed,
            total_xp = EXCLUDED.total_xp
    """, (date, template_id, datetime.now().isoformat(), int(all_completed), total_xp))
    
    conn.commit()
    conn.close()
    for signal in ['routine_submissions_count', 'evening_routine_submissions_count', 'cross_domain_logged_days']:
        dag_eval_for_signal(DAG_DEFAULT_USER_ID, signal)
    
    return total_xp

def _calculate_routine_totals(conn, date: str, template_id: int) -> tuple[int, bool]:
    """Calculate routine XP totals using only visible/valid active items."""
    items = conn.execute(
        """
        SELECT ri.xp_value, COALESCE(drp.completed, 0) AS completed
        FROM routine_items ri
        LEFT JOIN routine_items parent
          ON parent.id = ri.parent_item_id
         AND parent.active = 1
         AND parent.template_id = ri.template_id
        LEFT JOIN daily_routine_progress drp
          ON ri.id = drp.item_id
         AND drp.date = ?
         AND drp.template_id = ?
        WHERE ri.template_id = ?
          AND ri.active = 1
          AND (ri.parent_item_id IS NULL OR parent.id IS NOT NULL)
        """,
        (date, template_id, template_id),
    ).fetchall()

    total_xp = sum(item['xp_value'] for item in items if item['completed'])
    all_completed = len(items) > 0 and all(item['completed'] for item in items)
    if all_completed:
        template = conn.execute(
            "SELECT bonus_xp FROM routine_templates WHERE id = ?",
            (template_id,),
        ).fetchone()
        if template:
            total_xp += int(template['bonus_xp'] or 0)
    return int(total_xp), bool(all_completed)

def unsubmit_routine(date: str, template_id: int):
    """Unsubmit a routine (before midnight). Progress is preserved."""
    conn = get_connection()
    
    # Only delete the submission record, NOT the progress
    conn.execute("""
        DELETE FROM routine_submissions 
        WHERE date = ? AND template_id = ?
    """, (date, template_id))
    
    conn.commit()
    conn.close()

def award_pending_routine_xp():
    """Award XP for all submitted routines from previous days."""
    from analytics.scoring import award_xp
    
    conn = get_connection()
    
    # Get today in Brisbane timezone
    today_brisbane = get_brisbane_date()
    
    # Find all submissions from yesterday or earlier
    pending = conn.execute("""
        SELECT rs.*, rt.name as routine_name
        FROM routine_submissions rs
        JOIN routine_templates rt ON rs.template_id = rt.id
        WHERE rs.date < ?
    """, (today_brisbane,)).fetchall()
    
    awarded_count = 0
    for submission in pending:
        activity_key = f"routine_{submission['template_id']}_{submission['date']}"
        existing = conn.execute("""
            SELECT id
            FROM xp_logs
            WHERE domain = 'health' AND activity = ?
            LIMIT 1
        """, (activity_key,)).fetchone()
        if existing:
            continue

        # Recompute with current valid items/progress to avoid stale submission snapshots.
        recalculated_xp, _ = _calculate_routine_totals(conn, submission['date'], submission['template_id'])

        # Award XP
        award_xp(
            submission['date'], 
            'health',
            activity_key, 
            recalculated_xp,
            notes=f"{submission['routine_name']} routine"
        )
        awarded_count += 1
    
    # Delete old submissions after awarding (keep today's)
    conn.execute("""
        DELETE FROM routine_submissions 
        WHERE date < ?
    """, (today_brisbane,))
    
    # Delete old progress entries (keep today's)
    conn.execute("""
        DELETE FROM daily_routine_progress 
        WHERE date < ?
    """, (today_brisbane,))
    
    conn.commit()
    conn.close()
    dag_eval_for_signal(DAG_DEFAULT_USER_ID, 'routine_submissions_count')
    
    return awarded_count


def add_routine_item(template_id: int, item_text: str, xp_value: int, parent_item_id: int = None):
    """Add a new item to a routine."""
    conn = get_connection()
    
    # Get next order index
    max_order = conn.execute("""
        SELECT MAX(order_index) as max_order FROM routine_items
        WHERE template_id = ? AND parent_item_id IS ?
    """, (template_id, parent_item_id)).fetchone()
    
    next_order = (max_order['max_order'] or 0) + 1
    
    conn.execute("""
        INSERT INTO routine_items (template_id, item_text, xp_value, parent_item_id, order_index)
        VALUES (?, ?, ?, ?, ?)
    """, (template_id, item_text, xp_value, parent_item_id, next_order))
    
    conn.commit()
    conn.close()


def delete_routine_item(item_id: int):
    """Soft delete a routine item."""
    conn = get_connection()
    conn.execute("UPDATE routine_items SET active = 0 WHERE id = ?", (item_id,))
    conn.execute("UPDATE routine_items SET active = 0 WHERE parent_item_id = ?", (item_id,))
    conn.commit()
    conn.close()


def update_routine_item(item_id: int, item_text: str, xp_value: int):
    """Update a routine item."""
    conn = get_connection()
    conn.execute("""
        UPDATE routine_items 
        SET item_text = ?, xp_value = ?
        WHERE id = ?
    """, (item_text, xp_value, item_id))
    conn.commit()
    conn.close()


def get_today_routine_progress(date: str, template_id: int) -> dict:
    """Get progress for a routine on a specific date. Returns dict of {item_id: completed}."""
    template_id = int(template_id)
    key = ("get_today_routine_progress", date, template_id)

    def _load():
        conn = get_connection()
        progress = conn.execute(
            """
            SELECT item_id, completed
            FROM daily_routine_progress
            WHERE date = ? AND template_id = ?
            """,
            (date, template_id),
        ).fetchall()
        conn.close()
        return {row['item_id']: bool(row['completed']) for row in progress}

    return _cached_read(key, _load)


# =============================================================================
# DAG OPERATIONS
# =============================================================================

def upsert_dag_node(
    node_id: str,
    name: str,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    description: str = "",
    tier: int = 0,
    domain_tags: Optional[List[str]] = None,
    node_type: str = "standard",
    branch_key: Optional[str] = None,
    pos_x: Optional[float] = None,
    pos_y: Optional[float] = None,
    xp_reward: int = 0,
    bonus_prob: float = 0.0,
    teaser_unlock_delay_hours: Optional[int] = None,
    hidden_until_unlocked: bool = False,
):
    """Insert or update a DAG node."""
    conn = get_connection()
    tags = json.dumps(domain_tags or [])
    conn.execute(
        """
        INSERT INTO dag_nodes (
            node_id, graph_id, name, description, tier, domain_tags, node_type, branch_key, pos_x, pos_y, xp_reward,
            bonus_prob, teaser_unlock_delay_hours, hidden_until_unlocked,
            created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(node_id) DO UPDATE SET
            graph_id = excluded.graph_id,
            name = excluded.name,
            description = excluded.description,
            tier = excluded.tier,
            domain_tags = excluded.domain_tags,
            node_type = excluded.node_type,
            branch_key = excluded.branch_key,
            pos_x = COALESCE(excluded.pos_x, dag_nodes.pos_x),
            pos_y = COALESCE(excluded.pos_y, dag_nodes.pos_y),
            xp_reward = excluded.xp_reward,
            bonus_prob = excluded.bonus_prob,
            teaser_unlock_delay_hours = excluded.teaser_unlock_delay_hours,
            hidden_until_unlocked = excluded.hidden_until_unlocked,
            updated_ts = CURRENT_TIMESTAMP
        """,
        (
            node_id,
            (graph_id or DEFAULT_DAG_GRAPH_ID).strip() or DEFAULT_DAG_GRAPH_ID,
            name,
            description or "",
            int(tier),
            tags,
            node_type or "standard",
            (branch_key or "").strip() or None,
            None if pos_x is None else float(pos_x),
            None if pos_y is None else float(pos_y),
            int(xp_reward or 0),
            float(bonus_prob or 0.0),
            teaser_unlock_delay_hours,
            int(bool(hidden_until_unlocked)),
        ),
    )
    conn.commit()
    conn.close()


def add_dag_edge(parent_id: str, child_id: str):
    """Create a DAG edge (idempotent)."""
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO dag_edges (graph_id, parent_id, child_id)
        VALUES (
            COALESCE(
                (SELECT graph_id FROM dag_nodes WHERE node_id = ?),
                ?
            ),
            ?, ?
        )
        """,
        (parent_id, DEFAULT_DAG_GRAPH_ID, parent_id, child_id),
    )
    conn.commit()
    conn.close()


def add_dag_prereq(
    node_id: str,
    signal_key: str,
    window_days: int,
    operator: str,
    threshold: float,
    weight: float = 1.0,
    progress_cap: float = 1.0,
    notes: str = "",
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
) -> int:
    """Attach a prerequisite rule to a DAG node."""
    conn = get_connection()
    cur = conn.execute(
        """
        INSERT INTO dag_node_prereqs (
            graph_id, node_id, signal_key, window_days, operator, threshold,
            weight, progress_cap, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (
            graph_id,
            node_id,
            signal_key,
            int(window_days),
            operator,
            float(threshold),
            float(weight),
            float(progress_cap),
            notes or "",
        ),
    )
    prereq_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()
    return prereq_id


def get_dag_node(node_id: str) -> Optional[Dict[str, Any]]:
    """Get a DAG node by id."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM dag_nodes WHERE node_id = ?",
        (node_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    node = dict(row)
    try:
        node["domain_tags"] = json.loads(node.get("domain_tags") or "[]")
    except Exception:
        node["domain_tags"] = []
    return node


def get_all_dag_nodes(graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all DAG nodes ordered by tier then name."""
    conn = get_connection()
    if graph_id:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_nodes
            WHERE graph_id = ?
            ORDER BY tier ASC, COALESCE(branch_key, ''), name ASC
            """,
            (graph_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_nodes
            ORDER BY graph_id ASC, tier ASC, COALESCE(branch_key, ''), name ASC
            """
        ).fetchall()
    conn.close()

    result = []
    for row in rows:
        node = dict(row)
        try:
            node["domain_tags"] = json.loads(node.get("domain_tags") or "[]")
        except Exception:
            node["domain_tags"] = []
        result.append(node)
    return result


def get_dag_edges(graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all DAG edges."""
    conn = get_connection()
    if graph_id:
        rows = conn.execute(
            """
            SELECT edge_id, parent_id, child_id
            FROM dag_edges
            WHERE graph_id = ?
            ORDER BY parent_id, child_id
            """,
            (graph_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT edge_id, parent_id, child_id
            FROM dag_edges
            ORDER BY parent_id, child_id
            """
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_dag_prereqs(node_id: Optional[str] = None, graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get prerequisite rules, optionally for one node."""
    conn = get_connection()
    if node_id is None and graph_id is None:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_node_prereqs
            ORDER BY node_id, prereq_id
            """
        ).fetchall()
    elif node_id is None and graph_id is not None:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_node_prereqs
            WHERE graph_id = ?
            ORDER BY node_id, prereq_id
            """,
            (graph_id,),
        ).fetchall()
    elif node_id is not None and graph_id is None:
        node_graph = get_dag_node(node_id)
        row_graph_id = (node_graph or {}).get("graph_id") or DEFAULT_DAG_GRAPH_ID
        rows = conn.execute(
            """
            SELECT *
            FROM dag_node_prereqs
            WHERE node_id = ? AND graph_id = ?
            ORDER BY prereq_id
            """,
            (node_id, row_graph_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_node_prereqs
            WHERE node_id = ? AND graph_id = ?
            ORDER BY prereq_id
            """,
            (node_id, graph_id),
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def ensure_dag_user_state(user_id: str = DAG_DEFAULT_USER_ID, graph_id: str = DEFAULT_DAG_GRAPH_ID):
    """Ensure the user has one state row for every DAG node."""
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO dag_user_node_state (
            user_id, graph_id, node_id, state, progress, near_miss, available_since_ts, last_eval_ts
        )
        SELECT ?, ?, node_id, 'locked', 0.0, 0, NULL, CURRENT_TIMESTAMP
        FROM dag_nodes
        WHERE graph_id = ?
        """,
        (user_id, graph_id, graph_id),
    )
    conn.commit()
    conn.close()


def upsert_dag_user_node_state(
    node_id: str,
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    state: str = "locked",
    progress: float = 0.0,
    near_miss: bool = False,
    available_since_ts: Optional[str] = None,
    unlocked_ts: Optional[str] = None,
    mastered_ts: Optional[str] = None,
):
    """Insert or update DAG node state for a user."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO dag_user_node_state (
            user_id, graph_id, node_id, state, progress, near_miss, available_since_ts, unlocked_ts, mastered_ts, last_eval_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, node_id) DO UPDATE SET
            graph_id = excluded.graph_id,
            state = excluded.state,
            progress = excluded.progress,
            near_miss = excluded.near_miss,
            available_since_ts = COALESCE(excluded.available_since_ts, dag_user_node_state.available_since_ts),
            unlocked_ts = COALESCE(excluded.unlocked_ts, dag_user_node_state.unlocked_ts),
            mastered_ts = COALESCE(excluded.mastered_ts, dag_user_node_state.mastered_ts),
            last_eval_ts = CURRENT_TIMESTAMP
        """,
        (
            user_id,
            graph_id,
            node_id,
            state,
            max(0.0, min(1.0, float(progress))),
            int(bool(near_miss)),
            available_since_ts,
            unlocked_ts,
            mastered_ts,
        ),
    )
    conn.commit()
    conn.close()


def get_dag_user_states(user_id: str = DAG_DEFAULT_USER_ID, graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get DAG state rows for a user."""
    conn = get_connection()
    if graph_id:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_user_node_state
            WHERE user_id = ? AND graph_id = ?
            ORDER BY state, progress DESC, node_id
            """,
            (user_id, graph_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM dag_user_node_state
            WHERE user_id = ?
            ORDER BY graph_id, state, progress DESC, node_id
            """,
            (user_id,),
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_dag_frontier_nodes(
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """Get closest-to-unlock nodes (locked/available sorted by progress)."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            s.user_id,
            s.graph_id,
            s.node_id,
            s.state,
            s.progress,
            s.near_miss,
            n.name,
            n.tier,
            n.node_type,
            n.hidden_until_unlocked
        FROM dag_user_node_state s
        JOIN dag_nodes n ON n.node_id = s.node_id
        WHERE s.user_id = ?
          AND s.graph_id = ?
          AND n.graph_id = ?
          AND s.state IN ('locked', 'available')
        ORDER BY s.near_miss DESC, s.progress DESC, n.tier ASC, n.name ASC
        LIMIT ?
        """,
        (user_id, graph_id, graph_id, int(limit)),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def log_dag_event(
    event_type: str,
    node_id: str,
    payload: Optional[Dict[str, Any]] = None,
    user_id: str = DAG_DEFAULT_USER_ID,
):
    """Write a DAG event entry for UX/debug timelines."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO dag_event_log (user_id, event_type, node_id, payload, ts)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (user_id, event_type, node_id, json.dumps(payload or {})),
    )
    conn.commit()
    conn.close()


def get_dag_events(user_id: str = DAG_DEFAULT_USER_ID, limit: int = 50) -> List[Dict[str, Any]]:
    """Get recent DAG events."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT *
        FROM dag_event_log
        WHERE user_id = ?
        ORDER BY ts DESC, id DESC
        LIMIT ?
        """,
        (user_id, int(limit)),
    ).fetchall()
    conn.close()
    events = []
    for row in rows:
        evt = dict(row)
        try:
            evt["payload"] = json.loads(evt.get("payload") or "{}")
        except Exception:
            evt["payload"] = {}
        events.append(evt)
    return events


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _invalidate_dag_cache():
    global _DAG_SIGNAL_NODE_CACHE
    _DAG_SIGNAL_NODE_CACHE = None


def _parse_json_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    except Exception:
        pass
    return [p.strip() for p in text.split(",") if p.strip()]


def _has_dag_cycle(conn) -> bool:
    rows = conn.execute("SELECT parent_id, child_id FROM dag_edges").fetchall()
    graph = defaultdict(list)
    nodes = set()
    for row in rows:
        graph[row["parent_id"]].append(row["child_id"])
        nodes.add(row["parent_id"])
        nodes.add(row["child_id"])

    color = {n: 0 for n in nodes}

    def dfs(node: str) -> bool:
        color[node] = 1
        for nxt in graph.get(node, []):
            if color.get(nxt, 0) == 1:
                return True
            if color.get(nxt, 0) == 0 and dfs(nxt):
                return True
        color[node] = 2
        return False

    for node in nodes:
        if color[node] == 0 and dfs(node):
            return True
    return False


def dag_add_edge(parent_id: str, child_id: str):
    """Add edge with cycle protection."""
    if parent_id == child_id:
        raise ValueError("DAG cycle detected: self-loop edge is not allowed.")

    conn = get_connection()
    try:
        parent = conn.execute("SELECT graph_id FROM dag_nodes WHERE node_id = ?", (parent_id,)).fetchone()
        child = conn.execute("SELECT graph_id FROM dag_nodes WHERE node_id = ?", (child_id,)).fetchone()
        graph_id = (parent["graph_id"] if parent else DEFAULT_DAG_GRAPH_ID) or DEFAULT_DAG_GRAPH_ID
        if child and (child["graph_id"] or DEFAULT_DAG_GRAPH_ID) != graph_id:
            raise ValueError(f"Cannot connect nodes across graphs: {parent_id} -> {child_id}")
        conn.execute("BEGIN")
        conn.execute(
            "INSERT OR IGNORE INTO dag_edges (graph_id, parent_id, child_id) VALUES (?, ?, ?)",
            (graph_id, parent_id, child_id),
        )
        if _has_dag_cycle(conn):
            conn.execute("ROLLBACK")
            raise ValueError(f"DAG cycle detected when adding edge {parent_id} -> {child_id}")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        conn.close()
        raise
    conn.close()


def add_dag_edge(parent_id: str, child_id: str):
    """Backward-compatible wrapper with cycle check."""
    dag_add_edge(parent_id, child_id)


def _build_signal_node_index(force: bool = False) -> Dict[str, List[str]]:
    global _DAG_SIGNAL_NODE_CACHE
    if _DAG_SIGNAL_NODE_CACHE is not None and not force:
        return _DAG_SIGNAL_NODE_CACHE
    rows = get_dag_prereqs()
    idx = defaultdict(set)
    for row in rows:
        idx[str(row["signal_key"])].add(str(row["node_id"]))
    _DAG_SIGNAL_NODE_CACHE = {k: sorted(v) for k, v in idx.items()}
    return _DAG_SIGNAL_NODE_CACHE


def get_dag_frontier_badge_count(user_id: str = DAG_DEFAULT_USER_ID) -> int:
    if not _dag_tables_available():
        return 0
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM dag_user_node_state
        WHERE user_id = ?
          AND state IN ('locked', 'available')
          AND near_miss = 1
        """,
        (user_id,),
    ).fetchone()
    conn.close()
    return int((row["cnt"] if row else 0) or 0)


def _window_cutoff(window_days: int) -> str:
    base = date.fromisoformat(get_brisbane_date())
    return (base - timedelta(days=max(0, int(window_days) - 1))).isoformat()


def _weekly_cutoff(window_days: int) -> str:
    base = date.fromisoformat(get_brisbane_date())
    return (base - timedelta(days=max(0, int(window_days)))).isoformat()


def _max_iso_date(d1: Optional[str], d2: Optional[str]) -> Optional[str]:
    vals = [d for d in [d1, d2] if d]
    if not vals:
        return None
    return max(vals)


def _max_iso_ts(ts1: Optional[str], ts2: Optional[str]) -> Optional[str]:
    vals = [ts for ts in [ts1, ts2] if ts]
    if not vals:
        return None
    return max(vals)


def _iso_to_date_text(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    text = str(ts).strip()
    if not text:
        return None
    if "T" in text:
        return text.split("T", 1)[0]
    return text[:10]


def _iso_to_sqlite_ts(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    text = str(ts).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        if "T" in text:
            return text.replace("T", " ").replace("Z", "")[:19]
        return text[:19]


def compute_prereq_progress(value: float, operator: str, threshold: float) -> float:
    value = float(value or 0.0)
    threshold = float(threshold or 0.0)
    op = (operator or ">=").strip()
    eps = 1e-6

    if op == ">=":
        if threshold <= 0:
            return 1.0
        return max(0.0, min(1.0, value / threshold))
    if op == "<=":
        if value <= threshold:
            return 1.0
        return max(0.0, min(1.0, threshold / max(value, eps)))
    if op == "==":
        if threshold == 0:
            return 1.0 if abs(value) < eps else 0.0
        return max(0.0, min(1.0, 1.0 - (abs(value - threshold) / max(abs(threshold), eps))))
    return 0.0


def _compute_keystone_weeks_count(conn, window_days: int, min_ts: Optional[str] = None) -> float:
    min_date = _iso_to_date_text(min_ts)
    cutoff = _max_iso_date(_window_cutoff(window_days), min_date) or _window_cutoff(window_days)
    row = conn.execute(
        """
        WITH base_weeks AS (
            SELECT DISTINCT date(d, 'weekday 0', '-6 days') AS week_start
            FROM (
                SELECT date AS d FROM sleep_logs WHERE date >= ?
                UNION
                SELECT date AS d FROM daily_health_logs WHERE date >= ?
                UNION
                SELECT date AS d FROM project_sessions WHERE date >= ?
            )
        ),
        s AS (
            SELECT date(date, 'weekday 0', '-6 days') AS week_start, COUNT(*) AS sleep_logs
            FROM sleep_logs WHERE date >= ? GROUP BY week_start
        ),
        c AS (
            SELECT date(date, 'weekday 0', '-6 days') AS week_start, SUM(duration_min) AS cardio_min
            FROM exercise_cardio WHERE date >= ? GROUP BY week_start
        ),
        p AS (
            SELECT date(date, 'weekday 0', '-6 days') AS week_start, SUM(duration_min) AS project_min
            FROM project_sessions WHERE date >= ? GROUP BY week_start
        ),
        r AS (
            SELECT date(date, 'weekday 0', '-6 days') AS week_start, COUNT(*) AS routine_subs
            FROM routine_submissions WHERE date >= ? GROUP BY week_start
        ),
        f AS (
            SELECT date(week_end_date, 'weekday 0', '-6 days') AS week_start, MAX(savings) AS savings
            FROM weekly_finance WHERE week_end_date >= ? GROUP BY week_start
        )
        SELECT COUNT(*) AS cnt
        FROM base_weeks w
        LEFT JOIN s ON s.week_start = w.week_start
        LEFT JOIN c ON c.week_start = w.week_start
        LEFT JOIN p ON p.week_start = w.week_start
        LEFT JOIN r ON r.week_start = w.week_start
        LEFT JOIN f ON f.week_start = w.week_start
        WHERE COALESCE(s.sleep_logs, 0) >= 7
          AND COALESCE(c.cardio_min, 0) >= 150
          AND COALESCE(p.project_min, 0) >= 300
          AND COALESCE(r.routine_subs, 0) >= 10
          AND COALESCE(f.savings, -999999) >= 0
        """,
        (cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff),
    ).fetchone()
    return float((row["cnt"] if row else 0) or 0)


def compute_signal_value(user_id: str, signal_key: str, window_days: int, min_ts: Optional[str] = None) -> float:
    """Compute a signal value from existing tracking data."""
    conn = get_connection()
    min_date = _iso_to_date_text(min_ts)
    sqlite_min_ts = _iso_to_sqlite_ts(min_ts)
    cutoff = _max_iso_date(_window_cutoff(window_days), min_date) or _window_cutoff(window_days)
    weekly_cutoff = _max_iso_date(_weekly_cutoff(window_days), min_date) or _weekly_cutoff(window_days)
    key = (signal_key or "").strip()
    value = 0.0

    if key == "sleep_logs_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM sleep_logs WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "avg_sleep_quality":
        row = conn.execute(
            "SELECT AVG(sleep_quality) AS v FROM sleep_logs WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "avg_sleep_duration_hours":
        row = conn.execute(
            "SELECT AVG(duration_minutes) AS v FROM sleep_logs WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = (row["v"] or 0) / 60.0
    elif key == "avg_sleep_energy":
        row = conn.execute(
            "SELECT AVG(energy) AS v FROM sleep_logs WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "wake_on_time_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM sleep_logs WHERE date >= ? AND wake_method = 'on_time' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "sleep_quality_days_70":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM sleep_logs WHERE date >= ? AND sleep_quality >= 70 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "daily_health_logs_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_health_logs WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "water_days_2l":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_health_logs WHERE date >= ? AND COALESCE(water_liters,0) >= 2.0 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "calorie_logged_days":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_health_logs WHERE date >= ? AND calories IS NOT NULL AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "steps_days_8k":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_health_logs WHERE date >= ? AND COALESCE(steps,0) >= 8000 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "cardio_minutes":
        row = conn.execute(
            "SELECT SUM(duration_min) AS v FROM exercise_cardio WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "resistance_reps":
        row = conn.execute(
            "SELECT SUM(reps*sets) AS v FROM exercise_resistance WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "active_exercise_days":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM (
                SELECT date FROM exercise_cardio WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
                UNION
                SELECT date FROM exercise_resistance WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
            )
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "intense_cardio_days":
        row = conn.execute(
            "SELECT COUNT(DISTINCT date) AS c FROM exercise_cardio WHERE date >= ? AND is_intense = 1 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "recovered_sleep_after_intense_ratio":
        row = conn.execute(
            """
            WITH intense AS (
                SELECT DISTINCT date(date, '+1 day') AS next_day
                FROM exercise_cardio
                WHERE date >= ? AND is_intense = 1
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
            )
            SELECT COUNT(*) AS total_days,
                   SUM(CASE WHEN COALESCE(s.sleep_quality,0) >= 70 THEN 1 ELSE 0 END) AS recovered
            FROM intense i
            LEFT JOIN sleep_logs s
              ON s.date = i.next_day
             AND (? IS NULL OR datetime(s.created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        total = float(row["total_days"] or 0)
        recovered = float(row["recovered"] or 0)
        value = (recovered / total) if total > 0 else 0
    elif key == "project_sessions_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM project_sessions WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "project_minutes":
        row = conn.execute(
            "SELECT SUM(duration_min) AS v FROM project_sessions WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "weekly_finance_entries_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM weekly_finance WHERE week_end_date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (weekly_cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "weekly_non_negative_savings_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM weekly_finance WHERE week_end_date >= ? AND savings >= 0 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (weekly_cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "discretionary_ratio_weeks_35":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM weekly_finance WHERE week_end_date >= ? AND income > 0 AND (discretionary/income) <= 0.35 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (weekly_cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "routine_submissions_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM routine_submissions WHERE date >= ? AND (? IS NULL OR datetime(submitted_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "evening_routine_submissions_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM routine_submissions rs JOIN routine_templates rt ON rt.id = rs.template_id WHERE rs.date >= ? AND LOWER(rt.name) = 'evening' AND (? IS NULL OR datetime(rs.submitted_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "next_day_project_after_evening_ratio":
        row = conn.execute(
            """
            WITH evening_days AS (
                SELECT rs.date AS day
                FROM routine_submissions rs
                JOIN routine_templates rt ON rt.id = rs.template_id
                WHERE rs.date >= ? AND LOWER(rt.name) = 'evening'
                  AND (? IS NULL OR datetime(rs.submitted_at) >= datetime(?))
            )
            SELECT COUNT(*) AS total_days,
                   SUM(
                       CASE WHEN EXISTS (
                           SELECT 1
                           FROM project_sessions ps
                           WHERE ps.date = date(e.day, '+1 day')
                             AND (? IS NULL OR datetime(ps.created_at) >= datetime(?))
                       ) THEN 1 ELSE 0 END
                   ) AS with_project
            FROM evening_days e
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        total = float(row["total_days"] or 0)
        with_project = float(row["with_project"] or 0)
        value = (with_project / total) if total > 0 else 0
    elif key == "journal_entries_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM journal_entries WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "cross_domain_logged_days":
        row = conn.execute(
            """
            WITH combined AS (
              SELECT date AS d, 1 AS s, 0 AS h, 0 AS p, 0 AS f
              FROM sleep_logs
              WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
              UNION ALL
              SELECT date AS d, 0, 1, 0, 0
              FROM daily_health_logs
              WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
              UNION ALL
              SELECT date AS d, 0, 0, 1, 0
              FROM project_sessions
              WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
              UNION ALL
              SELECT week_end_date AS d, 0, 0, 0, 1
              FROM weekly_finance
              WHERE week_end_date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))
            ),
            daily AS (
              SELECT d, MAX(s)+MAX(h)+MAX(p)+MAX(f) AS domains
              FROM combined
              GROUP BY d
            )
            SELECT COUNT(*) AS c FROM daily WHERE domains >= 2
            """,
            (
                cutoff,
                sqlite_min_ts,
                sqlite_min_ts,
                cutoff,
                sqlite_min_ts,
                sqlite_min_ts,
                cutoff,
                sqlite_min_ts,
                sqlite_min_ts,
                cutoff,
                sqlite_min_ts,
                sqlite_min_ts,
            ),
        ).fetchone()
        value = row["c"] or 0
    elif key == "quest_completions_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_quests WHERE date >= ? AND completed = 1 AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "all_domains_level_min":
        row = conn.execute("SELECT MIN(level) AS m FROM identity_levels").fetchone()
        value = row["m"] or 0
    elif key == "min_domain_xp_share_30d":
        share_cutoff = _max_iso_date(_window_cutoff(30), min_date) or _window_cutoff(30)
        row = conn.execute(
            """
            WITH recent AS (
                SELECT domain, SUM(xp_gained) AS xp
                FROM xp_logs
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY domain
            ),
            totals AS (SELECT SUM(xp) AS total FROM recent)
            SELECT MIN(CASE WHEN t.total > 0 THEN (r.xp * 1.0 / t.total) ELSE 0 END) AS min_share
            FROM recent r
            CROSS JOIN totals t
            """,
            (share_cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["min_share"] or 0
    elif key == "routine_item_modifications_count":
        row = conn.execute("SELECT COUNT(*) AS c FROM routine_items WHERE active = 1 AND id > 20").fetchone()
        value = row["c"] or 0
    elif key == "overall_level":
        value = float(get_overall_level().get("level") or 0)
    elif key == "keystone_weeks_count":
        value = _compute_keystone_weeks_count(conn, window_days, min_ts=min_ts)
    elif key == "run_distance_week":
        row = conn.execute(
            """
            SELECT SUM(COALESCE(distance_km, 0)) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "single_run":
        row = conn.execute(
            """
            SELECT MAX(COALESCE(distance_km, 0)) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "workouts_in_7d":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM (
                SELECT date FROM exercise_cardio
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                UNION
                SELECT date FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
            )
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "pushups_session":
        row = conn.execute(
            """
            SELECT MAX(COALESCE(reps, 0) * COALESCE(sets, 1)) AS v
            FROM exercise_resistance
            WHERE date >= ?
              AND LOWER(COALESCE(exercise_type, '')) LIKE 'pushup%'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "pullups_session":
        row = conn.execute(
            """
            SELECT MAX(COALESCE(reps, 0) * COALESCE(sets, 1)) AS v
            FROM exercise_resistance
            WHERE date >= ?
              AND LOWER(COALESCE(exercise_type, '')) LIKE 'pullup%'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "squats_session":
        row = conn.execute(
            """
            SELECT MAX(COALESCE(reps, 0) * COALESCE(sets, 1)) AS v
            FROM exercise_resistance
            WHERE date >= ?
              AND LOWER(COALESCE(exercise_type, '')) LIKE 'squat%'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "plank_seconds_session":
        row = conn.execute(
            """
            SELECT MAX(COALESCE(reps, 0)) AS v
            FROM exercise_resistance
            WHERE date >= ?
              AND LOWER(COALESCE(exercise_type, '')) LIKE 'plank%'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "deep_work_sessions":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM project_sessions
            WHERE date >= ?
              AND COALESCE(duration_min, 0) >= 45
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "study_sessions":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM project_sessions
            WHERE date >= ?
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "weeks_with_4_study_sessions":
        row = conn.execute(
            """
            WITH weekly AS (
                SELECT strftime('%Y-%W', date) AS yw, COUNT(*) AS c
                FROM project_sessions
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY strftime('%Y-%W', date)
            )
            SELECT COUNT(*) AS c FROM weekly WHERE c >= 4
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "books_read_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM books WHERE date_finished >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key.startswith("books_category_count:"):
        category = key.split(":", 1)[1].strip().lower()
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM books
            WHERE date_finished >= ?
              AND LOWER(COALESCE(category, '')) = ?
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, category, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "essays_published_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM essays WHERE date_published >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "essays_1000_count":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM essays
            WHERE date_published >= ?
              AND COALESCE(word_count, 0) >= 1000
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "debate_events_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM debate_events WHERE date >= ? AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "debate_club_participations":
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM debate_events
            WHERE date >= ?
              AND LOWER(COALESCE(event_type, '')) LIKE '%club%'
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "debate_win_count":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM debate_events WHERE date >= ? AND LOWER(COALESCE(result, '')) LIKE 'win%' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "debate_tournament_participations":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM debate_events WHERE date >= ? AND LOWER(COALESCE(event_type,'')) LIKE '%tournament%' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "debate_tournament_podium":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM debate_events WHERE date >= ? AND (LOWER(COALESCE(result,'')) LIKE '%podium%' OR LOWER(COALESCE(result,'')) LIKE '%top 3%') AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "official_half_marathon_completed":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM races WHERE date >= ? AND official = 1 AND race_type = 'half' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "official_marathon_completed":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM races WHERE date >= ? AND official = 1 AND race_type = 'marathon' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "official_half_marathon_time_best_seconds":
        row = conn.execute(
            "SELECT MIN(time_seconds) AS v FROM races WHERE date >= ? AND official = 1 AND race_type = 'half' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "official_marathon_time_best_seconds":
        row = conn.execute(
            "SELECT MIN(time_seconds) AS v FROM races WHERE date >= ? AND official = 1 AND race_type = 'marathon' AND (? IS NULL OR datetime(created_at) >= datetime(?))",
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "run_5k_best_seconds":
        row = conn.execute(
            """
            SELECT MIN(CASE WHEN COALESCE(distance_km, 0) > 0 THEN (duration_min * 60.0 * 5.0 / distance_km) END) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND COALESCE(distance_km, 0) >= 5
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "run_3k_best_seconds":
        row = conn.execute(
            """
            SELECT MIN(CASE WHEN COALESCE(distance_km, 0) > 0 THEN (duration_min * 60.0 * 3.0 / distance_km) END) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND COALESCE(distance_km, 0) >= 3
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "run_1k_best_seconds":
        row = conn.execute(
            """
            SELECT MIN(CASE WHEN COALESCE(distance_km, 0) > 0 THEN (duration_min * 60.0 * 1.0 / distance_km) END) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND COALESCE(distance_km, 0) >= 1
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "run_10k_best_seconds":
        row = conn.execute(
            """
            SELECT MIN(CASE WHEN COALESCE(distance_km, 0) > 0 THEN (duration_min * 60.0 * 10.0 / distance_km) END) AS v
            FROM exercise_cardio
            WHERE date >= ?
              AND LOWER(COALESCE(type, '')) = 'run'
              AND COALESCE(distance_km, 0) >= 10
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["v"] or 0
    elif key == "hybrid_conditioning_1_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date, SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            ),
            c AS (
                SELECT date, MAX(COALESCE(distance_km,0)) AS run_km
                FROM exercise_cardio
                WHERE date >= ?
                  AND LOWER(COALESCE(type,'')) = 'run'
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c
            FROM c
            LEFT JOIN r USING(date)
            WHERE (run_km >= 5 AND COALESCE(pushups,0) >= 100)
               OR (run_km >= 8 AND COALESCE(squats,0) >= 150)
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "centurion_1_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pullup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pullups
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c FROM r
            WHERE pushups >= 100 AND squats >= 100 AND pullups >= 15
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "centurion_2_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pullup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pullups
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c FROM r
            WHERE pushups >= 100 AND squats >= 100 AND pullups >= 20
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "iron_centurion_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pullup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pullups
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c FROM r
            WHERE pushups >= 150 AND squats >= 300 AND pullups >= 30
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "spartan_endurance_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pullup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pullups
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            ),
            c AS (
                SELECT date,
                       MAX(COALESCE(distance_km,0)) AS run_km,
                       MIN(CASE WHEN COALESCE(distance_km,0) >= 5 THEN COALESCE(duration_min, 999999) END) AS run_5k_min
                FROM exercise_cardio
                WHERE date >= ?
                  AND LOWER(COALESCE(type,'')) = 'run'
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c
            FROM c
            LEFT JOIN r USING(date)
            WHERE run_km >= 5
              AND COALESCE(run_5k_min, 999999) <= 60
              AND COALESCE(pushups,0) >= 100
              AND COALESCE(squats,0) >= 100
              AND COALESCE(pullups,0) >= 20
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "titan_protocol_sessions":
        row = conn.execute(
            """
            WITH r AS (
                SELECT date,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pushup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pushups,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'squat%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS squats,
                       SUM(CASE WHEN LOWER(COALESCE(exercise_type,'')) LIKE 'pullup%' THEN COALESCE(reps,0)*COALESCE(sets,1) ELSE 0 END) AS pullups
                FROM exercise_resistance
                WHERE date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            ),
            c AS (
                SELECT date,
                       MAX(COALESCE(distance_km,0)) AS run_km,
                       MIN(CASE WHEN COALESCE(distance_km,0) >= 5 THEN COALESCE(duration_min, 999999) END) AS run_5k_min
                FROM exercise_cardio
                WHERE date >= ?
                  AND LOWER(COALESCE(type,'')) = 'run'
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
                GROUP BY date
            )
            SELECT COUNT(*) AS c
            FROM r
            LEFT JOIN c USING(date)
            WHERE (
                    COALESCE(pushups,0) >= 100 AND COALESCE(pullups,0) >= 100 AND COALESCE(squats,0) >= 100
                    AND COALESCE(run_km,0) >= 5 AND COALESCE(run_5k_min,999999) <= 90
                  )
               OR (
                    COALESCE(pushups,0) >= 200 AND COALESCE(pullups,0) >= 40 AND COALESCE(squats,0) >= 400
                  )
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, cutoff, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key == "iron_or_titan_sessions":
        iron = compute_signal_value(user_id, "iron_centurion_sessions", window_days, min_ts=min_ts)
        titan = compute_signal_value(user_id, "titan_protocol_sessions", window_days, min_ts=min_ts)
        value = max(iron, titan)
    elif key == "run_5k_within_48h_of_marathon":
        row = conn.execute(
            """
            WITH marathons AS (
                SELECT date
                FROM races
                WHERE official = 1
                  AND race_type = 'marathon'
                  AND date >= ?
                  AND (? IS NULL OR datetime(created_at) >= datetime(?))
            )
            SELECT COUNT(*) AS c
            FROM marathons m
            WHERE EXISTS (
                SELECT 1
                FROM exercise_cardio c
                WHERE LOWER(COALESCE(c.type,'')) = 'run'
                  AND COALESCE(c.distance_km,0) >= 5
                  AND date(c.date) >= date(m.date)
                  AND date(c.date) <= date(m.date, '+2 day')
                  AND (? IS NULL OR datetime(c.created_at) >= datetime(?))
            )
            """,
            (cutoff, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0
    elif key.startswith("exam_passed:"):
        exam_id = key.split(":", 1)[1].strip()
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM exams
            WHERE date >= ?
              AND exam_id = ?
              AND passed = 1
              AND (? IS NULL OR datetime(created_at) >= datetime(?))
            """,
            (cutoff, exam_id, sqlite_min_ts, sqlite_min_ts),
        ).fetchone()
        value = row["c"] or 0

    conn.close()
    return float(value or 0.0)


def compute_node_progress(
    prereq_rows: List[Dict[str, Any]],
    user_id: str = DAG_DEFAULT_USER_ID,
    min_ts: Optional[str] = None,
) -> float:
    if not prereq_rows:
        return 1.0
    weighted = 0.0
    total_weight = 0.0
    for row in prereq_rows:
        raw = compute_signal_value(user_id, row["signal_key"], int(row["window_days"]), min_ts=min_ts)
        pr = compute_prereq_progress(raw, row.get("operator", ">="), float(row["threshold"]))
        pr = min(float(row.get("progress_cap", 1.0) or 1.0), pr)
        w = float(row.get("weight", 1.0) or 1.0)
        weighted += pr * w
        total_weight += w
    if total_weight <= 0:
        return 0.0
    return max(0.0, min(1.0, weighted / total_weight))


def _node_prereq_breakdown(
    node_id: str,
    user_id: str = DAG_DEFAULT_USER_ID,
    min_ts: Optional[str] = None,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
) -> List[Dict[str, Any]]:
    rows = get_dag_prereqs(node_id, graph_id=graph_id)
    out = []
    for row in rows:
        val = compute_signal_value(user_id, row["signal_key"], int(row["window_days"]), min_ts=min_ts)
        p = compute_prereq_progress(val, row["operator"], float(row["threshold"]))
        p = min(float(row.get("progress_cap", 1.0) or 1.0), p)
        out.append({**row, "value": val, "progress": p, "satisfied": p >= 1.0 - 1e-9})
    return out


def _parent_map(graph_id: str = DEFAULT_DAG_GRAPH_ID) -> Dict[str, List[str]]:
    out = defaultdict(list)
    for edge in get_dag_edges(graph_id=graph_id):
        out[edge["child_id"]].append(edge["parent_id"])
    return out


def _children_map(graph_id: str = DEFAULT_DAG_GRAPH_ID) -> Dict[str, List[str]]:
    out = defaultdict(list)
    for edge in get_dag_edges(graph_id=graph_id):
        out[edge["parent_id"]].append(edge["child_id"])
    return out


def _topological_nodes(graph_id: str = DEFAULT_DAG_GRAPH_ID) -> List[str]:
    nodes = [n["node_id"] for n in get_all_dag_nodes(graph_id=graph_id)]
    indegree = {n: 0 for n in nodes}
    children = defaultdict(list)
    for edge in get_dag_edges(graph_id=graph_id):
        p, c = edge["parent_id"], edge["child_id"]
        if p in indegree and c in indegree:
            indegree[c] += 1
            children[p].append(c)
    q = deque([n for n in nodes if indegree[n] == 0])
    out = []
    while q:
        cur = q.popleft()
        out.append(cur)
        for nxt in children.get(cur, []):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                q.append(nxt)
    if len(out) != len(nodes):
        raise ValueError("DAG contains a cycle.")
    return out


def _descendants(node_ids: List[str], graph_id: str = DEFAULT_DAG_GRAPH_ID) -> List[str]:
    cmap = _children_map(graph_id=graph_id)
    seen = set(node_ids)
    q = deque(node_ids)
    while q:
        cur = q.popleft()
        for child in cmap.get(cur, []):
            if child not in seen:
                seen.add(child)
                q.append(child)
    return list(seen)


def _get_user_state_map(user_id: str, graph_id: str = DEFAULT_DAG_GRAPH_ID) -> Dict[str, Dict[str, Any]]:
    ensure_dag_user_state(user_id, graph_id=graph_id)
    return {row["node_id"]: row for row in get_dag_user_states(user_id, graph_id=graph_id)}


def _evaluate_nodes(
    node_ids: List[str],
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
):
    if not node_ids:
        return
    ensure_dag_user_state(user_id, graph_id=graph_id)
    state_map = _get_user_state_map(user_id, graph_id=graph_id)
    pmap = _parent_map(graph_id=graph_id)
    all_prereqs = get_dag_prereqs(graph_id=graph_id)
    by_node = defaultdict(list)
    for row in all_prereqs:
        by_node[row["node_id"]].append(row)

    ordered = [n for n in _topological_nodes(graph_id=graph_id) if n in set(node_ids)]
    conn = get_connection()
    now_iso = _utc_now_iso()
    for node_id in ordered:
        current = state_map.get(node_id, {})
        current_state = current.get("state", "locked")
        if current_state in ("unlocked", "mastered"):
            continue

        parents = pmap.get(node_id, [])
        parents_met = True
        for parent_id in parents:
            p_state = state_map.get(parent_id, {}).get("state", "locked")
            if p_state not in ("unlocked", "mastered"):
                parents_met = False
                break
        next_state = "available" if parents_met else "locked"
        available_since_ts = current.get("available_since_ts")
        if next_state == "available" and not available_since_ts:
            available_since_ts = now_iso
        if next_state != "available":
            available_since_ts = None

        if next_state == "available":
            progress = compute_node_progress(by_node.get(node_id, []), user_id=user_id, min_ts=available_since_ts)
            near_miss = int(NEAR_MISS_THRESHOLD <= progress < 1.0)
        else:
            progress = 0.0
            near_miss = 0

        conn.execute(
            """
            UPDATE dag_user_node_state
            SET state = ?, progress = ?, near_miss = ?, available_since_ts = ?, last_eval_ts = ?
            WHERE user_id = ? AND graph_id = ? AND node_id = ?
            """,
            (next_state, progress, near_miss, available_since_ts, now_iso, user_id, graph_id, node_id),
        )
        state_map[node_id] = {
            "state": next_state,
            "progress": progress,
            "near_miss": near_miss,
            "available_since_ts": available_since_ts,
        }
    conn.commit()
    conn.close()


def dag_eval_all(user_id: str = DAG_DEFAULT_USER_ID, graph_id: str = DEFAULT_DAG_GRAPH_ID):
    ensure_dag_user_state(user_id, graph_id=graph_id)
    _evaluate_nodes(_topological_nodes(graph_id=graph_id), user_id=user_id, graph_id=graph_id)


def dag_eval_for_signal(user_id: str, signal_key: str, graph_id: Optional[str] = None):
    if not _dag_tables_available():
        return
    if graph_id is None:
        for gid in (DEFAULT_DAG_GRAPH_ID, BODY_DAG_GRAPH_ID, MIND_DAG_GRAPH_ID):
            dag_eval_for_signal(user_id, signal_key, graph_id=gid)
        return
    idx = _build_signal_node_index()
    impacted = [node_id for node_id in idx.get(signal_key, []) if (get_dag_node(node_id) or {}).get("graph_id") == graph_id]
    if not impacted:
        return
    _evaluate_nodes(_descendants(impacted, graph_id=graph_id), user_id=user_id, graph_id=graph_id)


def _node_primary_domain(node: Dict[str, Any]) -> str:
    tags = _parse_json_list(node.get("domain_tags"))
    for tag in tags:
        t = str(tag).lower()
        if t in ("sleep", "health", "projects", "finance"):
            return t
        if t == "exercise":
            return "health"
    return "health"


def dag_unlock_node(user_id: str, node_id: str, graph_id: str = DEFAULT_DAG_GRAPH_ID) -> Dict[str, Any]:
    dag_eval_all(user_id, graph_id=graph_id)
    node = get_dag_node(node_id)
    if not node:
        raise ValueError(f"Unknown node: {node_id}")
    if (node.get("graph_id") or DEFAULT_DAG_GRAPH_ID) != graph_id:
        raise ValueError(f"Node {node_id} is not in graph {graph_id}")
    state_map = {s["node_id"]: s for s in get_dag_user_states(user_id, graph_id=graph_id)}
    state_row = state_map.get(node_id)
    if not state_row:
        raise ValueError(f"Missing state row for node: {node_id}")
    if state_row["state"] != "available":
        raise ValueError(f"Node {node_id} is not available")
    if float(state_row.get("progress") or 0) < 1.0:
        raise ValueError(f"Node {node_id} is not complete")

    now_iso = _utc_now_iso()
    conn = get_connection()
    conn.execute(
        """
        UPDATE dag_user_node_state
        SET state = 'unlocked', progress = 1.0, near_miss = 0, unlocked_ts = ?, last_eval_ts = ?
        WHERE user_id = ? AND graph_id = ? AND node_id = ?
        """,
        (now_iso, now_iso, user_id, graph_id, node_id),
    )
    conn.commit()
    conn.close()

    from analytics.scoring import award_xp
    domain = _node_primary_domain(node)
    xp_reward = int(node.get("xp_reward") or 0)
    if xp_reward > 0:
        award_xp(get_brisbane_date(), domain, f"dag_unlock_{node_id}", xp_reward, notes=f"DAG unlock: {node.get('name')}")

    bonus_triggered = False
    bonus_xp = 0
    effective_prob = min(1.0, max(0.0, float(node.get("bonus_prob") or 0.0) * BONUS_PROB_MULTIPLIER))
    if random.random() < effective_prob:
        bonus_triggered = True
        bonus_xp = BONUS_XP_REWARD
        award_xp(get_brisbane_date(), domain, f"dag_bonus_{node_id}_{int(time_module.time())}", bonus_xp, notes=f"DAG bonus: {node.get('name')}")
        log_dag_event("bonus_drop", node_id, {"bonus_xp": bonus_xp, "effective_prob": effective_prob}, user_id=user_id)

    log_dag_event("node_unlocked", node_id, {"xp_reward": xp_reward, "bonus_triggered": bonus_triggered, "bonus_xp": bonus_xp}, user_id=user_id)

    children = [e["child_id"] for e in get_dag_edges(graph_id=graph_id) if e["parent_id"] == node_id]
    conn = get_connection()
    for child_id in children:
        child_node = get_dag_node(child_id)
        if not child_node:
            continue
        delay_h = int(child_node.get("teaser_unlock_delay_hours") or TEASER_DELAY_DEFAULT_HOURS)
        reveal_ts = (datetime.utcnow() + timedelta(hours=delay_h)).replace(microsecond=0).isoformat() + "Z"
        conn.execute(
            """
            INSERT INTO dag_user_teasers (user_id, graph_id, node_id, reveal_ts, created_ts)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, node_id) DO UPDATE SET
                reveal_ts = CASE
                    WHEN dag_user_teasers.reveal_ts > excluded.reveal_ts THEN excluded.reveal_ts
                    ELSE dag_user_teasers.reveal_ts
                END
            """,
            (user_id, graph_id, child_id, reveal_ts, now_iso),
        )
        log_dag_event("teaser_revealed", child_id, {"parent_id": node_id, "reveal_ts": reveal_ts}, user_id=user_id)
    conn.commit()
    conn.close()

    dag_eval_all(user_id, graph_id=graph_id)
    return {"node_id": node_id, "xp_reward": xp_reward, "bonus_triggered": bonus_triggered, "bonus_xp": bonus_xp, "unlocked_ts": now_iso}


def get_dag_teasers(
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    limit: int = MAX_TEASERS_SHOWN,
) -> List[Dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT t.user_id, t.node_id, t.reveal_ts, t.created_ts, n.name, n.description, n.tier
        FROM dag_user_teasers t
        JOIN dag_nodes n ON n.node_id = t.node_id
        WHERE t.user_id = ?
          AND t.graph_id = ?
          AND n.graph_id = ?
        ORDER BY t.reveal_ts ASC, n.tier ASC
        LIMIT ?
        """,
        (user_id, graph_id, graph_id, int(limit)),
    ).fetchall()
    conn.close()
    now_iso = _utc_now_iso()
    out = []
    for row in rows:
        item = dict(row)
        item["is_revealed"] = item["reveal_ts"] <= now_iso
        out.append(item)
    return out


def get_dag_node_debug(user_id: str, node_id: str, graph_id: str = DEFAULT_DAG_GRAPH_ID) -> Optional[Dict[str, Any]]:
    node = get_dag_node(node_id)
    if not node:
        return None
    if (node.get("graph_id") or DEFAULT_DAG_GRAPH_ID) != graph_id:
        return None
    states = {s["node_id"]: s for s in get_dag_user_states(user_id, graph_id=graph_id)}
    state = states.get(node_id)
    state_name = (state or {}).get("state")
    if state_name not in ("available", "unlocked", "mastered"):
        prereqs = []
        for row in get_dag_prereqs(node_id, graph_id=graph_id):
            prereqs.append({**row, "value": 0.0, "progress": 0.0, "satisfied": False})
        return {"node": node, "state": state, "prereqs": prereqs}
    min_ts = (state or {}).get("available_since_ts")
    return {"node": node, "state": state, "prereqs": _node_prereq_breakdown(node_id, user_id=user_id, min_ts=min_ts, graph_id=graph_id)}


def get_dag_node_action_link(node_id: str, user_id: str = DAG_DEFAULT_USER_ID, graph_id: str = DEFAULT_DAG_GRAPH_ID) -> str:
    debug = get_dag_node_debug(user_id, node_id, graph_id=graph_id)
    if not debug:
        return "/"
    unsatisfied = [p for p in debug["prereqs"] if not p["satisfied"]]
    if not unsatisfied:
        return "/skill-tree"
    unsatisfied.sort(key=lambda r: r["progress"])
    key = unsatisfied[0]["signal_key"]
    route_map = {
        "sleep_logs_count": "/sleep",
        "avg_sleep_quality": "/sleep",
        "avg_sleep_duration_hours": "/sleep",
        "avg_sleep_energy": "/sleep",
        "wake_on_time_count": "/sleep",
        "cardio_minutes": "/health",
        "resistance_reps": "/health",
        "active_exercise_days": "/health",
        "daily_health_logs_count": "/health",
        "water_days_2l": "/health",
        "calorie_logged_days": "/health",
        "steps_days_8k": "/health",
        "project_sessions_count": "/projects",
        "project_minutes": "/projects",
        "next_day_project_after_evening_ratio": "/projects",
        "weekly_finance_entries_count": "/finance",
        "weekly_non_negative_savings_count": "/finance",
        "discretionary_ratio_weeks_35": "/finance",
        "routine_submissions_count": "/routines",
        "evening_routine_submissions_count": "/routines",
        "journal_entries_count": "/journal",
    }
    return route_map.get(key, "/summary")


def get_dag_frontier_with_details(
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    frontier = get_dag_frontier_nodes(user_id=user_id, graph_id=graph_id, limit=limit)
    state_map = {s["node_id"]: s for s in get_dag_user_states(user_id, graph_id=graph_id)}
    out = []
    for row in frontier:
        node_state = (state_map.get(row["node_id"]) or {}).get("state")
        if node_state not in ("available", "unlocked", "mastered"):
            prereqs = [{**p, "value": 0.0, "progress": 0.0, "satisfied": False} for p in get_dag_prereqs(row["node_id"], graph_id=graph_id)]
        else:
            min_ts = (state_map.get(row["node_id"]) or {}).get("available_since_ts")
            prereqs = _node_prereq_breakdown(row["node_id"], user_id=user_id, min_ts=min_ts, graph_id=graph_id)
        out.append(
            {
                **row,
                "prereqs": prereqs,
                "action_link": get_dag_node_action_link(row["node_id"], user_id=user_id, graph_id=graph_id),
            }
        )
    return out


def _infer_branch_key(node: Dict[str, Any]) -> str:
    explicit = (node.get("branch_key") or "").strip().lower()
    if explicit:
        return explicit
    tags = [t.lower() for t in _parse_json_list(node.get("domain_tags"))]
    if "finance" in tags:
        return "finance"
    if "projects" in tags:
        return "projects"
    if "sleep" in tags:
        return "sleep"
    if "health" in tags or "exercise" in tags:
        return "health"
    if any(t in ("routines", "journal", "identity", "summary", "quests", "system") for t in tags):
        return "meta"

    nid = (node.get("node_id") or "").lower()
    if "sleep" in nid:
        return "sleep"
    if any(k in nid for k in ("budget", "finance", "savings")):
        return "finance"
    if any(k in nid for k in ("project", "focus", "output")):
        return "projects"
    if any(k in nid for k in ("move", "health", "nutrition", "recovery", "cardio")):
        return "health"
    if any(k in nid for k in ("routine", "reflection", "identity", "system")):
        return "meta"
    return "core"


_GRAPH_ROOT_SPACING = 200.0
_GRAPH_X_SPACING_SMALL = 110.0
_GRAPH_Y_SPACING = 540.0
_GRAPH_X_CLAMP = 300.0
_GRAPH_NODE_MIN_GAP = 136.0
_GRAPH_TOP_PADDING = 72.0
_GRAPH_CANVAS_WIDTH = 860.0
_GRAPH_SIDE_PADDING = 56.0
_GRAPH_BRANCH_LANE_WIDTH = 300.0
_GRAPH_BRANCH_CLUSTER_GAP = 86.0


def _infer_lane_key(node: Dict[str, Any], graph_id: str) -> str:
    branch = _infer_branch_key(node)
    nid = str(node.get("node_id") or "").lower()
    if graph_id == MIND_DAG_GRAPH_ID:
        if branch == "math":
            if nid.startswith("calc_") or "calc_track" in nid:
                return "math_calc"
            if nid.startswith("linalg_") or "linalg_track" in nid:
                return "math_linalg"
            if nid.startswith("stats_") or "stats_track" in nid:
                return "math_stats"
            if nid.startswith("phys_") or "physics_track" in nid:
                return "math_phys"
            return "math_core"
        if branch == "cs":
            if nid.startswith("cs_e2e_") or any(k in nid for k in ("fullstack_", "deploy_fullstack", "auth_", "production_ui")):
                return "cs_e2e"
            if nid.startswith("cs_backend_") or nid.startswith("backend_"):
                return "cs_backend"
            if nid.startswith("git_"):
                return "cs_git"
            return "cs_core"
    return branch


def _lane_sort_key(lane_key: str, graph_id: str) -> tuple[int, str]:
    if graph_id == BODY_DAG_GRAPH_ID:
        order = {
            "run": 0,
            "core": 1,
            "center": 2,
            "strength": 3,
        }
        return (order.get(lane_key, 99), lane_key)
    if graph_id == MIND_DAG_GRAPH_ID:
        order = {
            "math_calc": 0,
            "math_linalg": 1,
            "math_stats": 2,
            "math_phys": 3,
            "math_core": 4,
            "core": 5,
            "cs_git": 6,
            "cs_e2e": 7,
            "cs_backend": 8,
            "cs_core": 9,
            "reading": 10,
            "writing": 11,
            "oratory": 12,
            "ai": 13,
            "systems": 14,
            "center": 15,
        }
        return (order.get(lane_key, 99), lane_key)
    return (0 if lane_key == "core" else 1, lane_key)


def _compute_vertical_tree_positions(
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
) -> Dict[str, Dict[str, float]]:
    """Deterministic vertical layout with branch lanes and parent-aware local clustering."""
    if not nodes:
        return {}

    node_map = {n["node_id"]: n for n in nodes}
    node_ids = list(node_map.keys())
    parents = defaultdict(list)
    children = defaultdict(list)
    indegree = {nid: 0 for nid in node_ids}
    for edge in edges:
        parent_id = edge["parent_id"]
        child_id = edge["child_id"]
        if parent_id not in node_map or child_id not in node_map:
            continue
        children[parent_id].append(child_id)
        parents[child_id].append(parent_id)
        indegree[child_id] += 1

    for parent_id in children:
        children[parent_id] = sorted(
            children[parent_id],
            key=lambda cid: (int(node_map[cid].get("tier") or 0), cid),
        )

    max_tier = max(int(n.get("tier") or 0) for n in nodes)

    def _tier_y(tier: int) -> float:
        return _GRAPH_TOP_PADDING + (max_tier - int(tier)) * _GRAPH_Y_SPACING

    node_lane = {nid: _infer_lane_key(node_map[nid], graph_id) for nid in node_ids}
    lane_counts = defaultdict(int)
    for n in nodes:
        if int(n.get("tier") or 0) >= 1:
            lane_counts[_infer_lane_key(n, graph_id)] += 1
    lane_order = sorted(lane_counts.keys(), key=lambda lane: _lane_sort_key(lane, graph_id))
    if not lane_order:
        lane_order = sorted({node_lane[nid] for nid in node_ids}, key=lambda lane: _lane_sort_key(lane, graph_id))

    lane_center: Dict[str, float] = {}
    if lane_order:
        offset = (len(lane_order) - 1) / 2.0
        lane_spacing = min(_GRAPH_BRANCH_LANE_WIDTH, (2.0 * _GRAPH_X_CLAMP) / max(1.0, float(len(lane_order) - 1)))
        for idx, lane in enumerate(lane_order):
            lane_center[lane] = (idx - offset) * lane_spacing
    for nid in node_ids:
        lane_center.setdefault(node_lane[nid], 0.0)

    positions: Dict[str, Dict[str, float]] = {}
    by_tier_lane = defaultdict(lambda: defaultdict(list))
    for nid, n in node_map.items():
        tier = int(n.get("tier") or 0)
        lane = node_lane[nid]
        by_tier_lane[tier][lane].append(nid)

    for tier, lane_map in by_tier_lane.items():
        for lane, ids in lane_map.items():
            ids_sorted = sorted(ids, key=lambda nid: (node_map[nid].get("name") or nid, nid))
            center = lane_center.get(lane, 0.0)
            local_offset = (len(ids_sorted) - 1) / 2.0
            for idx, nid in enumerate(ids_sorted):
                x = center + (idx - local_offset) * min(_GRAPH_BRANCH_CLUSTER_GAP, 74.0)
                x = max(-_GRAPH_X_CLAMP, min(_GRAPH_X_CLAMP, x))
                positions[nid] = {"x": x, "y": _tier_y(tier)}

    q = deque(sorted([nid for nid, deg in indegree.items() if deg == 0], key=lambda nid: (int(node_map[nid].get("tier") or 0), nid)))
    topo = []
    while q:
        cur = q.popleft()
        topo.append(cur)
        for nxt in children.get(cur, []):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                q.append(nxt)

    # Pull nodes toward parent centroid while retaining branch lane clustering.
    for node_id in topo:
        pids = parents.get(node_id, [])
        if not pids:
            continue
        parent_x = [positions[pid]["x"] for pid in pids if pid in positions]
        if not parent_x:
            continue
        lane = node_lane.get(node_id, "core")
        lane_center_x = lane_center.get(lane, 0.0)
        base_x = positions.get(node_id, {"x": lane_center_x})["x"]
        parent_center = sum(parent_x) / float(len(parent_x))
        blended = (0.82 * base_x) + (0.18 * parent_center)
        lane_span = 64.0
        min_lane_x = lane_center_x - lane_span
        max_lane_x = lane_center_x + lane_span
        positions[node_id]["x"] = max(min_lane_x, min(max_lane_x, blended))
        positions[node_id]["x"] = max(-_GRAPH_X_CLAMP, min(_GRAPH_X_CLAMP, positions[node_id]["x"]))

    # Enforce minimum horizontal gap within each tier to avoid node overlap.
    by_tier_node_ids = defaultdict(list)
    for nid, pos in positions.items():
        tier = int(
            round(
                ((max_tier * _GRAPH_Y_SPACING + _GRAPH_TOP_PADDING) - float(pos["y"]))
                / _GRAPH_Y_SPACING
            )
        )
        tier = max(0, min(max_tier, tier))
        by_tier_node_ids[tier].append(nid)

    for tier, ids in by_tier_node_ids.items():
        if len(ids) <= 1:
            continue
        ids_sorted = sorted(ids, key=lambda nid: positions[nid]["x"])
        original_center = sum(positions[nid]["x"] for nid in ids_sorted) / float(len(ids_sorted))
        for idx in range(1, len(ids_sorted)):
            prev_id = ids_sorted[idx - 1]
            cur_id = ids_sorted[idx]
            min_x = positions[prev_id]["x"] + _GRAPH_NODE_MIN_GAP
            if positions[cur_id]["x"] < min_x:
                positions[cur_id]["x"] = min_x

        new_center = sum(positions[nid]["x"] for nid in ids_sorted) / float(len(ids_sorted))
        shift = original_center - new_center
        for nid in ids_sorted:
            positions[nid]["x"] += shift

    # Shift so all x are positive with stable side padding.
    if positions:
        min_x = min(float(p["x"]) for p in positions.values())
        x_shift = _GRAPH_SIDE_PADDING - min_x
        for nid in positions:
            positions[nid]["x"] = float(positions[nid]["x"]) + x_shift
    return positions


def set_node_positions(items: List[Dict[str, Any]]):
    """Persist manual node coordinates."""
    if not items:
        return
    conn = get_connection()
    for row in items:
        node_id = row.get("node_id")
        if not node_id:
            continue
        conn.execute(
            """
            UPDATE dag_nodes
            SET pos_x = ?, pos_y = ?, updated_ts = CURRENT_TIMESTAMP
            WHERE node_id = ?
            """,
            (
                None if row.get("pos_x") is None else float(row.get("pos_x")),
                None if row.get("pos_y") is None else float(row.get("pos_y")),
                node_id,
            ),
        )
    conn.commit()
    conn.close()


def get_dag_graph_elements(
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
) -> Dict[str, Any]:
    """Return graph nodes/edges and engagement metadata for the skill tree UI."""
    dag_eval_all(user_id, graph_id=graph_id)
    nodes = get_all_dag_nodes(graph_id=graph_id)
    edges = get_dag_edges(graph_id=graph_id)
    states = {s["node_id"]: s for s in get_dag_user_states(user_id, graph_id=graph_id)}
    fallback_pos = _compute_vertical_tree_positions(nodes, edges, graph_id=graph_id)
    frontier = get_dag_frontier_with_details(user_id=user_id, graph_id=graph_id, limit=8)
    frontier_ids = [n["node_id"] for n in frontier]
    frontier_top_id = frontier_ids[0] if frontier_ids else None
    within_one_action = len([n for n in frontier if float(n.get("progress") or 0) >= 0.9])

    conn = get_connection()
    teaser_rows = conn.execute(
        """
        SELECT node_id, reveal_ts
        FROM dag_user_teasers
        WHERE user_id = ?
          AND graph_id = ?
        """,
        (user_id, graph_id),
    ).fetchall()
    conn.close()
    now_iso = _utc_now_iso()
    teaser_map = {}
    for row in teaser_rows:
        teaser_map[row["node_id"]] = {
            "reveal_ts": row["reveal_ts"],
            "is_revealed": row["reveal_ts"] <= now_iso,
        }

    elements = []
    node_lookup = {}
    x_values: List[float] = []
    lane_keys = sorted({_infer_lane_key(node, graph_id) for node in nodes}, key=lambda lane: _lane_sort_key(lane, graph_id))
    lane_index_map = {lane: idx for idx, lane in enumerate(lane_keys)}
    for node in nodes:
        node_id = node["node_id"]
        state = states.get(node_id, {})
        node_state = state.get("state", "locked")
        progress = float(state.get("progress") or 0.0)
        near_miss = int(bool(state.get("near_miss")))
        teaser = teaser_map.get(node_id)
        is_teaser = bool(teaser) and not teaser.get("is_revealed")

        use_manual_pos = graph_id == DEFAULT_DAG_GRAPH_ID
        if use_manual_pos and node.get("pos_x") is not None and node.get("pos_y") is not None:
            position = {"x": float(node["pos_x"]), "y": float(node["pos_y"])}
        else:
            position = fallback_pos.get(node_id, {"x": 0.0, "y": 0.0})
        x_values.append(float(position.get("x", 0.0)))

        label = "????" if is_teaser else node.get("name", node_id)
        if node_state in ("unlocked", "mastered") and not is_teaser:
            label = f"{label} \u2713"

        branch_key = _infer_branch_key(node)
        lane_key = _infer_lane_key(node, graph_id)
        lane_index = int(lane_index_map.get(lane_key, 0))
        node_lookup[node_id] = {
            "node_id": node_id,
            "name": node.get("name"),
            "description": node.get("description"),
            "tier": int(node.get("tier") or 0),
            "branch_key": branch_key,
            "lane_key": lane_key,
            "lane_index": lane_index,
            "state": node_state,
            "progress": progress,
            "near_miss": near_miss,
            "is_teaser": is_teaser,
            "reveal_ts": teaser.get("reveal_ts") if teaser else None,
            "action_link": get_dag_node_action_link(node_id, user_id=user_id, graph_id=graph_id),
            "graph_id": graph_id,
        }

        elements.append(
            {
                "data": {
                    "id": node_id,
                    "label": label,
                    "node_id": node_id,
                    "tier": int(node.get("tier") or 0),
                    "branch_key": branch_key,
                    "lane_key": lane_key,
                    "lane_index": lane_index,
                    "state": node_state,
                    "progress": progress,
                    "near_miss": near_miss,
                    "teaser": 1 if is_teaser else 0,
                    "frontier": 1 if node_id in frontier_ids else 0,
                },
                "position": position,
            }
        )

    for edge in edges:
        elements.append({"data": {"source": edge["parent_id"], "target": edge["child_id"]}})

    closest_unlock = None
    if frontier:
        closest = frontier[0]
        closest_unlock = {
            "node_id": closest["node_id"],
            "name": closest["name"],
            "progress_pct": int(round(float(closest.get("progress") or 0) * 100)),
        }

    return {
        "elements": elements,
        "node_lookup": node_lookup,
        "frontier": frontier,
        "frontier_node_ids": frontier_ids,
        "frontier_top_id": frontier_top_id,
        "within_one_action": within_one_action,
        "closest_unlock": closest_unlock,
        "teasers": teaser_map,
        "max_tier": max([int(n.get("tier") or 0) for n in nodes], default=0),
        "x_span": (max(x_values) - min(x_values)) if x_values else 0.0,
        "graph_id": graph_id,
    }


def _get_frontier_preferred_quest_types(user_id: str = DAG_DEFAULT_USER_ID) -> List[str]:
    if not _dag_tables_available():
        return []
    frontier = get_dag_frontier_with_details(user_id=user_id, limit=3)
    signal_to_type = {
        "sleep_logs_count": "sleep",
        "avg_sleep_quality": "sleep",
        "avg_sleep_duration_hours": "sleep",
        "avg_sleep_energy": "sleep",
        "wake_on_time_count": "sleep",
        "cardio_minutes": "exercise",
        "resistance_reps": "exercise",
        "active_exercise_days": "exercise",
        "daily_health_logs_count": "health",
        "water_days_2l": "health",
        "calorie_logged_days": "health",
        "steps_days_8k": "health",
        "project_sessions_count": "projects",
        "project_minutes": "projects",
        "weekly_finance_entries_count": "finance",
        "weekly_non_negative_savings_count": "finance",
        "discretionary_ratio_weeks_35": "finance",
        "routine_submissions_count": "sleep",
        "journal_entries_count": "projects",
    }
    prefs = []
    for node in frontier:
        unsatisfied = [p for p in node["prereqs"] if not p["satisfied"]]
        unsatisfied.sort(key=lambda r: r["progress"])
        for miss in unsatisfied[:2]:
            qtype = signal_to_type.get(miss["signal_key"])
            if qtype:
                prefs.append(qtype)
    return prefs


def _replace_node_prereqs(node_id: str, prereqs: List[Dict[str, Any]], graph_id: str = DEFAULT_DAG_GRAPH_ID):
    conn = get_connection()
    conn.execute("DELETE FROM dag_node_prereqs WHERE node_id = ? AND graph_id = ?", (node_id, graph_id))
    for p in prereqs:
        conn.execute(
            """
            INSERT INTO dag_node_prereqs (
                graph_id, node_id, signal_key, window_days, operator, threshold, weight, progress_cap, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                graph_id,
                node_id,
                p["signal_key"],
                int(p["window_days"]),
                p.get("operator", ">="),
                float(p["threshold"]),
                float(p.get("weight", 1.0)),
                float(p.get("progress_cap", 1.0)),
                p.get("notes", ""),
            ),
        )
    conn.commit()
    conn.close()
    _invalidate_dag_cache()


def seed_dag_defaults(force: bool = False):
    """Idempotently seed discipline/body/mind DAGs."""

    def _seed_graph_bundle(
        graph_id: str,
        nodes: List[Dict[str, Any]],
        edges: List[tuple],
        prereqs: Dict[str, List[Dict[str, Any]]],
    ):
        node_ids = {n["node_id"] for n in nodes}
        conn = get_connection()
        conn.execute("DELETE FROM dag_edges WHERE graph_id = ?", (graph_id,))
        conn.execute("DELETE FROM dag_node_prereqs WHERE graph_id = ? AND node_id NOT IN ({})".format(",".join("?" * len(node_ids))), (graph_id, *sorted(node_ids)))
        conn.execute("DELETE FROM dag_user_teasers WHERE graph_id = ? AND node_id NOT IN ({})".format(",".join("?" * len(node_ids))), (graph_id, *sorted(node_ids)))
        conn.execute("DELETE FROM dag_user_node_state WHERE graph_id = ? AND node_id NOT IN ({})".format(",".join("?" * len(node_ids))), (graph_id, *sorted(node_ids)))
        conn.execute("DELETE FROM dag_nodes WHERE graph_id = ? AND node_id NOT IN ({})".format(",".join("?" * len(node_ids))), (graph_id, *sorted(node_ids)))
        conn.commit()
        conn.close()

        for n in nodes:
            upsert_dag_node(
                n["node_id"],
                n["name"],
                graph_id=graph_id,
                description=n.get("description", ""),
                tier=n["tier"],
                domain_tags=n.get("domain_tags", []),
                branch_key=n.get("branch_key"),
                xp_reward=int(n.get("xp_reward", 80)),
                bonus_prob=float(n.get("bonus_prob", 0.02)),
            )

        for parent_id, child_id in edges:
            if parent_id in node_ids and child_id in node_ids:
                dag_add_edge(parent_id, child_id)

        for node_id, rules in prereqs.items():
            normalized = [{"operator": ">=", "weight": 1.0, "progress_cap": 1.0, **r} for r in rules]
            _replace_node_prereqs(node_id, normalized, graph_id=graph_id)

        ensure_dag_user_state(DAG_DEFAULT_USER_ID, graph_id=graph_id)

    discipline_nodes = [
        {"node_id": "disc_log_basics", "name": "Log Basics", "tier": 0, "branch_key": "core", "domain_tags": ["sleep", "health"]},
        {"node_id": "disc_move_minimum", "name": "Move Minimum", "tier": 0, "branch_key": "health", "domain_tags": ["health"]},
        {"node_id": "disc_focus_floor", "name": "Focus Floor", "tier": 0, "branch_key": "projects", "domain_tags": ["projects"]},
        {"node_id": "disc_consistency_engine", "name": "Consistency Engine", "tier": 1, "branch_key": "core", "domain_tags": ["sleep", "health", "projects"]},
        {"node_id": "disc_keystone_week", "name": "Keystone Week", "tier": 2, "branch_key": "core", "domain_tags": ["sleep", "health", "projects", "finance"]},
    ]
    discipline_edges = [
        ("disc_log_basics", "disc_consistency_engine"),
        ("disc_move_minimum", "disc_consistency_engine"),
        ("disc_focus_floor", "disc_consistency_engine"),
        ("disc_consistency_engine", "disc_keystone_week"),
    ]
    discipline_prereqs = {
        "disc_log_basics": [{"signal_key": "sleep_logs_count", "window_days": 7, "threshold": 3}],
        "disc_move_minimum": [{"signal_key": "workouts_in_7d", "window_days": 7, "threshold": 3}],
        "disc_focus_floor": [{"signal_key": "project_sessions_count", "window_days": 7, "threshold": 3}],
        "disc_consistency_engine": [
            {"signal_key": "sleep_logs_count", "window_days": 14, "threshold": 10},
            {"signal_key": "workouts_in_7d", "window_days": 14, "threshold": 8},
        ],
        "disc_keystone_week": [{"signal_key": "cross_domain_logged_days", "window_days": 14, "threshold": 6}],
    }

    body_nodes = [
        {"node_id": "body_baseline_mover", "name": "Baseline Mover", "tier": 0, "branch_key": "core", "domain_tags": ["health"]},
        {"node_id": "body_structured_training", "name": "Structured Training", "tier": 0, "branch_key": "core", "domain_tags": ["health"]},
        {"node_id": "run_3k_complete", "name": "3km Continuous", "tier": 1, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_5k_complete", "name": "5km Completion", "tier": 1, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "pushups_40", "name": "Pushups I (40)", "tier": 1, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "pullups_8", "name": "Pull-ups I (8)", "tier": 1, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "squats_80", "name": "Squats I (80)", "tier": 1, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "plank_2min", "name": "Core I (2 min)", "tier": 1, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "run_5k_time_1", "name": "5km Time I", "tier": 2, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_8k_complete", "name": "8km Completion", "tier": 2, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_10k_complete", "name": "10km Completion", "tier": 2, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "pushups_60", "name": "Pushups II (60)", "tier": 2, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "pullups_12", "name": "Pull-ups II (12)", "tier": 2, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "squats_120", "name": "Squats II (120)", "tier": 2, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "plank_3min", "name": "Core II (3 min)", "tier": 2, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "run_12k_complete", "name": "12km Continuous", "tier": 3, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_15k_complete", "name": "15km Continuous", "tier": 3, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_5k_time_2", "name": "5km Time II", "tier": 3, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_3k_time_1", "name": "3km Aggressive Time", "tier": 3, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "run_1k_time_1", "name": "1km Time Trial", "tier": 3, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "pushups_80", "name": "Pushups III (80)", "tier": 3, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "pullups_15", "name": "Pull-ups III (15)", "tier": 3, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "squats_180", "name": "Squats III (180)", "tier": 3, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "plank_4min", "name": "Core III (4 min)", "tier": 3, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "body_athletic_foundation", "name": "Athletic Foundation", "tier": 4, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "body_hybrid_conditioning_1", "name": "Hybrid Conditioning I", "tier": 4, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "race_half_official_finish", "name": "Half Marathon Finisher (Official)", "tier": 5, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "race_half_official_compete", "name": "Half Marathon Competitor", "tier": 5, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "pushups_100", "name": "Pushups IV (100)", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "pullups_20", "name": "Pull-ups IV (20)", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "squats_250", "name": "Squats IV (250)", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "plank_5min", "name": "Core IV (5 min)", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "centurion_1", "name": "Centurion I", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "centurion_2", "name": "Centurion II", "tier": 5, "branch_key": "strength", "domain_tags": ["health"]},
        {"node_id": "race_marathon_official_finish", "name": "Official Marathon Finisher", "tier": 6, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "race_marathon_official_compete", "name": "Marathon Competitor", "tier": 6, "branch_key": "run", "domain_tags": ["health"]},
        {"node_id": "iron_centurion", "name": "Iron Centurion", "tier": 6, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "spartan_endurance", "name": "Spartan Endurance", "tier": 6, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "spartan_hybrid", "name": "Spartan Hybrid", "tier": 7, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "endurance_warrior", "name": "Endurance Warrior", "tier": 7, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "power_endurance", "name": "Power Endurance", "tier": 7, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "titan_protocol", "name": "Titan Protocol", "tier": 8, "branch_key": "center", "domain_tags": ["health"]},
        {"node_id": "renaissance_body_1", "name": "Renaissance Body I", "tier": 8, "branch_key": "center", "domain_tags": ["health"]},
    ]
    body_edges = [
        ("body_baseline_mover", "run_3k_complete"), ("body_structured_training", "run_5k_complete"),
        ("body_baseline_mover", "pushups_40"), ("body_baseline_mover", "pullups_8"), ("body_baseline_mover", "squats_80"), ("body_baseline_mover", "plank_2min"),
        ("run_3k_complete", "run_8k_complete"), ("run_5k_complete", "run_5k_time_1"), ("run_5k_complete", "run_10k_complete"),
        ("pushups_40", "pushups_60"), ("pullups_8", "pullups_12"), ("squats_80", "squats_120"), ("plank_2min", "plank_3min"),
        ("run_8k_complete", "run_12k_complete"), ("run_10k_complete", "run_15k_complete"),
        ("run_5k_time_1", "run_5k_time_2"), ("run_5k_time_1", "run_3k_time_1"), ("run_3k_time_1", "run_1k_time_1"),
        ("pushups_60", "pushups_80"), ("pullups_12", "pullups_15"), ("squats_120", "squats_180"), ("plank_3min", "plank_4min"),
        ("run_10k_complete", "body_athletic_foundation"), ("pushups_60", "body_athletic_foundation"), ("pullups_12", "body_athletic_foundation"),
        ("squats_120", "body_athletic_foundation"), ("plank_3min", "body_athletic_foundation"),
        ("run_5k_complete", "body_hybrid_conditioning_1"), ("run_8k_complete", "body_hybrid_conditioning_1"),
        ("body_athletic_foundation", "race_half_official_finish"), ("race_half_official_finish", "race_half_official_compete"),
        ("pushups_80", "pushups_100"), ("pullups_15", "pullups_20"), ("squats_180", "squats_250"), ("plank_4min", "plank_5min"),
        ("pullups_20", "centurion_1"), ("centurion_1", "centurion_2"),
        ("race_half_official_finish", "race_marathon_official_finish"), ("race_marathon_official_finish", "race_marathon_official_compete"),
        ("centurion_2", "iron_centurion"), ("body_hybrid_conditioning_1", "spartan_endurance"),
        ("race_half_official_compete", "spartan_hybrid"), ("iron_centurion", "spartan_hybrid"),
        ("race_marathon_official_finish", "endurance_warrior"), ("pullups_20", "endurance_warrior"),
        ("run_5k_time_2", "power_endurance"), ("centurion_2", "power_endurance"),
        ("spartan_hybrid", "titan_protocol"), ("power_endurance", "titan_protocol"),
        ("race_marathon_official_finish", "renaissance_body_1"), ("titan_protocol", "renaissance_body_1"),
    ]
    body_prereqs = {
        "body_baseline_mover": [
            {"signal_key": "workouts_in_7d", "window_days": 7, "threshold": 3},
            {"signal_key": "single_run", "window_days": 30, "threshold": 1},
            {"signal_key": "pushups_session", "window_days": 30, "threshold": 20},
            {"signal_key": "squats_session", "window_days": 30, "threshold": 30},
        ],
        "body_structured_training": [
            {"signal_key": "workouts_in_7d", "window_days": 7, "threshold": 4},
            {"signal_key": "single_run", "window_days": 30, "threshold": 2},
            {"signal_key": "pushups_session", "window_days": 30, "threshold": 30},
            {"signal_key": "pullups_session", "window_days": 30, "threshold": 5},
        ],
        "run_3k_complete": [{"signal_key": "single_run", "window_days": 60, "threshold": 3}],
        "run_5k_complete": [{"signal_key": "single_run", "window_days": 60, "threshold": 5}],
        "pushups_40": [{"signal_key": "pushups_session", "window_days": 60, "threshold": 40}],
        "pullups_8": [{"signal_key": "pullups_session", "window_days": 60, "threshold": 8}],
        "squats_80": [{"signal_key": "squats_session", "window_days": 60, "threshold": 80}],
        "plank_2min": [{"signal_key": "plank_seconds_session", "window_days": 60, "threshold": 120}],
        "run_5k_time_1": [{"signal_key": "run_5k_best_seconds", "window_days": 365, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["run_5k_time_1_seconds"]}],
        "run_8k_complete": [{"signal_key": "single_run", "window_days": 365, "threshold": 8}],
        "run_10k_complete": [{"signal_key": "single_run", "window_days": 365, "threshold": 10}],
        "pushups_60": [{"signal_key": "pushups_session", "window_days": 365, "threshold": 60}],
        "pullups_12": [{"signal_key": "pullups_session", "window_days": 365, "threshold": 12}],
        "squats_120": [{"signal_key": "squats_session", "window_days": 365, "threshold": 120}],
        "plank_3min": [{"signal_key": "plank_seconds_session", "window_days": 365, "threshold": 180}],
        "run_12k_complete": [{"signal_key": "single_run", "window_days": 365, "threshold": 12}],
        "run_15k_complete": [{"signal_key": "single_run", "window_days": 730, "threshold": 15}],
        "run_5k_time_2": [{"signal_key": "run_5k_best_seconds", "window_days": 730, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["run_5k_time_2_seconds"]}],
        "run_3k_time_1": [{"signal_key": "run_3k_best_seconds", "window_days": 730, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["run_3k_time_1_seconds"]}],
        "run_1k_time_1": [{"signal_key": "run_1k_best_seconds", "window_days": 730, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["run_1k_time_1_seconds"]}],
        "pushups_80": [{"signal_key": "pushups_session", "window_days": 730, "threshold": 80}],
        "pullups_15": [{"signal_key": "pullups_session", "window_days": 730, "threshold": 15}],
        "squats_180": [{"signal_key": "squats_session", "window_days": 730, "threshold": 180}],
        "plank_4min": [{"signal_key": "plank_seconds_session", "window_days": 730, "threshold": 240}],
        "body_hybrid_conditioning_1": [{"signal_key": "hybrid_conditioning_1_sessions", "window_days": 730, "threshold": 1}],
        "race_half_official_finish": [{"signal_key": "official_half_marathon_completed", "window_days": 3650, "threshold": 1}],
        "race_half_official_compete": [{"signal_key": "official_half_marathon_time_best_seconds", "window_days": 3650, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["half_compete_time_seconds"]}],
        "pushups_100": [{"signal_key": "pushups_session", "window_days": 3650, "threshold": 100}],
        "pullups_20": [{"signal_key": "pullups_session", "window_days": 3650, "threshold": 20}],
        "squats_250": [{"signal_key": "squats_session", "window_days": 3650, "threshold": 250}],
        "plank_5min": [{"signal_key": "plank_seconds_session", "window_days": 3650, "threshold": 300}],
        "centurion_1": [{"signal_key": "centurion_1_sessions", "window_days": 3650, "threshold": 1}],
        "centurion_2": [{"signal_key": "centurion_2_sessions", "window_days": 3650, "threshold": 1}],
        "race_marathon_official_finish": [{"signal_key": "official_marathon_completed", "window_days": 3650, "threshold": 1}],
        "race_marathon_official_compete": [{"signal_key": "official_marathon_time_best_seconds", "window_days": 3650, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["marathon_compete_time_seconds"]}],
        "iron_centurion": [{"signal_key": "iron_centurion_sessions", "window_days": 3650, "threshold": 1}],
        "spartan_endurance": [{"signal_key": "spartan_endurance_sessions", "window_days": 3650, "threshold": 1}],
        "power_endurance": [{"signal_key": "run_10k_best_seconds", "window_days": 3650, "operator": "<=", "threshold": RENAISSANCE_THRESHOLDS["run_10k_time_aggressive_seconds"]}],
        "titan_protocol": [{"signal_key": "titan_protocol_sessions", "window_days": 3650, "threshold": 1}],
        "renaissance_body_1": [
            {"signal_key": "official_marathon_completed", "window_days": 3650, "threshold": 1},
            {"signal_key": "iron_or_titan_sessions", "window_days": 3650, "threshold": 1},
            {"signal_key": "run_5k_within_48h_of_marathon", "window_days": 3650, "threshold": 1},
            {"signal_key": "pullups_session", "window_days": 3650, "threshold": 30},
        ],
    }

    mind_nodes = [
        {"node_id": "mind_focus_engine", "name": "Focus Engine", "tier": 0, "branch_key": "core", "domain_tags": ["projects"]},
        {"node_id": "mind_study_consistency", "name": "Study Consistency", "tier": 0, "branch_key": "core", "domain_tags": ["projects"]},
        {"node_id": "math_root", "name": "Mathematics", "tier": 1, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "cs_root", "name": "Computer Science", "tier": 1, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "reading_root", "name": "Reading", "tier": 1, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "writing_root", "name": "Writing", "tier": 1, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "oratory_root", "name": "Oratory", "tier": 1, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "calc_track_root", "name": "Calculus Track", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_track_root", "name": "Linear Algebra Track", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_track_root", "name": "Statistics Track", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "physics_track_root", "name": "Physics Track", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_1", "name": "Calculus I", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_2", "name": "Calculus II", "tier": 3, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_3", "name": "Calculus III", "tier": 4, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_4", "name": "Calculus IV", "tier": 5, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_5", "name": "Calculus V", "tier": 6, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "calc_6", "name": "Calculus VI", "tier": 7, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_1", "name": "Linear Algebra I", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_2", "name": "Linear Algebra II", "tier": 3, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_3", "name": "Linear Algebra III", "tier": 4, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_4", "name": "Linear Algebra IV", "tier": 5, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "linalg_5", "name": "Linear Algebra V", "tier": 6, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_1", "name": "Statistics I", "tier": 2, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_2", "name": "Statistics II", "tier": 3, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_3", "name": "Statistics III", "tier": 4, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_4", "name": "Statistics IV", "tier": 5, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "stats_5", "name": "Statistics V", "tier": 6, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "phys_1", "name": "Physics I", "tier": 3, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "phys_2", "name": "Physics II", "tier": 4, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "phys_3", "name": "Physics III", "tier": 5, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "phys_4", "name": "Physics IV", "tier": 6, "branch_key": "math", "domain_tags": ["projects"]},
        {"node_id": "cs_e2e_root", "name": "End-to-End Track", "tier": 2, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "cs_backend_root", "name": "Backend Systems Track", "tier": 2, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "git_mastery", "name": "Git Mastery", "tier": 2, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "fullstack_crud_app", "name": "Fullstack CRUD App", "tier": 3, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "deploy_fullstack_app", "name": "Deploy Fullstack App", "tier": 4, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "auth_user_management", "name": "Auth & User Management", "tier": 5, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "production_ui_state", "name": "Production UI State", "tier": 6, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "backend_oop_modules", "name": "Backend OOP & Modules", "tier": 2, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "backend_dsa_core", "name": "Backend DSA Core", "tier": 3, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "backend_arch_patterns", "name": "Backend Architecture Patterns", "tier": 4, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "backend_db_optimization", "name": "Backend DB Optimization", "tier": 5, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "backend_async_concurrency", "name": "Backend Async & Concurrency", "tier": 6, "branch_key": "cs", "domain_tags": ["projects"]},
        {"node_id": "ai_root", "name": "AI / LLM Track", "tier": 5, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "systems_root", "name": "Systems Track", "tier": 5, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "ai_ml_basic", "name": "AI ML Basic", "tier": 5, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "ai_nn_pytorch", "name": "AI Neural Net", "tier": 6, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "ai_llm_app", "name": "AI LLM App", "tier": 7, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "ai_multi_agent", "name": "AI Multi-Agent", "tier": 8, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "ai_rag_or_finetune", "name": "AI RAG/Fine-tune", "tier": 9, "branch_key": "ai", "domain_tags": ["projects"]},
        {"node_id": "sys_scalable_api", "name": "Scalable API", "tier": 5, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "sys_docker", "name": "Docker", "tier": 6, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "sys_distributed_deploy", "name": "Distributed Deploy", "tier": 7, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "sys_fault_tolerance", "name": "Fault Tolerance", "tier": 8, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "sys_caching_monitoring", "name": "Caching & Monitoring", "tier": 9, "branch_key": "systems", "domain_tags": ["projects"]},
        {"node_id": "read_5_books", "name": "Read 5 Books", "tier": 2, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_10_books", "name": "Read 10 Books", "tier": 3, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_20_books", "name": "Read 20 Books", "tier": 4, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_35_books", "name": "Read 35 Books", "tier": 5, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_50_books", "name": "Read 50 Books", "tier": 6, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_philosophy_focus", "name": "Philosophy Focus", "tier": 5, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_economics_focus", "name": "Economics Focus", "tier": 5, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "read_literature_focus", "name": "Literature Focus", "tier": 5, "branch_key": "reading", "domain_tags": ["projects"]},
        {"node_id": "publish_3_essays", "name": "Publish 3 Essays", "tier": 2, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "publish_10_essays", "name": "Publish 10 Essays", "tier": 3, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "publish_25_essays", "name": "Publish 25 Essays", "tier": 4, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "publish_50_essays", "name": "Publish 50 Essays", "tier": 5, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "publish_100_essays", "name": "Publish 100 Essays", "tier": 6, "branch_key": "writing", "domain_tags": ["projects"]},
        {"node_id": "speech_5_recorded", "name": "5 Recorded Speeches", "tier": 2, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "debate_join_participate", "name": "Debate Club Participation", "tier": 3, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "debate_win_round", "name": "Debate Round Win", "tier": 4, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "debate_tournament_participate", "name": "Tournament Participation", "tier": 5, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "debate_tournament_podium", "name": "Tournament Podium", "tier": 6, "branch_key": "oratory", "domain_tags": ["projects"]},
        {"node_id": "mind_analytical_programmer", "name": "Analytical Programmer", "tier": 7, "branch_key": "center", "domain_tags": ["projects"]},
        {"node_id": "mind_quant_thinker", "name": "Quant Thinker", "tier": 7, "branch_key": "center", "domain_tags": ["projects"]},
        {"node_id": "mind_technical_communicator", "name": "Technical Communicator", "tier": 7, "branch_key": "center", "domain_tags": ["projects"]},
        {"node_id": "renaissance_mind_1", "name": "Renaissance Mind I", "tier": 8, "branch_key": "center", "domain_tags": ["projects"]},
    ]
    mind_edges = [
        ("mind_focus_engine", "math_root"), ("mind_focus_engine", "cs_root"), ("mind_focus_engine", "reading_root"),
        ("mind_study_consistency", "writing_root"), ("mind_study_consistency", "oratory_root"),
        ("math_root", "calc_track_root"), ("math_root", "linalg_track_root"), ("math_root", "stats_track_root"), ("math_root", "physics_track_root"),
        ("calc_track_root", "calc_1"), ("calc_1", "calc_2"), ("calc_2", "calc_3"), ("calc_3", "calc_4"), ("calc_4", "calc_5"), ("calc_5", "calc_6"),
        ("linalg_track_root", "linalg_1"), ("linalg_1", "linalg_2"), ("linalg_2", "linalg_3"), ("linalg_3", "linalg_4"), ("linalg_4", "linalg_5"),
        ("stats_track_root", "stats_1"), ("stats_1", "stats_2"), ("stats_2", "stats_3"), ("stats_3", "stats_4"), ("stats_4", "stats_5"),
        ("calc_1", "phys_1"), ("physics_track_root", "phys_1"), ("phys_1", "phys_2"), ("phys_2", "phys_3"), ("phys_3", "phys_4"),
        ("cs_root", "cs_e2e_root"), ("cs_root", "cs_backend_root"),
        ("cs_e2e_root", "git_mastery"), ("git_mastery", "fullstack_crud_app"), ("fullstack_crud_app", "deploy_fullstack_app"),
        ("deploy_fullstack_app", "auth_user_management"), ("auth_user_management", "production_ui_state"),
        ("cs_backend_root", "backend_oop_modules"), ("backend_oop_modules", "backend_dsa_core"), ("backend_dsa_core", "backend_arch_patterns"),
        ("backend_arch_patterns", "backend_db_optimization"), ("backend_db_optimization", "backend_async_concurrency"),
        ("backend_arch_patterns", "ai_root"), ("backend_arch_patterns", "systems_root"),
        ("ai_root", "ai_ml_basic"), ("ai_ml_basic", "ai_nn_pytorch"), ("ai_nn_pytorch", "ai_llm_app"), ("ai_llm_app", "ai_multi_agent"), ("ai_multi_agent", "ai_rag_or_finetune"),
        ("systems_root", "sys_scalable_api"), ("sys_scalable_api", "sys_docker"), ("sys_docker", "sys_distributed_deploy"), ("sys_distributed_deploy", "sys_fault_tolerance"), ("sys_fault_tolerance", "sys_caching_monitoring"),
        ("reading_root", "read_5_books"), ("read_5_books", "read_10_books"), ("read_10_books", "read_20_books"), ("read_20_books", "read_35_books"), ("read_35_books", "read_50_books"),
        ("read_10_books", "read_philosophy_focus"), ("read_10_books", "read_economics_focus"), ("read_10_books", "read_literature_focus"),
        ("writing_root", "publish_3_essays"), ("publish_3_essays", "publish_10_essays"), ("publish_10_essays", "publish_25_essays"), ("publish_25_essays", "publish_50_essays"), ("publish_50_essays", "publish_100_essays"),
        ("oratory_root", "speech_5_recorded"), ("speech_5_recorded", "debate_join_participate"), ("debate_join_participate", "debate_win_round"),
        ("debate_win_round", "debate_tournament_participate"), ("debate_tournament_participate", "debate_tournament_podium"),
        ("linalg_3", "mind_analytical_programmer"), ("backend_arch_patterns", "mind_analytical_programmer"),
        ("stats_4", "mind_quant_thinker"), ("backend_dsa_core", "mind_quant_thinker"),
        ("publish_25_essays", "mind_technical_communicator"), ("debate_join_participate", "mind_technical_communicator"), ("calc_3", "mind_technical_communicator"),
        ("calc_5", "renaissance_mind_1"), ("linalg_4", "renaissance_mind_1"), ("stats_4", "renaissance_mind_1"),
        ("backend_db_optimization", "renaissance_mind_1"), ("publish_25_essays", "renaissance_mind_1"),
        ("read_20_books", "renaissance_mind_1"), ("debate_join_participate", "renaissance_mind_1"),
    ]
    mind_prereqs = {
        "mind_focus_engine": [{"signal_key": "deep_work_sessions", "window_days": 365, "threshold": 10}],
        "mind_study_consistency": [{"signal_key": "weeks_with_4_study_sessions", "window_days": 3650, "threshold": 4}],
        "calc_1": [{"signal_key": "exam_passed:calc_1", "window_days": 3650, "threshold": 1}],
        "calc_2": [{"signal_key": "exam_passed:calc_2", "window_days": 3650, "threshold": 1}],
        "calc_3": [{"signal_key": "exam_passed:calc_3", "window_days": 3650, "threshold": 1}],
        "calc_4": [{"signal_key": "exam_passed:calc_4", "window_days": 3650, "threshold": 1}],
        "calc_5": [{"signal_key": "exam_passed:calc_5", "window_days": 3650, "threshold": 1}],
        "calc_6": [{"signal_key": "exam_passed:calc_6", "window_days": 3650, "threshold": 1}],
        "linalg_1": [{"signal_key": "exam_passed:linalg_1", "window_days": 3650, "threshold": 1}],
        "linalg_2": [{"signal_key": "exam_passed:linalg_2", "window_days": 3650, "threshold": 1}],
        "linalg_3": [{"signal_key": "exam_passed:linalg_3", "window_days": 3650, "threshold": 1}],
        "linalg_4": [{"signal_key": "exam_passed:linalg_4", "window_days": 3650, "threshold": 1}],
        "linalg_5": [{"signal_key": "exam_passed:linalg_5", "window_days": 3650, "threshold": 1}],
        "stats_1": [{"signal_key": "exam_passed:stats_1", "window_days": 3650, "threshold": 1}],
        "stats_2": [{"signal_key": "exam_passed:stats_2", "window_days": 3650, "threshold": 1}],
        "stats_3": [{"signal_key": "exam_passed:stats_3", "window_days": 3650, "threshold": 1}],
        "stats_4": [{"signal_key": "exam_passed:stats_4", "window_days": 3650, "threshold": 1}],
        "stats_5": [{"signal_key": "exam_passed:stats_5", "window_days": 3650, "threshold": 1}],
        "phys_1": [{"signal_key": "exam_passed:phys_1", "window_days": 3650, "threshold": 1}],
        "phys_2": [{"signal_key": "exam_passed:phys_2", "window_days": 3650, "threshold": 1}],
        "phys_3": [{"signal_key": "exam_passed:phys_3", "window_days": 3650, "threshold": 1}],
        "phys_4": [{"signal_key": "exam_passed:phys_4", "window_days": 3650, "threshold": 1}],
        "git_mastery": [{"signal_key": "exam_passed:git_mastery", "window_days": 3650, "threshold": 1}],
        "fullstack_crud_app": [{"signal_key": "exam_passed:fullstack_crud_app", "window_days": 3650, "threshold": 1}],
        "deploy_fullstack_app": [{"signal_key": "exam_passed:deploy_fullstack_app", "window_days": 3650, "threshold": 1}],
        "auth_user_management": [{"signal_key": "exam_passed:auth_user_management", "window_days": 3650, "threshold": 1}],
        "production_ui_state": [{"signal_key": "exam_passed:production_ui_state", "window_days": 3650, "threshold": 1}],
        "backend_oop_modules": [{"signal_key": "exam_passed:backend_oop_modules", "window_days": 3650, "threshold": 1}],
        "backend_dsa_core": [{"signal_key": "exam_passed:backend_dsa_core", "window_days": 3650, "threshold": 1}],
        "backend_arch_patterns": [{"signal_key": "exam_passed:backend_arch_patterns", "window_days": 3650, "threshold": 1}],
        "backend_db_optimization": [{"signal_key": "exam_passed:backend_db_optimization", "window_days": 3650, "threshold": 1}],
        "backend_async_concurrency": [{"signal_key": "exam_passed:backend_async_concurrency", "window_days": 3650, "threshold": 1}],
        "ai_ml_basic": [{"signal_key": "exam_passed:ai_ml_basic", "window_days": 3650, "threshold": 1}],
        "ai_nn_pytorch": [{"signal_key": "exam_passed:ai_nn_pytorch", "window_days": 3650, "threshold": 1}],
        "ai_llm_app": [{"signal_key": "exam_passed:ai_llm_app", "window_days": 3650, "threshold": 1}],
        "ai_multi_agent": [{"signal_key": "exam_passed:ai_multi_agent", "window_days": 3650, "threshold": 1}],
        "ai_rag_or_finetune": [{"signal_key": "exam_passed:ai_rag_or_finetune", "window_days": 3650, "threshold": 1}],
        "sys_scalable_api": [{"signal_key": "exam_passed:sys_scalable_api", "window_days": 3650, "threshold": 1}],
        "sys_docker": [{"signal_key": "exam_passed:sys_docker", "window_days": 3650, "threshold": 1}],
        "sys_distributed_deploy": [{"signal_key": "exam_passed:sys_distributed_deploy", "window_days": 3650, "threshold": 1}],
        "sys_fault_tolerance": [{"signal_key": "exam_passed:sys_fault_tolerance", "window_days": 3650, "threshold": 1}],
        "sys_caching_monitoring": [{"signal_key": "exam_passed:sys_caching_monitoring", "window_days": 3650, "threshold": 1}],
        "read_5_books": [{"signal_key": "books_read_count", "window_days": 3650, "threshold": 5}],
        "read_10_books": [{"signal_key": "books_read_count", "window_days": 3650, "threshold": 10}],
        "read_20_books": [{"signal_key": "books_read_count", "window_days": 3650, "threshold": 20}],
        "read_35_books": [{"signal_key": "books_read_count", "window_days": 3650, "threshold": 35}],
        "read_50_books": [{"signal_key": "books_read_count", "window_days": 3650, "threshold": 50}],
        "read_philosophy_focus": [{"signal_key": "books_category_count:philosophy", "window_days": 3650, "threshold": 10}],
        "read_economics_focus": [{"signal_key": "books_category_count:economics", "window_days": 3650, "threshold": 10}],
        "read_literature_focus": [{"signal_key": "books_category_count:literature", "window_days": 3650, "threshold": 10}],
        "publish_3_essays": [{"signal_key": "essays_1000_count", "window_days": 3650, "threshold": 3}],
        "publish_10_essays": [{"signal_key": "essays_published_count", "window_days": 3650, "threshold": 10}],
        "publish_25_essays": [{"signal_key": "essays_published_count", "window_days": 3650, "threshold": 25}],
        "publish_50_essays": [{"signal_key": "essays_published_count", "window_days": 3650, "threshold": 50}],
        "publish_100_essays": [{"signal_key": "essays_published_count", "window_days": 3650, "threshold": 100}],
        "speech_5_recorded": [{"signal_key": "debate_events_count", "window_days": 3650, "threshold": 5}],
        "debate_join_participate": [{"signal_key": "debate_club_participations", "window_days": 3650, "threshold": 1}],
        "debate_win_round": [{"signal_key": "debate_win_count", "window_days": 3650, "threshold": 1}],
        "debate_tournament_participate": [{"signal_key": "debate_tournament_participations", "window_days": 3650, "threshold": 1}],
        "debate_tournament_podium": [{"signal_key": "debate_tournament_podium", "window_days": 3650, "threshold": 1}],
        "renaissance_mind_1": [
            {"signal_key": "exam_passed:calc_5", "window_days": 3650, "threshold": 1},
            {"signal_key": "exam_passed:linalg_4", "window_days": 3650, "threshold": 1},
            {"signal_key": "exam_passed:stats_4", "window_days": 3650, "threshold": 1},
            {"signal_key": "exam_passed:backend_db_optimization", "window_days": 3650, "threshold": 1},
            {"signal_key": "essays_published_count", "window_days": 3650, "threshold": 25},
            {"signal_key": "books_read_count", "window_days": 3650, "threshold": 20},
            {"signal_key": "debate_club_participations", "window_days": 3650, "threshold": 1},
        ],
    }

    _seed_graph_bundle(DEFAULT_DAG_GRAPH_ID, discipline_nodes, discipline_edges, discipline_prereqs)
    _seed_graph_bundle(BODY_DAG_GRAPH_ID, body_nodes, body_edges, body_prereqs)
    _seed_graph_bundle(MIND_DAG_GRAPH_ID, mind_nodes, mind_edges, mind_prereqs)


def add_dag_prereq(
    node_id: str,
    signal_key: str,
    window_days: int,
    operator: str,
    threshold: float,
    weight: float = 1.0,
    progress_cap: float = 1.0,
    notes: str = "",
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
) -> int:
    """Override with cache invalidation."""
    conn = get_connection()
    cur = conn.execute(
        """
        INSERT INTO dag_node_prereqs (
            graph_id, node_id, signal_key, window_days, operator, threshold,
            weight, progress_cap, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (
            graph_id,
            node_id,
            signal_key,
            int(window_days),
            operator,
            float(threshold),
            float(weight),
            float(progress_cap),
            notes or "",
        ),
    )
    prereq_id = cur.fetchone()["id"]
    conn.commit()
    conn.close()
    _invalidate_dag_cache()
    return prereq_id


def get_dag_frontier_nodes(
    user_id: str = DAG_DEFAULT_USER_ID,
    graph_id: str = DEFAULT_DAG_GRAPH_ID,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """Weighted frontier sort with near-miss emphasis."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            s.user_id, s.graph_id, s.node_id, s.state, s.progress, s.near_miss,
            n.name, n.description, n.tier, n.node_type, n.hidden_until_unlocked
        FROM dag_user_node_state s
        JOIN dag_nodes n ON n.node_id = s.node_id
        WHERE s.user_id = ?
          AND s.graph_id = ?
          AND n.graph_id = ?
          AND s.state IN ('locked', 'available')
        """
        ,
        (user_id, graph_id, graph_id),
    ).fetchall()
    conn.close()
    items = [dict(r) for r in rows]
    weights = FRONTIER_SORT_WEIGHTS
    for item in items:
        item["_rank"] = (
            float(weights.get("near_miss", 0.0)) * int(bool(item.get("near_miss")))
            + float(weights.get("progress", 0.0)) * float(item.get("progress") or 0.0)
            + float(weights.get("tier", 0.0)) * float(item.get("tier") or 0.0)
        )
    items.sort(key=lambda x: (x["_rank"], x.get("progress", 0.0)), reverse=True)
    return items[: int(limit)]


def get_brisbane_date() -> str:
    """Get current date in Brisbane timezone (UTC+10)."""
    from datetime import timezone, timedelta
    brisbane_tz = timezone(timedelta(hours=10))
    return datetime.now(brisbane_tz).date().isoformat()

