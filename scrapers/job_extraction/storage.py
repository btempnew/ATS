"""Bounded cache and explicit compatibility with existing database schemas."""
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
import json
import logging
import os
from pathlib import Path
import sqlite3
import threading

_memory = OrderedDict()
_lock = threading.RLock()
_key_locks = {}
log = logging.getLogger(__name__)

EXTENDED_FIELDS = {'description_raw', 'extraction', 'salary_period', 'skills_required', 'skills_preferred',
                   'experience_min', 'experience_max', 'structured_fields', 'baseSalary', 'salaryRange'}


def database_rows(rows):
    """New columns are opt-in until the supplied migration has been applied."""
    migrated = os.getenv('JOB_EXTRACTION_DB_FIELDS', 'false').lower() == 'true'
    internal = {'structured_fields', 'baseSalary', 'salaryRange'}
    excluded = internal if migrated else EXTENDED_FIELDS
    return [{k: v for k, v in row.items() if k not in excluded} for row in rows]


def cache_get(key):
    with _lock:
        if key in _memory:
            _memory.move_to_end(key)
            return deepcopy(_memory[key])
    path = os.getenv('JOB_EXTRACTION_CACHE', '')
    if not path:
        return None
    try:
        with _connect(path) as conn:
            row = conn.execute('SELECT payload FROM extraction_cache WHERE cache_key=?', (key,)).fetchone()
        if row:
            return json.loads(row[0])
    except (sqlite3.Error, OSError, ValueError) as exc:
        log.warning('Extraction cache unavailable: %s', type(exc).__name__)
    return None


@contextmanager
def _connect(path):
    Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(Path(path).expanduser()), timeout=5)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('CREATE TABLE IF NOT EXISTS extraction_cache (cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT (unixepoch()))')
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def cache_put(key, value):
    with _lock:
        _memory[key] = deepcopy(value)
        _memory.move_to_end(key)
        while len(_memory) > 512:
            _memory.popitem(last=False)
    path = os.getenv('JOB_EXTRACTION_CACHE', '')
    if not path:
        return
    try:
        with _connect(path) as conn:
            conn.execute('INSERT OR REPLACE INTO extraction_cache(cache_key,payload) VALUES (?,?)', (key, json.dumps(value)))
            limit = max(1, int(os.getenv('JOB_EXTRACTION_CACHE_ENTRIES', '20000')))
            conn.execute('DELETE FROM extraction_cache WHERE cache_key IN (SELECT cache_key FROM extraction_cache ORDER BY created_at DESC, rowid DESC LIMIT -1 OFFSET ?)', (limit,))
    except (sqlite3.Error, OSError, ValueError) as exc:
        log.warning('Extraction cache write failed: %s', type(exc).__name__)


def cached_rules(key, compute):
    value = cache_get(key)
    if value is not None:
        return value
    with _lock:
        key_lock = _key_locks.setdefault(key, threading.Lock())
    try:
        with key_lock:
            value = cache_get(key)
            if value is None:
                value = compute()
                cache_put(key, value)
            return value
    finally:
        with _lock:
            if not key_lock.locked():
                _key_locks.pop(key, None)
