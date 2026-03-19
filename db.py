"""
SQLite storage and deduplication for regulatory items.

Schema (table: items)
─────────────────────
  id              INTEGER  PK autoincrement
  title           TEXT     NOT NULL
  url             TEXT     UNIQUE NOT NULL  ← dedup key
  date            TEXT     ISO 8601 (YYYY-MM-DD)
  source          TEXT     human-readable source name
  jurisdiction    TEXT     MY / HK / US / MY-LABUAN
  tags            TEXT     comma-separated
  summary         TEXT     AI-generated summary (NULL until enriched)
  relevance_score REAL     0.0–1.0 (NULL until scored)
  sent_at         TEXT     datetime when included in a digest (NULL = unsent)
  created_at      TEXT     datetime inserted, DEFAULT datetime('now')

Migrations
──────────
  PRAGMA user_version is used to track schema version.
  v0 → v1: initial items table creation (or migration from legacy articles table)
"""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "regulatory.db"

_SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT    NOT NULL,
    url             TEXT    UNIQUE NOT NULL,
    date            TEXT,
    source          TEXT,
    jurisdiction    TEXT,
    tags            TEXT,
    summary         TEXT,
    relevance_score REAL,
    sent_at         TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_items_date         ON items(date);
CREATE INDEX IF NOT EXISTS idx_items_jurisdiction ON items(jurisdiction);
CREATE INDEX IF NOT EXISTS idx_items_sent_at      ON items(sent_at);
CREATE INDEX IF NOT EXISTS idx_items_score        ON items(relevance_score);
"""


# ── Connection & migration ─────────────────────────────────────────────────────

def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open a connection and apply any pending migrations."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row          # rows behave like dicts
    conn.execute("PRAGMA journal_mode=WAL") # safe for concurrent reads
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= _SCHEMA_VERSION:
        return

    # Check for legacy 'articles' table from earlier schema
    has_articles = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='articles'"
    ).fetchone()

    # Create the new items table
    conn.executescript(_DDL)

    if has_articles:
        # Migrate data from the old articles table
        conn.execute("""
            INSERT OR IGNORE INTO items
                (title, url, date, source, jurisdiction, tags, created_at)
            SELECT
                COALESCE(title, ''),
                url,
                date,
                source,
                jurisdiction,
                tags,
                COALESCE(fetched_at, datetime('now'))
            FROM articles
        """)
        conn.execute("DROP TABLE articles")

    conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
    conn.commit()


# ── Core write operations ──────────────────────────────────────────────────────

def insert_item(item: dict, db_path: Path = DB_PATH) -> Optional[int]:
    """
    Insert a regulatory item.

    Args:
        item: dict with keys: title, url, date, source, jurisdiction,
              tags (list[str]), and optionally summary, relevance_score.

    Returns:
        The new row ``id`` if inserted, or ``None`` if the URL already exists.
    """
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO items
                (title, url, date, source, jurisdiction, tags, summary, relevance_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.get("title", ""),
                item["url"],
                item.get("date"),
                item.get("source"),
                item.get("jurisdiction"),
                ",".join(item["tags"]) if isinstance(item.get("tags"), list)
                else (item.get("tags") or ""),
                item.get("summary"),
                item.get("relevance_score"),
            ),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None         # duplicate URL
    finally:
        conn.close()


def is_duplicate(url: str, db_path: Path = DB_PATH) -> bool:
    """Return True if the URL is already stored."""
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM items WHERE url = ? LIMIT 1", (url,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


# ── Read operations ────────────────────────────────────────────────────────────

def get_unsent_items(
    jurisdiction: Optional[str] = None,
    min_score: Optional[float] = None,
    limit: int = 500,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """
    Return items that have not yet been included in a digest (sent_at IS NULL).

    Args:
        jurisdiction:  filter to a single jurisdiction code (e.g. "HK")
        min_score:     only return items with relevance_score >= this value;
                       items with NULL score are always included
        limit:         max rows to return (default 500)

    Returns:
        List of dicts ordered by date DESC, created_at DESC.
    """
    conn = get_conn(db_path)
    try:
        sql = "SELECT * FROM items WHERE sent_at IS NULL"
        params: list = []

        if jurisdiction:
            sql += " AND jurisdiction = ?"
            params.append(jurisdiction)

        if min_score is not None:
            # Include unscored items (relevance_score IS NULL) so nothing is
            # silently dropped before the scoring step runs.
            sql += " AND (relevance_score IS NULL OR relevance_score >= ?)"
            params.append(min_score)

        sql += " ORDER BY date DESC, created_at DESC LIMIT ?"
        params.append(limit)

        cur = conn.execute(sql, params)
        return [_row_to_dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_item_by_id(item_id: int, db_path: Path = DB_PATH) -> Optional[dict]:
    """Fetch a single item by primary key."""
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM items WHERE id = ?", (item_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def query_recent(
    days: int = 7,
    jurisdiction: Optional[str] = None,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """Return items created in the last N days (convenience query)."""
    conn = get_conn(db_path)
    try:
        sql = "SELECT * FROM items WHERE created_at >= datetime('now', ?)"
        params: list = [f"-{days} days"]
        if jurisdiction:
            sql += " AND jurisdiction = ?"
            params.append(jurisdiction)
        sql += " ORDER BY date DESC"
        cur = conn.execute(sql, params)
        return [_row_to_dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ── Write helpers ──────────────────────────────────────────────────────────────

def update_summary(item_id: int, summary: str, db_path: Path = DB_PATH) -> None:
    """Store an AI-generated summary for an item."""
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE items SET summary = ? WHERE id = ?", (summary, item_id)
        )
        conn.commit()
    finally:
        conn.close()


def update_relevance_score(
    item_id: int, score: float, db_path: Path = DB_PATH
) -> None:
    """Store a relevance score (0.0–1.0) for an item."""
    if not 0.0 <= score <= 1.0:
        raise ValueError(f"relevance_score must be 0.0–1.0, got {score}")
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE items SET relevance_score = ? WHERE id = ?", (score, item_id)
        )
        conn.commit()
    finally:
        conn.close()


def mark_sent(item_ids: list[int], db_path: Path = DB_PATH) -> int:
    """
    Stamp sent_at = now() on the given item IDs.

    Returns the number of rows updated.
    """
    if not item_ids:
        return 0
    placeholders = ",".join("?" * len(item_ids))
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            f"UPDATE items SET sent_at = datetime('now') WHERE id IN ({placeholders})",
            item_ids,
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ── Batch helper (used by fetcher) ─────────────────────────────────────────────

def deduplicate(
    items: list[dict], db_path: Path = DB_PATH
) -> tuple[list[dict], int]:
    """
    Insert new items in bulk; skip URLs already in the DB.

    Returns:
        (new_items, skipped_count)

    This is the primary entry point called by fetcher.fetch_all().
    """
    conn = get_conn(db_path)
    new_items: list[dict] = []
    skipped = 0
    try:
        for item in items:
            try:
                conn.execute(
                    """
                    INSERT INTO items
                        (title, url, date, source, jurisdiction, tags)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.get("title", ""),
                        item["url"],
                        item.get("date"),
                        item.get("source"),
                        item.get("jurisdiction"),
                        ",".join(item["tags"]) if isinstance(item.get("tags"), list)
                        else (item.get("tags") or ""),
                    ),
                )
                conn.commit()
                new_items.append(item)
            except sqlite3.IntegrityError:
                skipped += 1
    finally:
        conn.close()
    return new_items, skipped


# ── Internal ───────────────────────────────────────────────────────────────────

def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    # Expand comma-separated tags back to a list for callers
    if isinstance(d.get("tags"), str):
        d["tags"] = [t for t in d["tags"].split(",") if t]
    return d
