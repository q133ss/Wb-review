import json
import sqlite3
from typing import Any


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS marketplaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feedbacks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            marketplace_id INTEGER NOT NULL,
            external_id TEXT NOT NULL,
            created_at TEXT,
            rating INTEGER,
            text TEXT,
            pros TEXT,
            cons TEXT,
            product_name TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            raw_json TEXT,
            ai_response TEXT,
            ai_model TEXT,
            ai_prompt TEXT,
            ai_created_at TEXT,
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (marketplace_id, external_id),
            FOREIGN KEY (marketplace_id) REFERENCES marketplaces(id)
        );

        CREATE TABLE IF NOT EXISTS ai_examples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT NOT NULL UNIQUE,
            feedback_created_at TEXT,
            rating INTEGER,
            user_name TEXT,
            text TEXT,
            pros TEXT,
            cons TEXT,
            product_name TEXT,
            answer_text TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()


def get_or_create_marketplace(conn: sqlite3.Connection, code: str, name: str) -> int:
    row = conn.execute(
        "SELECT id FROM marketplaces WHERE code = ?",
        (code,),
    ).fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute(
        "INSERT INTO marketplaces (code, name) VALUES (?, ?)",
        (code, name),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_setting(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?",
        (key,),
    ).fetchone()
    return row["value"] if row else None


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    conn.commit()


def insert_or_touch_feedback(conn: sqlite3.Connection, data: dict[str, Any]) -> sqlite3.Row:
    payload = json.dumps(data.get("raw_json") or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO feedbacks (
            marketplace_id,
            external_id,
            created_at,
            rating,
            text,
            pros,
            cons,
            product_name,
            status,
            raw_json,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(marketplace_id, external_id) DO UPDATE SET
            last_seen_at = CURRENT_TIMESTAMP
        """,
        (
            data["marketplace_id"],
            data["external_id"],
            data.get("created_at"),
            data.get("rating"),
            data.get("text"),
            data.get("pros"),
            data.get("cons"),
            data.get("product_name"),
            data.get("status", "new"),
            payload,
        ),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT *
        FROM feedbacks
        WHERE marketplace_id = ? AND external_id = ?
        """,
        (data["marketplace_id"], data["external_id"]),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to load feedback after insert")
    return row


def update_ai_response(
    conn: sqlite3.Connection,
    feedback_id: int,
    response: str,
    model: str,
    prompt: str,
) -> None:
    conn.execute(
        """
        UPDATE feedbacks
        SET ai_response = ?, ai_model = ?, ai_prompt = ?, ai_created_at = CURRENT_TIMESTAMP,
            status = 'ai_generated'
        WHERE id = ?
        """,
        (response, model, prompt, feedback_id),
    )
    conn.commit()


def mark_skipped(conn: sqlite3.Connection, feedback_id: int, status: str) -> None:
    conn.execute(
        "UPDATE feedbacks SET status = ? WHERE id = ?",
        (status, feedback_id),
    )
    conn.commit()


def get_new_feedbacks(conn: sqlite3.Connection, marketplace_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT *
        FROM feedbacks
        WHERE marketplace_id = ? AND status = 'new'
        ORDER BY created_at ASC
        """,
        (marketplace_id,),
    ).fetchall()
    return list(rows)


def upsert_ai_example(conn: sqlite3.Connection, data: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ai_examples (
            external_id,
            feedback_created_at,
            rating,
            user_name,
            text,
            pros,
            cons,
            product_name,
            answer_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
            feedback_created_at = excluded.feedback_created_at,
            rating = excluded.rating,
            user_name = excluded.user_name,
            text = excluded.text,
            pros = excluded.pros,
            cons = excluded.cons,
            product_name = excluded.product_name,
            answer_text = excluded.answer_text
        """,
        (
            data["external_id"],
            data.get("feedback_created_at"),
            data.get("rating"),
            data.get("user_name"),
            data.get("text"),
            data.get("pros"),
            data.get("cons"),
            data.get("product_name"),
            data.get("answer_text"),
        ),
    )
    conn.commit()


def get_ai_examples(
    conn: sqlite3.Connection,
    product_name: str,
    rating: int | None,
    limit: int = 6,
) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT *
        FROM ai_examples
        ORDER BY
            CASE WHEN product_name = ? THEN 1 ELSE 0 END DESC,
            CASE WHEN rating = ? THEN 1 ELSE 0 END DESC,
            feedback_created_at DESC,
            id DESC
        LIMIT ?
        """,
        (product_name, rating, limit),
    ).fetchall()
    return list(rows)
