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

        CREATE TABLE IF NOT EXISTS marketplace_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            marketplace_id INTEGER NOT NULL UNIQUE,
            marketplace_type TEXT NOT NULL,
            account_name TEXT NOT NULL,
            api_token TEXT NOT NULL,
            business_id INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            auto_reply_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (marketplace_id) REFERENCES marketplaces(id)
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
            draft_response TEXT,
            sent_response TEXT,
            sent_at TEXT,
            sent_raw TEXT,
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (marketplace_id, external_id),
            FOREIGN KEY (marketplace_id) REFERENCES marketplaces(id)
        );

        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            marketplace_id INTEGER NOT NULL,
            external_id TEXT NOT NULL,
            vendor_code TEXT,
            name TEXT,
            description TEXT,
            brand TEXT,
            characteristics TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (marketplace_id, external_id),
            FOREIGN KEY (marketplace_id) REFERENCES marketplaces(id)
        );

        CREATE TABLE IF NOT EXISTS rag_examples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT NOT NULL UNIQUE,
            feedback_created_at TEXT,
            rating INTEGER,
            user_name TEXT,
            text TEXT,
            pros TEXT,
            cons TEXT,
            product_id INTEGER,
            product_name TEXT,
            product_description TEXT,
            product_benefits TEXT,
            answer_text TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        """
    )
    _drop_legacy_rag_table(conn)
    _ensure_rag_example_columns(conn)
    _ensure_feedback_columns(conn)
    _ensure_marketplace_account_columns(conn)
    conn.commit()


def _ensure_feedback_columns(conn: sqlite3.Connection) -> None:
    columns = {
        "draft_response": "TEXT",
        "sent_response": "TEXT",
        "sent_at": "TEXT",
        "sent_raw": "TEXT",
        "product_nm_id": "INTEGER",
    }
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(feedbacks)").fetchall()
    }
    for name, ddl in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE feedbacks ADD COLUMN {name} {ddl}")


def _ensure_marketplace_account_columns(conn: sqlite3.Connection) -> None:
    columns = {
        "auto_reply_enabled": "INTEGER NOT NULL DEFAULT 1",
        "business_id": "INTEGER",
    }
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(marketplace_accounts)").fetchall()
    }
    for name, ddl in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE marketplace_accounts ADD COLUMN {name} {ddl}")


def _ensure_rag_example_columns(conn: sqlite3.Connection) -> None:
    columns = {
        "product_id": "INTEGER",
    }
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(rag_examples)").fetchall()
    }
    for name, ddl in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE rag_examples ADD COLUMN {name} {ddl}")


def _drop_legacy_rag_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS ai_examples")


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
            product_nm_id,
            status,
            raw_json,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
            data.get("product_nm_id"),
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


def get_feedback(conn: sqlite3.Connection, feedback_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM feedbacks WHERE id = ?",
        (feedback_id,),
    ).fetchone()


def get_marketplace(conn: sqlite3.Connection, marketplace_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM marketplaces WHERE id = ?",
        (marketplace_id,),
    ).fetchone()


def list_marketplaces(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT *
        FROM marketplaces
        ORDER BY name ASC, id ASC
        """,
    ).fetchall()
    return list(rows)


def list_marketplace_accounts(
    conn: sqlite3.Connection,
    marketplace_type: str | None = None,
    active_only: bool = True,
) -> list[sqlite3.Row]:
    where = []
    params: list[Any] = []
    if active_only:
        where.append("a.is_active = 1")
    if marketplace_type:
        where.append("a.marketplace_type = ?")
        params.append(marketplace_type)
    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)
    rows = conn.execute(
        f"""
        SELECT a.*, m.name AS marketplace_name, m.code AS marketplace_code
        FROM marketplace_accounts AS a
        JOIN marketplaces AS m ON m.id = a.marketplace_id
        {where_sql}
        ORDER BY a.marketplace_type ASC, a.account_name ASC, a.id ASC
        """,
        params,
    ).fetchall()
    return list(rows)


def get_marketplace_account_by_marketplace_id(
    conn: sqlite3.Connection,
    marketplace_id: int,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT a.*, m.name AS marketplace_name, m.code AS marketplace_code
        FROM marketplace_accounts AS a
        JOIN marketplaces AS m ON m.id = a.marketplace_id
        WHERE a.marketplace_id = ?
        """,
        (marketplace_id,),
    ).fetchone()


def create_marketplace_account(
    conn: sqlite3.Connection,
    marketplace_type: str,
    account_name: str,
    api_token: str,
    marketplace_code: str,
    marketplace_name: str,
    business_id: int | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO marketplaces (code, name) VALUES (?, ?)",
        (marketplace_code, marketplace_name),
    )
    marketplace_id = int(cur.lastrowid)
    cur = conn.execute(
        """
        INSERT INTO marketplace_accounts (
            marketplace_id,
            marketplace_type,
            account_name,
            api_token,
            business_id
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (marketplace_id, marketplace_type, account_name, api_token, business_id),
    )
    conn.commit()
    return int(cur.lastrowid)


def deactivate_marketplace_account(conn: sqlite3.Connection, account_id: int) -> None:
    conn.execute(
        "UPDATE marketplace_accounts SET is_active = 0 WHERE id = ?",
        (account_id,),
    )
    conn.commit()


def set_marketplace_account_auto_reply(
    conn: sqlite3.Connection,
    account_id: int,
    enabled: bool,
) -> None:
    conn.execute(
        "UPDATE marketplace_accounts SET auto_reply_enabled = ? WHERE id = ?",
        (1 if enabled else 0, account_id),
    )
    conn.commit()


def set_marketplace_account_business_id(
    conn: sqlite3.Connection,
    account_id: int,
    business_id: int,
) -> None:
    conn.execute(
        "UPDATE marketplace_accounts SET business_id = ? WHERE id = ?",
        (business_id, account_id),
    )
    conn.commit()


def list_pending_feedbacks(
    conn: sqlite3.Connection,
    marketplace_id: int | None = None,
    limit: int = 200,
) -> list[sqlite3.Row]:
    where = "WHERE f.status != 'sent' AND a.is_active = 1"
    params: list[Any] = []
    if marketplace_id is not None:
        where += " AND f.marketplace_id = ?"
        params.append(marketplace_id)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT f.*, m.name AS marketplace_name, m.code AS marketplace_code
        FROM feedbacks AS f
        JOIN marketplace_accounts AS a ON a.marketplace_id = f.marketplace_id
        JOIN marketplaces AS m ON m.id = f.marketplace_id
        {where}
        ORDER BY f.created_at DESC, f.id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return list(rows)


def list_sent_feedbacks(
    conn: sqlite3.Connection,
    marketplace_id: int | None = None,
    limit: int = 200,
) -> list[sqlite3.Row]:
    where = "WHERE f.status = 'sent'"
    params: list[Any] = []
    if marketplace_id is not None:
        where += " AND f.marketplace_id = ?"
        params.append(marketplace_id)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT f.*, m.name AS marketplace_name, m.code AS marketplace_code
        FROM feedbacks AS f
        JOIN marketplaces AS m ON m.id = f.marketplace_id
        {where}
        ORDER BY f.sent_at DESC, f.id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return list(rows)


def update_draft_response(conn: sqlite3.Connection, feedback_id: int, text: str) -> None:
    conn.execute(
        """
        UPDATE feedbacks
        SET draft_response = ?
        WHERE id = ?
        """,
        (text, feedback_id),
    )
    conn.commit()


def mark_sent(
    conn: sqlite3.Connection,
    feedback_id: int,
    response_text: str,
    raw_payload: dict[str, Any] | None = None,
) -> None:
    payload = json.dumps(raw_payload or {}, ensure_ascii=False)
    conn.execute(
        """
        UPDATE feedbacks
        SET status = 'sent',
            sent_response = ?,
            sent_raw = ?,
            sent_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (response_text, payload, feedback_id),
    )
    conn.commit()


def has_admin_users(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM admin_users LIMIT 1").fetchone()
    return row is not None


def create_admin_user(conn: sqlite3.Connection, username: str, password_hash: str) -> int:
    cur = conn.execute(
        "INSERT INTO admin_users (username, password_hash) VALUES (?, ?)",
        (username, password_hash),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_admin_user_by_username(conn: sqlite3.Connection, username: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM admin_users WHERE username = ?",
        (username,),
    ).fetchone()


def get_admin_user_by_id(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM admin_users WHERE id = ?",
        (user_id,),
    ).fetchone()


def upsert_product(conn: sqlite3.Connection, marketplace_id: int, data: dict[str, Any]) -> None:
    payload = json.dumps(data.get("raw_json") or {}, ensure_ascii=False)
    characteristics = json.dumps(data.get("characteristics") or [], ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO products (
            marketplace_id,
            external_id,
            vendor_code,
            name,
            description,
            brand,
            characteristics,
            raw_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(marketplace_id, external_id) DO UPDATE SET
            vendor_code = excluded.vendor_code,
            name = excluded.name,
            description = excluded.description,
            brand = excluded.brand,
            characteristics = excluded.characteristics,
            raw_json = excluded.raw_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            marketplace_id,
            data["external_id"],
            data.get("vendor_code"),
            data.get("name"),
            data.get("description"),
            data.get("brand"),
            characteristics,
            payload,
        ),
    )
    conn.commit()


def list_products(
    conn: sqlite3.Connection,
    marketplace_id: int | None = None,
) -> list[sqlite3.Row]:
    where = ""
    params: list[Any] = []
    if marketplace_id is not None:
        where = "WHERE marketplace_id = ?"
        params.append(marketplace_id)
    rows = conn.execute(
        f"""
        SELECT *
        FROM products
        {where}
        ORDER BY name ASC, id ASC
        """,
        params,
    ).fetchall()
    return list(rows)


def get_product_by_marketplace_external_id(
    conn: sqlite3.Connection,
    marketplace_id: int,
    external_id: int | str | None,
) -> sqlite3.Row | None:
    if external_id is None:
        return None
    return conn.execute(
        """
        SELECT *
        FROM products
        WHERE marketplace_id = ? AND external_id = ?
        """,
        (marketplace_id, str(external_id)),
    ).fetchone()


def get_product_by_marketplace_name(
    conn: sqlite3.Connection,
    marketplace_id: int,
    name: str | None,
) -> sqlite3.Row | None:
    if not name:
        return None
    return conn.execute(
        """
        SELECT *
        FROM products
        WHERE marketplace_id = ? AND name = ?
        """,
        (marketplace_id, name),
    ).fetchone()


def list_rag_examples(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT *
        FROM rag_examples
        ORDER BY created_at DESC, id DESC
        """,
    ).fetchall()
    return list(rows)


def get_rag_example(conn: sqlite3.Connection, example_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM rag_examples WHERE id = ?",
        (example_id,),
    ).fetchone()


def delete_rag_example(conn: sqlite3.Connection, example_id: int) -> None:
    conn.execute(
        "DELETE FROM rag_examples WHERE id = ?",
        (example_id,),
    )
    conn.commit()


def upsert_rag_example(conn: sqlite3.Connection, data: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO rag_examples (
            external_id,
            feedback_created_at,
            rating,
            user_name,
            text,
            pros,
            cons,
            product_id,
            product_name,
            product_description,
            product_benefits,
            answer_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
            feedback_created_at = excluded.feedback_created_at,
            rating = excluded.rating,
            user_name = excluded.user_name,
            text = excluded.text,
            pros = excluded.pros,
            cons = excluded.cons,
            product_id = excluded.product_id,
            product_name = excluded.product_name,
            product_description = excluded.product_description,
            product_benefits = excluded.product_benefits,
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
            data.get("product_id"),
            data.get("product_name"),
            data.get("product_description"),
            data.get("product_benefits"),
            data.get("answer_text"),
        ),
    )
    conn.commit()


def get_rag_examples(
    conn: sqlite3.Connection,
    product_name: str,
    rating: int | None,
    limit: int = 6,
) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT *
        FROM rag_examples
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
