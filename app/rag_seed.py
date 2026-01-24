import json
import os
from pathlib import Path

from app.db import connect, init_db, upsert_rag_example


def _load_seed(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def seed_rag_examples(db_path: str, seed_path: Path) -> int:
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")
    data = _load_seed(seed_path)
    conn = connect(db_path)
    init_db(conn)
    count = 0
    forced_product_id = _parse_int(os.getenv("RAG_PRODUCT_ID"))
    for item in data:
        external_id = str(item.get("external_id") or "").strip()
        answer_text = str(item.get("answer_text") or "").strip()
        if not external_id or not answer_text:
            raise ValueError("Each item must include external_id and answer_text.")
        product_id = _parse_int(item.get("product_id"))
        if product_id is None and forced_product_id is not None:
            product_id = forced_product_id
        upsert_rag_example(
            conn,
            {
                "external_id": external_id,
                "feedback_created_at": item.get("feedback_created_at"),
                "rating": item.get("rating"),
                "user_name": item.get("user_name") or "",
                "text": item.get("text") or "",
                "pros": item.get("pros") or "",
                "cons": item.get("cons") or "",
                "product_id": product_id,
                "product_name": item.get("product_name") or "",
                "product_description": item.get("product_description") or "",
                "product_benefits": item.get("product_benefits") or "",
                "answer_text": answer_text,
            },
        )
        count += 1
    return count


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    db_path = os.getenv("DB_PATH", str(root / "app.db"))
    seed_path = Path(os.getenv("RAG_SEED_PATH", str(root / "rag_example.json")))
    count = seed_rag_examples(db_path, seed_path)
    print(f"Inserted/updated {count} RAG example(s).")


def _parse_int(value) -> int | None:
    if value is None:
        return None
    try:
        value = str(value).strip()
    except Exception:
        return None
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


if __name__ == "__main__":
    main()
