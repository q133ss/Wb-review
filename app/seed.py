import json
import os
from pathlib import Path

from app.db import connect, init_db, upsert_ai_example


def _load_seed(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def seed_ai_examples(db_path: str, seed_path: Path) -> int:
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")
    data = _load_seed(seed_path)
    conn = connect(db_path)
    init_db(conn)
    count = 0
    for item in data:
        fb = item.get("feedback") or {}
        ans = item.get("answer") or {}
        product = fb.get("product") or {}
        upsert_ai_example(
            conn,
            {
                "external_id": str(fb.get("id") or ""),
                "feedback_created_at": fb.get("createdDate"),
                "rating": fb.get("rating"),
                "user_name": fb.get("userName") or "",
                "text": fb.get("text") or "",
                "pros": fb.get("pros") or "",
                "cons": fb.get("cons") or "",
                "product_name": product.get("productName") or "",
                "answer_text": ans.get("text") or "",
            },
        )
        count += 1
    return count


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    db_path = os.getenv("DB_PATH", str(root / "app.db"))
    seed_path = root / "data" / "ai_examples_seed.json"
    count = seed_ai_examples(db_path, seed_path)
    print(f"Inserted/updated {count} AI examples.")


if __name__ == "__main__":
    main()
