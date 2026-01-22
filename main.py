import json
import time

from app.ai import build_prompt, generate_response
from app.config import get_settings
from app.db import (
    connect,
    get_new_feedbacks,
    get_or_create_marketplace,
    get_ai_examples,
    get_setting,
    init_db,
    insert_or_touch_feedback,
    mark_sent,
    mark_skipped,
    set_setting,
    update_ai_response,
)
from app.marketplaces.wb import WildberriesClient


def ensure_prompt(conn, default_prompt: str) -> str:
    existing = get_setting(conn, "prompt_template")
    if existing == default_prompt:
        return existing
    set_setting(conn, "prompt_template", default_prompt)
    return default_prompt


def upsert_feedbacks(conn, marketplace_id: int, items):
    stored = []
    for item in items:
        row = insert_or_touch_feedback(
            conn,
            {
                "marketplace_id": marketplace_id,
                "external_id": item.external_id,
                "created_at": item.created_at,
                "rating": item.rating,
                "text": item.text,
                "pros": item.pros,
                "cons": item.cons,
                "product_name": item.product_name,
                "status": "new",
                "raw_json": item.raw_json,
            },
        )
        stored.append(row)
    return stored


def _reply_mode(rating: int | None) -> str:
    if rating is None:
        return "skip"
    try:
        value = int(rating)
    except (TypeError, ValueError):
        return "skip"
    if value >= 4:
        return "auto_send"
    if value >= 1:
        return "manual_confirm"
    return "skip"


def process_ai(conn, settings, marketplace_id: int, client: WildberriesClient) -> None:
    prompt_template = ensure_prompt(conn, settings.prompt_template)
    rows = get_new_feedbacks(conn, marketplace_id)
    for row in rows:
        mode = _reply_mode(row["rating"])
        if mode == "skip":
            mark_skipped(conn, row["id"], "manual_needed")
            continue
        if not settings.openai_api_key:
            mark_skipped(conn, row["id"], "ai_skipped_no_key")
            continue
        payload = {
            "text": row["text"] or "",
            "rating": row["rating"] or "",
            "pros": row["pros"] or "",
            "cons": row["cons"] or "",
            "product_name": row["product_name"] or "",
            "marketplace": "WB",
        }
        examples = get_ai_examples(
            conn,
            row["product_name"] or "",
            row["rating"],
        )
        prompt = build_prompt(prompt_template, payload, examples)
        answer = generate_response(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            prompt=prompt,
        )
        update_ai_response(conn, row["id"], answer, settings.openai_model, prompt)
        if mode == "auto_send":
            try:
                sent_payload = client.send_response(str(row["external_id"]), answer)
                mark_sent(conn, row["id"], answer, sent_payload)
            except Exception as exc:
                print(f"Auto-send error for feedback {row['external_id']}: {exc}")


def poll_wb(conn, settings) -> None:
    if not settings.wb_api_token:
        print("WB API token is not set. Set WB_API_TOKEN in .env or environment.")
        return
    client = WildberriesClient(settings.wb_api_token)
    marketplace_id = get_or_create_marketplace(conn, client.code, client.name)
    print("WB poll: fetching unanswered feedbacks...")
    items, raw_payload = client.fetch_unanswered_with_raw()
    print(f"WB poll: received {len(items)} feedback(s).")
    if items:
        upsert_feedbacks(conn, marketplace_id, items)
        if raw_payload is not None:
            with open("wb_feedbacks_last_response.json", "w", encoding="utf-8") as f:
                json.dump(raw_payload, f, ensure_ascii=False, indent=2)
    process_ai(conn, settings, marketplace_id, client)


def main() -> None:
    settings = get_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    print("Polling started. Press Ctrl+C to stop.")
    while True:
        try:
            poll_wb(conn, settings)
        except Exception as exc:
            print(f"Polling error: {exc}")
        time.sleep(settings.poll_interval_sec)


if __name__ == "__main__":
    main()
