import json
import time
import uuid

from app.ai import build_prompt, generate_response
from app.config import WBAccount, get_settings
from app.db import (
    connect,
    get_new_feedbacks,
    get_ai_examples,
    get_setting,
    init_db,
    insert_or_touch_feedback,
    list_marketplace_accounts,
    mark_sent,
    mark_skipped,
    create_marketplace_account,
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


def _last_response_path(account_row) -> str:
    account_key = str(account_row["id"])
    return f"wb_feedbacks_last_response_{account_key}.json"


def poll_wb(conn, settings, account_row) -> None:
    client = WildberriesClient(account_row["api_token"])
    marketplace_id = int(account_row["marketplace_id"])
    print(f"WB poll ({account_row['account_name']}): fetching unanswered feedbacks...")
    items, raw_payload = client.fetch_unanswered_with_raw()
    print(f"WB poll ({account_row['account_name']}): received {len(items)} feedback(s).")
    if items:
        upsert_feedbacks(conn, marketplace_id, items)
        if raw_payload is not None:
            with open(_last_response_path(account_row), "w", encoding="utf-8") as f:
                json.dump(raw_payload, f, ensure_ascii=False, indent=2)
    process_ai(conn, settings, marketplace_id, client)


def _seed_wb_accounts(conn, accounts: tuple[WBAccount, ...]) -> None:
    if not accounts:
        return
    existing = list_marketplace_accounts(conn, marketplace_type="wb", active_only=False)
    if existing:
        return
    for account in accounts:
        account_name = account.key if account.key != "default" else "Основной"
        marketplace_code = f"wb:{uuid.uuid4().hex[:8]}"
        marketplace_name = f"Wildberries — {account_name}"
        create_marketplace_account(
            conn,
            marketplace_type="wb",
            account_name=account_name,
            api_token=account.token,
            marketplace_code=marketplace_code,
            marketplace_name=marketplace_name,
        )


def main() -> None:
    settings = get_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    _seed_wb_accounts(conn, settings.wb_accounts)
    print("Polling started. Press Ctrl+C to stop.")
    while True:
        try:
            accounts = list_marketplace_accounts(conn, marketplace_type="wb", active_only=True)
            if not accounts:
                print("WB accounts are not configured. Add them in the admin panel.")
            for account_row in accounts:
                poll_wb(conn, settings, account_row)
        except Exception as exc:
            print(f"Polling error: {exc}")
        time.sleep(settings.poll_interval_sec)


if __name__ == "__main__":
    main()
