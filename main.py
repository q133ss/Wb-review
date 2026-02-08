import json
import time
import uuid

from app.ai import build_prompt, generate_response
from app.config import WBAccount, get_settings
from app.db import (
    connect,
    get_new_feedbacks,
    get_product_by_marketplace_external_id,
    get_product_by_marketplace_name,
    get_rag_examples,
    get_setting,
    init_db,
    insert_or_touch_feedback,
    list_marketplace_accounts,
    mark_sent,
    mark_skipped,
    create_marketplace_account,
    set_marketplace_account_business_id,
    set_setting,
    update_ai_response,
)
from app.marketplaces.wb import WildberriesClient
from app.marketplaces.ym import YandexMarketClient


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
                "product_nm_id": item.product_nm_id,
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


def _row_value(row, key: str, default=None):
    if row is None:
        return default
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def process_ai(
    conn,
    settings,
    marketplace_id: int,
    client,
    auto_reply_enabled: bool,
    marketplace_label: str,
    send_response=None,
) -> None:
    prompt_template = ensure_prompt(conn, settings.prompt_template)
    rows = get_new_feedbacks(conn, marketplace_id)
    for row in rows:
        try:
            mode = _reply_mode(row["rating"])
            if mode == "skip":
                mark_skipped(conn, row["id"], "manual_needed")
                continue
            if mode == "auto_send" and not auto_reply_enabled:
                mode = "manual_confirm"
            if not settings.openai_api_key:
                mark_skipped(conn, row["id"], "ai_skipped_no_key")
                continue
            product_row = _get_product_context(
                conn,
                marketplace_id,
                row["product_nm_id"],
                row["product_name"],
            )
            payload = {
                "text": row["text"] or "",
                "rating": row["rating"] or "",
                "pros": row["pros"] or "",
                "cons": row["cons"] or "",
                "product_name": row["product_name"] or "",
                "product_title": _row_value(product_row, "name", "") or "",
                "product_description": _row_value(product_row, "description", "") or "",
                "product_benefits": _format_product_benefits(product_row),
                "marketplace": marketplace_label,
            }
            examples = get_rag_examples(
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
                    if send_response is None:
                        sent_payload = client.send_response(str(row["external_id"]), answer)
                    else:
                        sent_payload = send_response(str(row["external_id"]), answer)
                    mark_sent(conn, row["id"], answer, sent_payload)
                except Exception as exc:
                    print(f"Auto-send error for feedback {row['external_id']}: {exc}")
        except Exception as exc:
            mark_skipped(conn, row["id"], "ai_error")
            print(f"AI error for feedback {row['external_id']}: {exc}")


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
    auto_reply_raw = _row_value(account_row, "auto_reply_enabled", 1)
    try:
        auto_reply_enabled = int(auto_reply_raw) == 1
    except (TypeError, ValueError):
        auto_reply_enabled = True
    process_ai(conn, settings, marketplace_id, client, auto_reply_enabled, "WB")


def _get_business_id(conn, account_row, client: YandexMarketClient) -> int:
    business_id = account_row["business_id"]
    if business_id is not None:
        return int(business_id)
    detected = client.detect_business_id()
    set_marketplace_account_business_id(conn, int(account_row["id"]), int(detected))
    return int(detected)


def _ym_last_response_path(account_row) -> str:
    account_key = str(account_row["id"])
    return f"ym_feedbacks_last_response_{account_key}.json"


def poll_ym(conn, settings, account_row) -> None:
    client = YandexMarketClient(account_row["api_token"])
    marketplace_id = int(account_row["marketplace_id"])
    business_id = _get_business_id(conn, account_row, client)
    print(f"YM poll ({account_row['account_name']}): fetching unanswered feedbacks...")
    try:
        items, raw_payload = client.fetch_unanswered_with_raw(business_id)
    except Exception as exc:
        print(f"YM poll ({account_row['account_name']}): error: {exc}")
        return
    print(f"YM poll ({account_row['account_name']}): received {len(items)} feedback(s).")
    if items:
        upsert_feedbacks(conn, marketplace_id, items)
        if raw_payload is not None:
            with open(_ym_last_response_path(account_row), "w", encoding="utf-8") as f:
                json.dump(raw_payload, f, ensure_ascii=False, indent=2)
    auto_reply_raw = _row_value(account_row, "auto_reply_enabled", 1)
    try:
        auto_reply_enabled = int(auto_reply_raw) == 1
    except (TypeError, ValueError):
        auto_reply_enabled = True
    process_ai(
        conn,
        settings,
        marketplace_id,
        client,
        auto_reply_enabled,
        "Яндекс Маркет",
        send_response=lambda feedback_id, text: client.send_response(
            int(business_id), feedback_id, text
        ),
    )


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


def _get_product_context(conn, marketplace_id: int, product_nm_id, product_name):
    product_row = get_product_by_marketplace_external_id(
        conn,
        marketplace_id,
        product_nm_id,
    )
    if product_row is not None:
        return product_row
    return get_product_by_marketplace_name(conn, marketplace_id, product_name)


def _format_product_benefits(product_row) -> str:
    if not product_row:
        return ""
    raw = product_row["characteristics"]
    if not raw:
        return ""
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        return ""
    lines = []
    for item in items:
        name = str(item.get("name") or "").strip()
        value = item.get("value")
        if isinstance(value, list):
            value = ", ".join(str(part) for part in value if part is not None)
        value = str(value or "").strip()
        if not name and not value:
            continue
        if name and value:
            lines.append(f"{name}: {value}")
        elif name:
            lines.append(name)
        else:
            lines.append(value)
    return "\n".join(lines)


def main() -> None:
    settings = get_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    _seed_wb_accounts(conn, settings.wb_accounts)
    print("Polling started. Press Ctrl+C to stop.")
    while True:
        accounts = list_marketplace_accounts(conn, marketplace_type="wb", active_only=True)
        ym_accounts = list_marketplace_accounts(conn, marketplace_type="ym", active_only=True)
        if not accounts and not ym_accounts:
            print("Accounts are not configured. Add them in the admin panel.")
        for account_row in accounts:
            try:
                poll_wb(conn, settings, account_row)
            except Exception as exc:
                print(f"WB poll ({account_row['account_name']}): error: {exc}")
        for account_row in ym_accounts:
            try:
                poll_ym(conn, settings, account_row)
            except Exception as exc:
                print(f"YM poll ({account_row['account_name']}): error: {exc}")
        time.sleep(settings.poll_interval_sec)


if __name__ == "__main__":
    main()
