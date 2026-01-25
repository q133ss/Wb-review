from __future__ import annotations

import json

from app.ai import build_prompt, generate_response
from app.db import (
    get_new_feedbacks,
    get_product_by_marketplace_external_id,
    get_product_by_marketplace_name,
    get_rag_examples,
    get_setting,
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
    client: WildberriesClient,
    auto_reply_enabled: bool,
) -> None:
    prompt_template = ensure_prompt(conn, settings.prompt_template)
    rows = get_new_feedbacks(conn, marketplace_id)
    for row in rows:
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
            "marketplace": "WB",
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
                sent_payload = client.send_response(str(row["external_id"]), answer)
                mark_sent(conn, row["id"], answer, sent_payload)
            except Exception as exc:
                print(f"Auto-send error for feedback {row['external_id']}: {exc}")


def sync_wb_account(conn, settings, account_row, save_raw: bool = True) -> int:
    client = WildberriesClient(account_row["api_token"])
    marketplace_id = int(account_row["marketplace_id"])
    items, raw_payload = client.fetch_unanswered_with_raw()
    if items:
        upsert_feedbacks(conn, marketplace_id, items)
        if raw_payload is not None and save_raw:
            with open(_last_response_path(account_row), "w", encoding="utf-8") as f:
                json.dump(raw_payload, f, ensure_ascii=False, indent=2)
    auto_reply_raw = _row_value(account_row, "auto_reply_enabled", 1)
    try:
        auto_reply_enabled = int(auto_reply_raw) == 1
    except (TypeError, ValueError):
        auto_reply_enabled = True
    process_ai(conn, settings, marketplace_id, client, auto_reply_enabled)
    return len(items)


def _last_response_path(account_row) -> str:
    account_key = str(account_row["id"])
    return f"wb_feedbacks_last_response_{account_key}.json"


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
