from __future__ import annotations

import time
from typing import Any

import requests

from .base import FeedbackItem, MarketplaceClient


class WildberriesClient(MarketplaceClient):
    code = "wb"
    name = "Wildberries"

    def __init__(self, api_token: str, rate_delay_sec: float = 0.4) -> None:
        self.api_token = api_token
        self.rate_delay_sec = rate_delay_sec

    def fetch_unanswered(self) -> list[FeedbackItem]:
        items, _ = self.fetch_unanswered_with_raw()
        return items

    def fetch_unanswered_with_raw(self) -> tuple[list[FeedbackItem], dict[str, Any] | None]:
        all_items: list[FeedbackItem] = []
        last_payload: dict[str, Any] | None = None
        skip = 0
        take = 100
        while True:
            data = self._fetch_page(is_answered=0, take=take, skip=skip)
            last_payload = data
            feedbacks = (data.get("data") or {}).get("feedbacks") or []
            if not feedbacks:
                break
            for item in feedbacks:
                all_items.append(self._normalize(item))
            skip += len(feedbacks)
            time.sleep(self.rate_delay_sec)
        return all_items, last_payload

    def _fetch_page(self, is_answered: int, take: int, skip: int) -> dict[str, Any]:
        url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
        headers = {"Authorization": self.api_token, "Accept": "application/json"}
        params = {
            "isAnswered": is_answered,
            "take": take,
            "skip": skip,
            "order": "dateDesc",
        }
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"WB API error {resp.status_code}: {resp.text[:200]}")
        try:
            payload = resp.json()
        except Exception as exc:
            raise RuntimeError(f"WB API invalid JSON: {exc}") from exc
        if payload.get("error"):
            raise RuntimeError(f"WB API error payload: {payload}")
        return payload

    def send_response(self, feedback_id: str, text: str) -> dict[str, Any]:
        url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks/answer"
        headers = {
            "Authorization": self.api_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        payload = {"id": feedback_id, "text": text}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code not in (200, 204):
            raise RuntimeError(f"WB API error {resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 204:
            return {"status": "no_content"}
        try:
            data = resp.json()
        except Exception as exc:
            raise RuntimeError(f"WB API invalid JSON: {exc}") from exc
        if data.get("error"):
            raise RuntimeError(f"WB API error payload: {data}")
        return data

    def _normalize(self, item: dict[str, Any]) -> FeedbackItem:
        product_name = ""
        product = item.get("productDetails") or {}
        if isinstance(product, dict):
            product_name = str(product.get("productName") or "")
        return FeedbackItem(
            external_id=str(item.get("id")),
            created_at=item.get("createdDate"),
            rating=item.get("productValuation"),
            text=str(item.get("text") or ""),
            pros=str(item.get("pros") or ""),
            cons=str(item.get("cons") or ""),
            product_name=product_name,
            raw_json=item,
        )
