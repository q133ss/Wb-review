from __future__ import annotations

import time
from typing import Any

import requests

from .base import FeedbackItem, MarketplaceClient, ProductItem


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

    def fetch_products(self, limit: int = 100) -> list[ProductItem]:
        items, _ = self.fetch_products_with_raw(limit=limit)
        return items

    def fetch_products_with_raw(
        self,
        limit: int = 100,
    ) -> tuple[list[ProductItem], dict[str, Any] | None]:
        all_items: list[ProductItem] = []
        last_payload: dict[str, Any] | None = None
        cursor: dict[str, Any] = {"limit": limit}
        while True:
            data = self._fetch_products_page(cursor)
            last_payload = data
            cards = data.get("cards") or (data.get("data") or {}).get("cards") or []
            if not cards:
                break
            for item in cards:
                all_items.append(self._normalize_product(item))
            server_cursor = data.get("cursor") or (data.get("data") or {}).get("cursor") or {}
            cursor = {"limit": limit}
            if server_cursor.get("updatedAt"):
                cursor["updatedAt"] = server_cursor.get("updatedAt")
            if server_cursor.get("nmID"):
                cursor["nmID"] = server_cursor.get("nmID")
            if not cursor.get("updatedAt") and not cursor.get("nmID"):
                break
            time.sleep(self.rate_delay_sec)
        return all_items, last_payload

    def _fetch_products_page(self, cursor: dict[str, Any]) -> dict[str, Any]:
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": self.api_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        payload = {
            "settings": {
                "cursor": cursor,
                "filter": {"withPhoto": -1},
            }
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"WB content API error {resp.status_code}: {resp.text[:200]}")
        try:
            payload_data = resp.json()
        except Exception as exc:
            raise RuntimeError(f"WB content API invalid JSON: {exc}") from exc
        if payload_data.get("error"):
            raise RuntimeError(f"WB content API error payload: {payload_data}")
        return payload_data

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
        product_nm_id = None
        if isinstance(product, dict):
            product_name = str(product.get("productName") or "")
            product_nm_id = product.get("nmId")
            if product_nm_id is None:
                product_nm_id = product.get("nmID")
        if product_nm_id is not None:
            try:
                product_nm_id = int(product_nm_id)
            except (TypeError, ValueError):
                product_nm_id = None
        return FeedbackItem(
            external_id=str(item.get("id")),
            created_at=item.get("createdDate"),
            rating=item.get("productValuation"),
            text=str(item.get("text") or ""),
            pros=str(item.get("pros") or ""),
            cons=str(item.get("cons") or ""),
            product_name=product_name,
            product_nm_id=product_nm_id,
            raw_json=item,
        )

    def _normalize_product(self, item: dict[str, Any]) -> ProductItem:
        external_id = item.get("nmID") or item.get("nmId") or ""
        return ProductItem(
            external_id=str(external_id),
            vendor_code=str(item.get("vendorCode") or ""),
            name=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
            brand=str(item.get("brand") or ""),
            characteristics=list(item.get("characteristics") or []),
            raw_json=item,
        )
