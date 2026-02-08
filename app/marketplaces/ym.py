from __future__ import annotations

from typing import Any, Dict, Generator, Optional

import requests

from .base import FeedbackItem, MarketplaceClient


BASE_URL = "https://api.partner.market.yandex.ru"


class YandexMarketAPIError(RuntimeError):
    pass


class YandexMarketClient(MarketplaceClient):
    code = "ym"
    name = "Яндекс Маркет"

    def __init__(self, api_key: str, timeout: int = 30) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Api-Key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
    ) -> Any:
        url = f"{BASE_URL}{path}"
        resp = self.session.request(method, url, params=params, json=json, timeout=self.timeout)
        if not resp.ok:
            try:
                payload = resp.json()
            except Exception:
                payload = {"raw": resp.text}
            raise YandexMarketAPIError(f"HTTP {resp.status_code} {resp.reason}: {payload}")
        try:
            payload = resp.json()
        except Exception as exc:
            raise YandexMarketAPIError(f"Invalid JSON response: {exc}") from exc
        if payload.get("status") and payload.get("status") != "OK":
            raise YandexMarketAPIError(f"API error payload: {payload}")
        if payload.get("error") or payload.get("errors"):
            raise YandexMarketAPIError(f"API error payload: {payload}")
        return payload

    def get_campaigns(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/v2/campaigns")
        return data.get("campaigns", [])

    def detect_business_id(self) -> int:
        campaigns = self.get_campaigns()
        if not campaigns:
            raise YandexMarketAPIError("No campaigns available for this API key.")
        business = campaigns[0].get("business") or {}
        business_id = business.get("id")
        if not isinstance(business_id, int):
            raise YandexMarketAPIError("Failed to detect business.id from /v2/campaigns.")
        return business_id

    def iter_goods_feedbacks(
        self,
        business_id: int,
        *,
        reaction_status: str = "NEED_REACTION",
        limit: int = 50,
    ) -> Generator[tuple[dict[str, Any], dict[str, Any]], None, None]:
        page_token: Optional[str] = None
        body: Dict[str, Any] = {"reactionStatus": reaction_status}
        while True:
            params: dict[str, Any] = {"limit": limit}
            if page_token:
                params["page_token"] = page_token
            data = self._request(
                "POST",
                f"/v2/businesses/{business_id}/goods-feedback",
                params=params,
                json=body,
            )
            feedbacks = (data.get("result") or {}).get("feedbacks") or []
            for fb in feedbacks:
                yield fb, data
            page_token = (data.get("result") or {}).get("paging", {}).get("nextPageToken")
            if not page_token:
                break

    def fetch_unanswered_with_raw(
        self, business_id: int
    ) -> tuple[list[FeedbackItem], dict[str, Any] | None]:
        items: list[FeedbackItem] = []
        last_payload: dict[str, Any] | None = None
        for fb, payload in self.iter_goods_feedbacks(
            business_id, reaction_status="NEED_REACTION"
        ):
            items.append(self._normalize(fb))
            last_payload = payload
        return items, last_payload

    def fetch_unanswered(self, business_id: int) -> list[FeedbackItem]:
        items, _ = self.fetch_unanswered_with_raw(business_id)
        return items

    def send_response(self, business_id: int, feedback_id: str, text: str) -> dict[str, Any]:
        payload = {
            "feedbackId": int(feedback_id),
            "comment": {"text": text},
        }
        return self._request(
            "POST",
            f"/v2/businesses/{business_id}/goods-feedback/comments/update",
            json=payload,
        )

    def _normalize(self, item: dict[str, Any]) -> FeedbackItem:
        description = item.get("description") or {}
        identifiers = item.get("identifiers") or {}
        stats = item.get("statistics") or {}
        offer_id = identifiers.get("offerId")
        return FeedbackItem(
            external_id=str(item.get("feedbackId") or ""),
            created_at=item.get("createdAt"),
            rating=stats.get("rating"),
            text=str(description.get("comment") or ""),
            pros=str(description.get("advantages") or ""),
            cons=str(description.get("disadvantages") or ""),
            product_name=str(offer_id or ""),
            product_nm_id=None,
            raw_json=item,
        )
