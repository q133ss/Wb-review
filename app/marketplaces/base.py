from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FeedbackItem:
    external_id: str
    created_at: str | None
    rating: int | None
    text: str
    pros: str
    cons: str
    product_name: str
    product_nm_id: int | None
    raw_json: dict[str, Any]


@dataclass(frozen=True)
class ProductItem:
    external_id: str
    vendor_code: str
    name: str
    description: str
    brand: str
    characteristics: list[dict[str, Any]]
    raw_json: dict[str, Any]


class MarketplaceClient:
    code: str
    name: str

    def fetch_unanswered(self) -> list[FeedbackItem]:
        raise NotImplementedError
