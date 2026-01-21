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
    raw_json: dict[str, Any]


class MarketplaceClient:
    code: str
    name: str

    def fetch_unanswered(self) -> list[FeedbackItem]:
        raise NotImplementedError
