import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WBAccount:
    key: str
    token: str
    code: str
    name: str


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key and key not in os.environ:
                os.environ[key] = value


@dataclass(frozen=True)
class Settings:
    db_path: str
    poll_interval_sec: int
    wb_api_token: str | None
    wb_accounts: tuple[WBAccount, ...]
    openai_api_key: str | None
    openai_model: str
    prompt_template: str


def _parse_wb_accounts(raw: str | None, fallback_token: str | None) -> tuple[WBAccount, ...]:
    accounts: list[WBAccount] = []
    if raw:
        parts = [part.strip() for part in raw.split(",") if part.strip()]
        for idx, part in enumerate(parts, start=1):
            if ":" in part:
                key, token = part.split(":", 1)
                key = key.strip()
                token = token.strip()
            else:
                key = f"account_{idx}"
                token = part.strip()
            if not token:
                continue
            code = "wb" if key in ("default", "wb") else f"wb:{key}"
            name = "Wildberries" if code == "wb" else f"Wildberries ({key})"
            accounts.append(WBAccount(key=key, token=token, code=code, name=name))
    elif fallback_token:
        accounts.append(
            WBAccount(key="default", token=fallback_token, code="wb", name="Wildberries")
        )
    return tuple(accounts)


def get_settings() -> Settings:
    load_dotenv()
    wb_api_token = os.getenv("WB_API_TOKEN") or os.getenv("WB_API_KEY")
    wb_accounts = _parse_wb_accounts(os.getenv("WB_ACCOUNTS"), wb_api_token)
    return Settings(
        db_path=os.getenv("DB_PATH", "app.db"),
        poll_interval_sec=int(os.getenv("POLL_INTERVAL_SEC", "60")),
        wb_api_token=wb_api_token,
        wb_accounts=wb_accounts,
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        prompt_template=os.getenv(
            "PROMPT_TEMPLATE",
            (
                "Ты — специалист поддержки маркетплейса. "
                "Ответь вежливо и кратко. "
                "Отвечай строго на русском. "
                "Верни только текст ответа клиенту без вступлений и пояснений. "
                "Текст клиента: {text}. Оценка: {rating}. "
                "Плюсы: {pros}. Минусы: {cons}. Товар: {product_name}."
            ),
        ),
    )
