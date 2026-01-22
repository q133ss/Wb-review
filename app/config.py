import os
from dataclasses import dataclass


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
    openai_api_key: str | None
    openai_model: str
    prompt_template: str


def get_settings() -> Settings:
    load_dotenv()
    return Settings(
        db_path=os.getenv("DB_PATH", "app.db"),
        poll_interval_sec=int(os.getenv("POLL_INTERVAL_SEC", "60")),
        wb_api_token=os.getenv("WB_API_TOKEN") or os.getenv("WB_API_KEY"),
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
