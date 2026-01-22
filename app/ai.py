from __future__ import annotations

from typing import Any, Iterable


def _get_item_value(item: Any, key: str) -> Any:
    if isinstance(item, dict):
        return item.get(key)
    try:
        return item[key]
    except Exception:
        return None


def _render_examples(examples: Iterable[Any]) -> str:
    items = list(examples)
    if not items:
        return ""
    lines = [
        "Примеры ответов (для стиля, не копировать дословно):",
    ]
    for idx, item in enumerate(items, 1):
        text = str(_get_item_value(item, "text") or "")
        pros = str(_get_item_value(item, "pros") or "")
        cons = str(_get_item_value(item, "cons") or "")
        rating = _get_item_value(item, "rating") or ""
        product = str(_get_item_value(item, "product_name") or "")
        answer = str(_get_item_value(item, "answer_text") or "")
        cleaned = answer.lstrip()
        if cleaned.startswith("Ответ"):
            answer = cleaned[len("Ответ") :].lstrip()
        lines.append(f"{idx}) Отзыв: {text} Плюсы: {pros} Минусы: {cons} "
                     f"Оценка: {rating} Товар: {product}")
        lines.append(f"Ответ: {answer}")
    return "\n".join(lines)


def build_prompt(template: str, data: dict[str, Any], examples: Iterable[Any] = ()) -> str:
    safe = {key: (value if value is not None else "") for key, value in data.items()}
    prompt = template.format(**safe)
    examples_block = _render_examples(examples)
    if examples_block:
        return f"{prompt}\n\n{examples_block}"
    return prompt


def generate_response(
    api_key: str,
    model: str,
    prompt: str,
) -> str:
    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("OpenAI SDK is not installed. Add 'openai' to requirements.") from exc

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content.strip()
