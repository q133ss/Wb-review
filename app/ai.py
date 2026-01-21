from __future__ import annotations

from typing import Any


def build_prompt(template: str, data: dict[str, Any]) -> str:
    safe = {key: (value if value is not None else "") for key, value in data.items()}
    return template.format(**safe)


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
