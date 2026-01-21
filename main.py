# wb_get_feedbacks.py
import os
import json
import time
import requests

URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"

def fetch(token: str, is_answered: int, take: int, skip: int, order: str = "dateDesc", nm_id: int | None = None):
    headers = {"Authorization": token, "Accept": "application/json"}
    params = {
        "isAnswered": is_answered,  # ВАЖНО: 0 или 1
        "take": take,
        "skip": skip,
        "order": order,
    }
    if nm_id is not None:
        params["nmId"] = nm_id

    r = requests.get(URL, headers=headers, params=params, timeout=30)
    # WB иногда отдаёт JSON даже при ошибках
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text[:500]}

def main():
    token = os.getenv("WB_API_TOKEN")
    if not token:
        print("WB_API_TOKEN not set")
        return

    is_answered = 1     # 0 = неотвеченные, 1 = отвеченные
    per_page = 100
    max_total = 300

    all_fb = []
    skip = 0

    while len(all_fb) < max_total:
        take = min(per_page, max_total - len(all_fb))
        code, data = fetch(token, is_answered=is_answered, take=take, skip=skip)

        if code == 200 and not data.get("error"):
            feedbacks = (data.get("data") or {}).get("feedbacks") or []
            if not feedbacks:
                break

            all_fb.extend(feedbacks)
            skip += len(feedbacks)
            time.sleep(0.4)  # лимит запросов
            continue

        print("HTTP", code, data)
        return

    with open("wb_feedbacks.json", "w", encoding="utf-8") as f:
        json.dump(all_fb, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(all_fb)} feedbacks to wb_feedbacks.json")

if __name__ == "__main__":
    main()
