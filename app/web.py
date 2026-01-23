from __future__ import annotations

import os
import uuid
from datetime import datetime
from functools import wraps
from typing import Callable

from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from app.config import get_settings, load_dotenv
from app.db import (
    connect,
    create_admin_user,
    create_marketplace_account,
    deactivate_marketplace_account,
    delete_ai_example,
    get_admin_user_by_id,
    get_admin_user_by_username,
    get_ai_example,
    get_feedback,
    get_marketplace_account_by_marketplace_id,
    has_admin_users,
    init_db,
    list_ai_examples,
    list_marketplace_accounts,
    list_pending_feedbacks,
    list_sent_feedbacks,
    mark_sent,
    set_marketplace_account_auto_reply,
    update_draft_response,
    upsert_ai_example,
)
from app.marketplaces.wb import WildberriesClient

load_dotenv()
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("ADMIN_SECRET_KEY", "dev-secret-key")

MARKETPLACE_LABELS = {
    "wb": "Wildberries",
    "ozon": "Ozon",
    "ym": "Яндекс Маркет",
}


@app.template_filter("format_dt")
def format_dt(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%d-%m-%Y %H:%M")
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.strftime("%d-%m-%Y %H:%M")
    except ValueError:
        return text


def _get_settings():
    settings = getattr(g, "settings", None)
    if settings is None:
        settings = get_settings()
        g.settings = settings
    return settings


def _get_db():
    conn = getattr(g, "db", None)
    if conn is None:
        settings = _get_settings()
        conn = connect(settings.db_path)
        init_db(conn)
        g.db = conn
    return conn


@app.teardown_appcontext
def _close_db(exception: Exception | None) -> None:
    conn = getattr(g, "db", None)
    if conn is not None:
        conn.close()


def login_required(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        user_id = session.get("user_id")
        if not user_id:
            return redirect(url_for("login"))
        user = get_admin_user_by_id(_get_db(), int(user_id))
        if not user:
            session.clear()
            return redirect(url_for("login"))
        g.user = user
        return func(*args, **kwargs)

    return wrapper


@app.route("/")
def index():
    return redirect(url_for("admin"))


@app.route("/login", methods=["GET", "POST"])
def login():
    conn = _get_db()
    if not has_admin_users(conn):
        return redirect(url_for("setup"))
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = get_admin_user_by_username(conn, username)
        if not user or not check_password_hash(user["password_hash"], password):
            flash("Неверный логин или пароль.", "error")
            return render_template("login.html")
        session["user_id"] = int(user["id"])
        return redirect(url_for("admin"))
    return render_template("login.html")


@app.route("/setup", methods=["GET", "POST"])
def setup():
    conn = _get_db()
    if has_admin_users(conn):
        return redirect(url_for("login"))
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("Нужны логин и пароль.", "error")
            return render_template("setup.html")
        password_hash = generate_password_hash(password)
        create_admin_user(conn, username, password_hash)
        flash("Администратор создан. Войдите в систему.", "success")
        return redirect(url_for("login"))
    return render_template("setup.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/admin")
@login_required
def admin():
    tab = request.args.get("tab") or "inbox"
    conn = _get_db()
    accounts = list_marketplace_accounts(conn, active_only=False)
    active_accounts = [acc for acc in accounts if int(acc["is_active"]) == 1]
    selected_marketplace_id = _parse_marketplace_id(request.args.get("marketplace_id"))
    selected_account = None
    if selected_marketplace_id is not None:
        selected_account = next(
            (
                acc
                for acc in accounts
                if int(acc["marketplace_id"]) == selected_marketplace_id
            ),
            None,
        )
        if selected_account is None:
            selected_marketplace_id = None
    accounts_by_type = _group_accounts_by_type(active_accounts)
    context = {
        "tab": tab,
        "accounts_by_type": accounts_by_type,
        "marketplace_labels": MARKETPLACE_LABELS,
        "selected_marketplace_id": selected_marketplace_id,
        "selected_account": selected_account,
    }
    if tab == "sent":
        context["feedbacks"] = list_sent_feedbacks(conn, selected_marketplace_id)
    elif tab == "examples":
        context["examples"] = list_ai_examples(conn)
    elif tab == "accounts":
        context["accounts"] = accounts
    else:
        context["feedbacks"] = list_pending_feedbacks(conn, selected_marketplace_id)
    return render_template("admin.html", **context)


@app.route("/admin/feedbacks/<int:feedback_id>/save", methods=["POST"])
@login_required
def save_feedback(feedback_id: int):
    conn = _get_db()
    text = (request.form.get("response_text") or "").strip()
    update_draft_response(conn, feedback_id, text)
    flash("Черновик сохранен.", "success")
    marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
    return redirect(_admin_url("inbox", marketplace_id))


@app.route("/admin/feedbacks/<int:feedback_id>/send", methods=["POST"])
@login_required
def send_feedback(feedback_id: int):
    conn = _get_db()
    feedback = get_feedback(conn, feedback_id)
    if not feedback:
        flash("Отзыв не найден.", "error")
        return redirect(_admin_url("inbox", None))
    text = (request.form.get("response_text") or "").strip()
    if not text:
        flash("Нужно заполнить текст ответа.", "error")
        marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
        return redirect(_admin_url("inbox", marketplace_id))
    account = get_marketplace_account_by_marketplace_id(
        conn, int(feedback["marketplace_id"])
    )
    if not account:
        flash("Аккаунт маркетплейса не найден.", "error")
        marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
        return redirect(_admin_url("inbox", marketplace_id))
    if int(account["is_active"]) != 1:
        flash("Аккаунт деактивирован.", "error")
        marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
        return redirect(_admin_url("inbox", marketplace_id))
    if account["marketplace_type"] != "wb":
        flash("Отправка доступна только для Wildberries.", "error")
        marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
        return redirect(_admin_url("inbox", marketplace_id))
    client = WildberriesClient(account["api_token"])
    try:
        payload = client.send_response(str(feedback["external_id"]), text)
    except Exception as exc:
        flash(f"Ошибка отправки: {exc}", "error")
        marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
        return redirect(_admin_url("inbox", marketplace_id))
    update_draft_response(conn, feedback_id, text)
    mark_sent(conn, feedback_id, text, payload)
    flash("Ответ отправлен в WB.", "success")
    marketplace_id = _parse_marketplace_id(request.form.get("marketplace_id"))
    return redirect(_admin_url("inbox", marketplace_id))


@app.route("/admin/accounts/new", methods=["POST"])
@login_required
def create_account():
    conn = _get_db()
    marketplace_type = (request.form.get("marketplace_type") or "").strip().lower()
    account_name = (request.form.get("account_name") or "").strip()
    api_token = (request.form.get("api_token") or "").strip()
    if marketplace_type not in MARKETPLACE_LABELS:
        flash("Укажите корректный тип маркетплейса.", "error")
        return redirect(url_for("admin", tab="accounts"))
    if not account_name or not api_token:
        flash("Нужно заполнить название и токен аккаунта.", "error")
        return redirect(url_for("admin", tab="accounts"))
    label = MARKETPLACE_LABELS[marketplace_type]
    marketplace_code = f"{marketplace_type}:{uuid.uuid4().hex[:8]}"
    marketplace_name = f"{label} — {account_name}"
    try:
        create_marketplace_account(
            conn,
            marketplace_type=marketplace_type,
            account_name=account_name,
            api_token=api_token,
            marketplace_code=marketplace_code,
            marketplace_name=marketplace_name,
        )
    except Exception as exc:
        flash(f"Не удалось создать аккаунт: {exc}", "error")
        return redirect(url_for("admin", tab="accounts"))
    flash("Аккаунт добавлен.", "success")
    return redirect(url_for("admin", tab="accounts"))


@app.route("/admin/accounts/<int:account_id>/delete", methods=["POST"])
@login_required
def delete_account(account_id: int):
    conn = _get_db()
    deactivate_marketplace_account(conn, account_id)
    flash("Аккаунт удален.", "success")
    return redirect(url_for("admin", tab="accounts"))


@app.route("/admin/accounts/<int:account_id>/auto-reply", methods=["POST"])
@login_required
def toggle_auto_reply(account_id: int):
    conn = _get_db()
    value = (request.form.get("auto_reply_enabled") or "").strip()
    enabled = value == "1"
    set_marketplace_account_auto_reply(conn, account_id, enabled)
    flash("Настройки автоответа обновлены.", "success")
    return redirect(url_for("admin", tab="accounts"))


@app.route("/admin/examples/new", methods=["POST"])
@login_required
def create_example():
    conn = _get_db()
    external_id = (request.form.get("external_id") or "").strip()
    if not external_id:
        external_id = f"manual-{uuid.uuid4().hex}"
    data = {
        "external_id": external_id,
        "feedback_created_at": (request.form.get("feedback_created_at") or "").strip(),
        "rating": _parse_int(request.form.get("rating")),
        "user_name": (request.form.get("user_name") or "").strip(),
        "text": (request.form.get("text") or "").strip(),
        "pros": (request.form.get("pros") or "").strip(),
        "cons": (request.form.get("cons") or "").strip(),
        "product_name": (request.form.get("product_name") or "").strip(),
        "answer_text": (request.form.get("answer_text") or "").strip(),
    }
    if not data["answer_text"]:
        flash("Для примера нужен текст ответа.", "error")
        return redirect(url_for("admin", tab="examples"))
    upsert_ai_example(conn, data)
    flash("Пример сохранен.", "success")
    return redirect(url_for("admin", tab="examples"))


@app.route("/admin/examples/<int:example_id>/update", methods=["POST"])
@login_required
def update_example(example_id: int):
    conn = _get_db()
    existing = get_ai_example(conn, example_id)
    if not existing:
        flash("Пример не найден.", "error")
        return redirect(url_for("admin", tab="examples"))
    data = {
        "external_id": existing["external_id"],
        "feedback_created_at": (request.form.get("feedback_created_at") or "").strip(),
        "rating": _parse_int(request.form.get("rating")),
        "user_name": (request.form.get("user_name") or "").strip(),
        "text": (request.form.get("text") or "").strip(),
        "pros": (request.form.get("pros") or "").strip(),
        "cons": (request.form.get("cons") or "").strip(),
        "product_name": (request.form.get("product_name") or "").strip(),
        "answer_text": (request.form.get("answer_text") or "").strip(),
    }
    if not data["answer_text"]:
        flash("Для примера нужен текст ответа.", "error")
        return redirect(url_for("admin", tab="examples"))
    upsert_ai_example(conn, data)
    flash("Пример обновлен.", "success")
    return redirect(url_for("admin", tab="examples"))


@app.route("/admin/examples/<int:example_id>/delete", methods=["POST"])
@login_required
def remove_example(example_id: int):
    conn = _get_db()
    delete_ai_example(conn, example_id)
    flash("Пример удален.", "success")
    return redirect(url_for("admin", tab="examples"))


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_marketplace_id(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _group_accounts_by_type(accounts: list) -> list[dict]:
    grouped: dict[str, dict] = {}
    for account in accounts:
        code = str(account["marketplace_type"])
        label = MARKETPLACE_LABELS.get(code, code)
        if code not in grouped:
            grouped[code] = {"type": code, "label": label, "accounts": []}
        grouped[code]["accounts"].append(account)
    return list(grouped.values())


def _admin_url(tab: str, marketplace_id: int | None) -> str:
    if marketplace_id is None:
        return url_for("admin", tab=tab)
    return url_for("admin", tab=tab, marketplace_id=marketplace_id)


def run() -> None:
    host = os.getenv("ADMIN_HOST", "127.0.0.1")
    port = int(os.getenv("ADMIN_PORT", "8000"))
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    run()
