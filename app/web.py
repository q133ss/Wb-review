from __future__ import annotations

import os
import uuid
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
    delete_ai_example,
    get_admin_user_by_id,
    get_admin_user_by_username,
    get_ai_example,
    get_feedback,
    has_admin_users,
    init_db,
    list_ai_examples,
    list_pending_feedbacks,
    list_sent_feedbacks,
    mark_sent,
    update_draft_response,
    upsert_ai_example,
)
from app.marketplaces.wb import WildberriesClient

load_dotenv()
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("ADMIN_SECRET_KEY", "dev-secret-key")


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
    context = {"tab": tab}
    if tab == "sent":
        context["feedbacks"] = list_sent_feedbacks(conn)
    elif tab == "examples":
        context["examples"] = list_ai_examples(conn)
    else:
        context["feedbacks"] = list_pending_feedbacks(conn)
    return render_template("admin.html", **context)


@app.route("/admin/feedbacks/<int:feedback_id>/save", methods=["POST"])
@login_required
def save_feedback(feedback_id: int):
    conn = _get_db()
    text = (request.form.get("response_text") or "").strip()
    update_draft_response(conn, feedback_id, text)
    flash("Черновик сохранен.", "success")
    return redirect(url_for("admin", tab="inbox"))


@app.route("/admin/feedbacks/<int:feedback_id>/send", methods=["POST"])
@login_required
def send_feedback(feedback_id: int):
    conn = _get_db()
    feedback = get_feedback(conn, feedback_id)
    if not feedback:
        flash("Отзыв не найден.", "error")
        return redirect(url_for("admin", tab="inbox"))
    text = (request.form.get("response_text") or "").strip()
    if not text:
        flash("Нужно заполнить текст ответа.", "error")
        return redirect(url_for("admin", tab="inbox"))
    settings = _get_settings()
    if not settings.wb_api_token:
        flash("WB API токен не настроен.", "error")
        return redirect(url_for("admin", tab="inbox"))
    client = WildberriesClient(settings.wb_api_token)
    try:
        payload = client.send_response(str(feedback["external_id"]), text)
    except Exception as exc:
        flash(f"Ошибка отправки: {exc}", "error")
        return redirect(url_for("admin", tab="inbox"))
    update_draft_response(conn, feedback_id, text)
    mark_sent(conn, feedback_id, text, payload)
    flash("Ответ отправлен в WB.", "success")
    return redirect(url_for("admin", tab="inbox"))


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


def run() -> None:
    host = os.getenv("ADMIN_HOST", "127.0.0.1")
    port = int(os.getenv("ADMIN_PORT", "8000"))
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    run()
