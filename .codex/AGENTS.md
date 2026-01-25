# Project context: wbReviewBot

## Summary
- Service to poll Wildberries (WB) feedbacks, generate AI responses, store everything in SQLite, and send replies back.
- Includes a simple Flask admin UI for reviewing/editing/sending responses, managing accounts, products, and RAG examples.

## Main entry points
- `main.py`: background polling loop, AI response generation, auto-send for high ratings.
- `admin.py`: starts the Flask admin UI (`app/web.py`).

## Key modules
- `app/marketplaces/wb.py`: WB API client (fetch feedbacks, products, send responses).
- `app/ai.py`: prompt assembly + OpenAI chat completions call.
- `app/db.py`: SQLite schema + data access helpers.
- `app/config.py`: .env loading and settings parsing.
- `app/web.py`: admin UI routes and actions.
- `app/rag_seed.py`: imports RAG examples from JSON into DB.

## Core flow
- Poll WB unanswered feedbacks -> store in `feedbacks` table.
- For each new feedback:
  - rating >= 4 -> auto-send if account has auto-reply enabled.
  - rating 1-3 -> AI draft, manual confirmation in admin UI.
  - rating missing/invalid -> skipped.
- Prompt includes product context (products table) + optional RAG examples.

## Storage
- SQLite database (default `app.db`).
- Tables: `marketplaces`, `marketplace_accounts`, `feedbacks`, `products`, `settings`, `rag_examples`, `admin_users`.

## Configuration (.env)
- `OPENAI_API_KEY`, `OPENAI_MODEL`
- `WB_API_TOKEN` or `WB_ACCOUNTS` (name:token list)
- `DB_PATH`, `POLL_INTERVAL_SEC`
- `PROMPT_TEMPLATE`
- Admin UI: `ADMIN_SECRET_KEY`, `ADMIN_HOST`, `ADMIN_PORT`

## Local data files
- `rag_example.json`: default RAG seed data.
- `wb_feedbacks*.json`: saved raw WB API payloads for debugging.
