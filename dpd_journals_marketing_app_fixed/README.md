# DPD Journals – Realtime Digital Marketing App

A single-file FastAPI app with:
- Realtime(ish) dashboard at `/dashboard`
- UTM tracking pixel at `/track?...`
- Schedule social posts & email campaigns (simulated; logged to DB)
- Simple blog with `/blog/<slug>`, plus `/sitemap.xml` & `/rss.xml`
- SQLite database (`dpd_marketing.sqlite3` by default)

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

Open http://localhost:8000/dashboard

## Notes
- The scheduler is managed via FastAPI lifespan so it won’t double-start with `--reload`.
- Replace the simulated send logic in `process_due_items()` with real provider SDK calls.
- Configure the base URL in sitemap/RSS by setting `SITE_BASE` env var.
