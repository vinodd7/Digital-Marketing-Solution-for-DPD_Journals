# === DPD Journals – Realtime Digital Marketing App (FastAPI, Lifespan-safe) ===
# Save as app.py and run with: uvicorn app:app --reload --port 8000

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import datetime, timedelta
import sqlite3
import os
import uuid

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager

APP_TITLE = "DPD Journals – Digital Marketing Solution"
DB_PATH = os.environ.get("DPD_DB", "dpd_marketing.sqlite3")

# ---- DB helpers ----
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(\"\"\"
        CREATE TABLE IF NOT EXISTS metrics (
            id TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            source TEXT,
            medium TEXT,
            campaign TEXT,
            content TEXT,
            term TEXT,
            ip TEXT,
            user_agent TEXT,
            referrer TEXT
        )
    \"\"\")
    cur.execute(\"\"\"
        CREATE TABLE IF NOT EXISTS social_posts (
            id TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            content TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            status TEXT NOT NULL,
            sent_at TEXT
        )
    \"\"\")
    cur.execute(\"\"\"
        CREATE TABLE IF NOT EXISTS email_campaigns (
            id TEXT PRIMARY KEY,
            subject TEXT NOT NULL,
            body TEXT NOT NULL,
            to_list TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            status TEXT NOT NULL,
            sent_at TEXT
        )
    \"\"\")
    cur.execute(\"\"\"
        CREATE TABLE IF NOT EXISTS blog_posts (
            id TEXT PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    \"\"\")
    conn.commit()
    conn.close()

# ---- Background job ----
def process_due_items():
    conn = get_db()
    cur = conn.cursor()
    now = datetime.utcnow()

    # Social posts
    cur.execute(\"SELECT * FROM social_posts WHERE status='scheduled' AND datetime(scheduled_at) <= datetime(?)\", (now.isoformat(),))
    for row in cur.fetchall():
        cur.execute(\"UPDATE social_posts SET status='sent', sent_at=? WHERE id=?\", (now.isoformat(), row[0]))
        # row: (id, channel, content, scheduled_at, status, sent_at)
        add_metric(conn, ts=now.isoformat(), source=row[1], medium=\"social\", campaign=\"scheduled_social\", content=row[2][:100])

    # Emails
    cur.execute(\"SELECT * FROM email_campaigns WHERE status='scheduled' AND datetime(scheduled_at) <= datetime(?)\", (now.isoformat(),))
    for row in cur.fetchall():
        cur.execute(\"UPDATE email_campaigns SET status='sent', sent_at=? WHERE id=?\", (now.isoformat(), row[0]))
        add_metric(conn, ts=now.isoformat(), source=\"email\", medium=\"email\", campaign=\"scheduled_email\", content=row[1][:100])

    conn.commit()
    conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB
    init_db()

    # Start scheduler safely (and ensure clean shutdown)
    scheduler = BackgroundScheduler()
    scheduler.add_job(process_due_items, IntervalTrigger(seconds=30), id=\"process_due_items\", replace_existing=True)
    scheduler.start()

    app.state.scheduler = scheduler
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)

app = FastAPI(title=APP_TITLE, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[\"*\"],
    allow_credentials=True,
    allow_methods=[\"*\"],
    allow_headers=[\"*\"],
)

# ---- Schemas ----
class SocialSchedule(BaseModel):
    channel: str
    content: str
    scheduled_at: datetime

class EmailSchedule(BaseModel):
    subject: str
    body: str
    to_list: str
    scheduled_at: datetime

class BlogInput(BaseModel):
    slug: str
    title: str
    body: str

# ---- Utils ----
def now_iso() -> str:
    return datetime.utcnow().isoformat()

def add_metric(conn: sqlite3.Connection, **kwargs):
    cur = conn.cursor()
    cur.execute(\"\"\"
        INSERT INTO metrics (id, ts, source, medium, campaign, content, term, ip, user_agent, referrer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    \"\"\", (
        str(uuid.uuid4()),
        kwargs.get(\"ts\", now_iso()),
        kwargs.get(\"source\"),
        kwargs.get(\"medium\"),
        kwargs.get(\"campaign\"),
        kwargs.get(\"content\"),
        kwargs.get(\"term\"),
        kwargs.get(\"ip\"),
        kwargs.get(\"user_agent\"),
        kwargs.get(\"referrer\"),
    ))
    conn.commit()

# ---- Dashboard HTML ----
DASHBOARD_HTML = \"\"\"\
<!doctype html>
<html>
<head>
  <meta charset=\\"utf-8\\" />
  <meta name=\\"viewport\\" content=\\"width=device-width, initial-scale=1\\" />
  <title>DPD Journals – Marketing Dashboard</title>
  <meta name=\\"description\\" content=\\"Realtime marketing dashboard: campaigns, posts, and traffic.\\" />
  <link rel=\\"preconnect\\" href=\\"https://fonts.googleapis.com\\">
  <link rel=\\"preconnect\\" href=\\"https://fonts.gstatic.com\\" crossorigin>
  <link href=\\"https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap\\" rel=\\"stylesheet\\">
  <style>
    body{font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#0b1020;color:#e8ebff}
    header{padding:24px 24px 8px;border-bottom:1px solid #1b2340;background:#0e1530;position:sticky;top:0}
    h1{margin:0;font-size:22px}
    .wrap{padding:24px;display:grid;gap:24px;grid-template-columns:1fr;max-width:1200px;margin:0 auto}
    .grid{display:grid;gap:24px;grid-template-columns:repeat(12,1fr)}
    .card{grid-column:span 12;background:#0f1736;border:1px solid #1d2a5b;border-radius:16px;padding:16px;box-shadow:0 6px 24px rgba(0,0,0,.25)}
    .card h2{margin:0 0 12px;font-size:18px}
    label{font-size:12px;color:#b8c1ff}
    input,textarea,select{width:100%;padding:10px;border-radius:12px;border:1px solid #263476;background:#0c1330;color:#fff}
    button{padding:10px 14px;border-radius:12px;border:0;background:#5a7cff;color:#000;font-weight:700;cursor:pointer}
    .two{grid-column:span 12;display:grid;gap:24px;grid-template-columns:repeat(12,1fr)}
    .two .card{grid-column:span 6}
    @media (max-width:960px){.two .card{grid-column:span 12}}
    .muted{color:#94a3ff;font-size:12px}
    .success{color:#9cffb6}
    .row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  </style>
</head>
<body>
  <header>
    <h1>DPD Journals – Realtime Digital Marketing Dashboard</h1>
    <div class=\\"muted\\">Auto-refresh metrics every 10s • Use forms below to add content & schedule campaigns</div>
  </header>
  <div class=\\"wrap\\">

    <div class=\\"grid\\">
      <div class=\\"card\\" style=\\"grid-column:span 12\\">
        <h2>Traffic (last 14 days)</h2>
        <canvas id=\\"trafficChart\\" height=\\"110\\"></canvas>
      </div>
    </div>

    <div class=\\"two\\">
      <div class=\\"card\\">
        <h2>Schedule Social Post</h2>
        <div class=\\"row\\">
          <div>
            <label>Channel</label>
            <select id=\\"social_channel\\">
              <option>X</option>
              <option>LinkedIn</option>
              <option>Facebook</option>
              <option>Instagram</option>
            </select>
          </div>
          <div>
            <label>When</label>
            <input type=\\"datetime-local\\" id=\\"social_time\\" />
          </div>
        </div>
        <div style=\\"margin-top:8px\\">
          <label>Content</label>
          <textarea id=\\"social_content\\" rows=\\"3\\" placeholder=\\"Write your post... include your UTM link like https://your.site/track?utm_source=linkedin&utm_medium=social&utm_campaign=AugLaunch\\"></textarea>
        </div>
        <div style=\\"margin-top:12px\\">
          <button onclick=\\"scheduleSocial()\\">Schedule</button>
          <span id=\\"social_msg\\" class=\\"muted\\"></span>
        </div>
      </div>

      <div class=\\"card\\">
        <h2>Schedule Email</h2>
        <div class=\\"row\\">
          <div>
            <label>Subject</label>
            <input id=\\"email_subject\\" placeholder=\\"Subject\\" />
          </div>
          <div>
            <label>When</label>
            <input type=\\"datetime-local\\" id=\\"email_time\\" />
          </div>
        </div>
        <div style=\\"margin-top:8px\\">
          <label>To List (comma emails or list name)</label>
          <input id=\\"email_list\\" placeholder=\\"subscribers@dpd, authors@dpd or user1@example.com,user2@example.com\\" />
        </div>
        <div style=\\"margin-top:8px\\">
          <label>Body (HTML allowed)</label>
          <textarea id=\\"email_body\\" rows=\\"4\\" placeholder='e.g., Read our new issue → <a href=\"/blog/ai-in-healthcare?utm_source=newsletter&utm_medium=email&utm_campaign=AugIssue\">Open</a>'></textarea>
        </div>
        <div style=\\"margin-top:12px\\">
          <button onclick=\\"scheduleEmail()\\">Schedule</button>
          <span id=\\"email_msg\\" class=\\"muted\\"></span>
        </div>
      </div>
    </div>

    <div class=\\"card\\">
      <h2>Publish Blog Post (SEO)</h2>
      <div class=\\"row\\">
        <div>
          <label>Slug</label>
          <input id=\\"blog_slug\\" placeholder=\\"ai-in-healthcare\\" />
        </div>
        <div>
          <label>Title</label>
          <input id=\\"blog_title\\" placeholder=\\"AI in Healthcare: 2025 Outlook\\" />
        </div>
      </div>
      <div style=\\"margin-top:8px\\">
        <label>Body (Markdown/HTML)</label>
        <textarea id=\\"blog_body\\" rows=\\"6\\" placeholder=\\"Write your article...\\"></textarea>
      </div>
      <div style=\\"margin-top:12px\\">
        <button onclick=\\"publishBlog()\\">Publish</button>
        <span id=\\"blog_msg\\" class=\\"muted\\"></span>
      </div>
      <div class=\\"muted\\" style=\\"margin-top:8px\\">After publishing, your post appears at <code>/blog/&lt;slug&gt;</code> and is included in <code>/sitemap.xml</code> and <code>/rss.xml</code>.</div>
    </div>

    <div class=\\"card\\">
      <h2>Quick Tracking Pixel Link</h2>
      <div class=\\"muted\\">Share a link like:
        <code>/track?utm_source=linkedin&utm_medium=social&utm_campaign=AugLaunch</code>. Each hit writes a row into metrics.</div>
    </div>

  </div>

  <script src=\\"https://cdn.jsdelivr.net/npm/chart.js\\"></script>
  <script>
    async function fetchSummary(days=14){
      const r = await fetch(`/api/metrics/summary?days=${days}`);
      return await r.json();
    }

    let chart;
    async function drawChart(){
      const data = await fetchSummary(14);
      const ctx = document.getElementById('trafficChart').getContext('2d');
      const labels = data.map(d=>d.date);
      const counts = data.map(d=>d.count);
      if(chart){ chart.destroy(); }
      chart = new Chart(ctx, { type: 'line', data: { labels, datasets: [{ label: 'Visits / Events', data: counts }] }, options: { responsive:true, scales:{ y:{ beginAtZero:true }}}});
    }

    async function scheduleSocial(){
      const payload = {
        channel: document.getElementById('social_channel').value,
        content: document.getElementById('social_content').value,
        scheduled_at: new Date(document.getElementById('social_time').value).toISOString()
      };
      const r = await fetch('/api/schedule/social', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      const res = await r.json();
      document.getElementById('social_msg').innerText = res.message || JSON.stringify(res);
      document.getElementById('social_msg').className = 'success';
    }

    async function scheduleEmail(){
      const payload = {
        subject: document.getElementById('email_subject').value,
        body: document.getElementById('email_body').value,
        to_list: document.getElementById('email_list').value,
        scheduled_at: new Date(document.getElementById('email_time').value).toISOString()
      };
      const r = await fetch('/api/schedule/email', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      const res = await r.json();
      document.getElementById('email_msg').innerText = res.message || JSON.stringify(res);
      document.getElementById('email_msg').className = 'success';
    }

    async function publishBlog(){
      const payload = {
        slug: document.getElementById('blog_slug').value,
        title: document.getElementById('blog_title').value,
        body: document.getElementById('blog_body').value,
      };
      const r = await fetch('/api/blog', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      const res = await r.json();
      document.getElementById('blog_msg').innerText = res.message || JSON.stringify(res);
      document.getElementById('blog_msg').className = 'success';
    }

    drawChart();
    setInterval(drawChart, 10000);
  </script>
</body>
</html>
\"\"\"

# ---- Routes ----
@app.get(\"/\", response_class=HTMLResponse)
@app.get(\"/dashboard\", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)

@app.post(\"/api/blog\")
def create_blog(post: BlogInput):
    conn = get_db()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    try:
        cur.execute(
            \"INSERT INTO blog_posts (id, slug, title, body, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)\",
            (str(uuid.uuid4()), post.slug, post.title, post.body, now, now)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return {\"ok\": False, \"message\": \"Slug already exists\"}
    finally:
        conn.close()
    return {\"ok\": True, \"message\": f\"Published '{post.title}'\"}

@app.post(\"/api/schedule/social\")
def schedule_social(item: SocialSchedule):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        \"INSERT INTO social_posts (id, channel, content, scheduled_at, status) VALUES (?, ?, ?, ?, 'scheduled')\",
        (str(uuid.uuid4()), item.channel, item.content, item.scheduled_at.isoformat())
    )
    conn.commit()
    conn.close()
    return {\"ok\": True, \"message\": f\"Scheduled {item.channel} post for {item.scheduled_at}\"}

@app.post(\"/api/schedule/email\")
def schedule_email(item: EmailSchedule):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        \"INSERT INTO email_campaigns (id, subject, body, to_list, scheduled_at, status) VALUES (?, ?, ?, ?, ?, 'scheduled')\",
        (str(uuid.uuid4()), item.subject, item.body, item.to_list, item.scheduled_at.isoformat())
    )
    conn.commit()
    conn.close()
    return {\"ok\": True, \"message\": f\"Scheduled email for {item.scheduled_at}\"}

@app.get(\"/api/metrics/summary\")
def metrics_summary(days: int = 14):
    conn = get_db()
    cur = conn.cursor()
    start = (datetime.utcnow() - timedelta(days=days-1)).date()
    cur.execute(\"SELECT ts FROM metrics WHERE date(ts) >= date(?)\", (start.isoformat(),))
    rows = cur.fetchall()
    buckets: Dict[str, int] = { (start + timedelta(days=i)).isoformat(): 0 for i in range(days) }
    for r in rows:
        d = r[\"ts\"][:10] if isinstance(r[\"ts\"], str) else str(r[\"ts\"])[:10]
        if d in buckets:
            buckets[d] += 1
    conn.close()
    return [{\"date\": k, \"count\": buckets[k]} for k in sorted(buckets.keys())]

# 1x1 transparent GIF for tracking pixel
GIF_BYTES = (b\"\\x47\\x49\\x46\\x38\\x39\\x61\\x01\\x00\\x01\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\xff\\xff\\xff\\x21\\xf9\\x04\\x01\\x00\\x00\\x00\\x00\\x2c\\x00\\x00\\x00\\x00\\x01\\x00\\x01\\x00\\x00\\x02\\x02\\x44\\x01\\x00\\x3b\")

@app.get(\"/track\")
def track(request: Request,
          utm_source: Optional[str] = None,
          utm_medium: Optional[str] = None,
          utm_campaign: Optional[str] = None,
          utm_content: Optional[str] = None,
          utm_term: Optional[str] = None):
    conn = get_db()
    add_metric(
        conn,
        source=utm_source,
        medium=utm_medium,
        campaign=utm_campaign,
        content=utm_content,
        term=utm_term,
        ip=(request.client.host if request.client else None),
        user_agent=request.headers.get(\"user-agent\"),
        referrer=request.headers.get(\"referer\") or request.headers.get(\"referrer\"),
    )
    conn.close()
    return Response(content=GIF_BYTES, media_type=\"image/gif\")

# robots + sitemap + rss
@app.get(\"/robots.txt\", response_class=PlainTextResponse)
def robots():
    return PlainTextResponse(\"\"\"
User-agent: *
Allow: /
Sitemap: /sitemap.xml
\"\"\".strip())

@app.get(\"/sitemap.xml\")
def sitemap():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(\"SELECT slug, updated_at FROM blog_posts ORDER BY updated_at DESC\")
    rows = cur.fetchall()
    base = os.environ.get(\"SITE_BASE\", \"http://localhost:8000\")
    urls = []
    for r in rows:
        urls.append(f\"<url><loc>{base}/blog/{r['slug']}</loc><lastmod>{r['updated_at']}</lastmod></url>\")
    xml = \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\\n\" + \
          \"<urlset xmlns=\\\"http://www.sitemaps.org/schemas/sitemap/0.9\\\">\" + \"\".join(urls) + \"</urlset>\"
    conn.close()
    return Response(content=xml, media_type=\"application/xml\")

@app.get(\"/rss.xml\")
def rss():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(\"SELECT slug, title, body, updated_at FROM blog_posts ORDER BY updated_at DESC LIMIT 20\")
    base = os.environ.get(\"SITE_BASE\", \"http://localhost:8000\")
    items = []
    for r in cur.fetchall():
        link = f\"{base}/blog/{r['slug']}\"
        items.append(f\"<item><title>{r['title']}</title><link>{link}</link><description><![CDATA[{r['body'][:400]}...]]></description></item>\")
    xml = (\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\"
           \"<rss version=\\\"2.0\\\"><channel>\"
           f\"<title>DPD Journals Feed</title><link>{base}</link><description>Latest content</description>\"
           + \"\".join(items) + \"</channel></rss>\")
    conn.close()
    return Response(content=xml, media_type=\"application/rss+xml\")

@app.get(\"/blog/{slug}\", response_class=HTMLResponse)
def view_blog(slug: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(\"SELECT title, body, updated_at FROM blog_posts WHERE slug = ?\", (slug,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return HTMLResponse(\"<h1>Not found</h1>\", status_code=404)
    html = f\"\"\"
    <!doctype html><html><head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <title>{row['title']} – DPD Journals</title>
      <meta name='description' content='{row['title'][:140]}'>
      <link rel='alternate' type='application/rss+xml' title='RSS' href='/rss.xml'>
      <style>body{{font-family:Inter,system-ui;max-width:800px;margin:40px auto;padding:0 16px;line-height:1.6}} h1{{line-height:1.25}}</style>
    </head><body>
      <article>
        <h1>{row['title']}</h1>
        <div class='muted'>Updated {row['updated_at']}</div>
        <div>{row['body']}</div>
      </article>
    </body></html>
    \"\"\"
    return HTMLResponse(html)

# Run (dev): uvicorn app:app --reload --port 8000
