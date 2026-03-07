# DAG Implementation Notes (Step 0)

## Conventions Observed

- DB access pattern:
  - Centralized in `database/db.py`.
  - `get_connection()` returns sqlite connection with `sqlite3.Row`.
  - Write helpers usually open connection, execute SQL, commit, close in-function.
  - Read helpers usually return plain dict/list structures (`dict(row)`).
- Schema/migrations pattern:
  - Base schema lives in `database/schema.sql`.
  - `init_db()` in `database/db.py` runs `executescript(schema.sql)`.
  - Post-schema migrations are additive/idempotent helper functions (`_ensure_*`) called from `init_db()`.
  - Existing style favors `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`.
- Global constants:
  - Primarily in `database/db.py` (`DOMAIN_XP_PER_LEVEL`, `GLOBAL_XP_PER_LEVEL`, `DB_PATH`).
  - Scoring constants and formulas live in `analytics/scoring.py`.
- Page/callback architecture:
  - Dash Pages (`dash.register_page(__name__)`) under `pages/`.
  - Shared live-loop UX is coordinated in `components/sidebar.py` via stores and interval sync signals.
  - Main app shell/routing is in `app.py` with `use_pages=True` and top-level stores/interval.
- Cadence/event flow:
  - Logging pages (`sleep.py`, `health.py`, `projects.py`, `finance.py`, `journal.py`) persist core metrics.
  - XP awarding is event-driven via `award_xp(...)`, with some idempotency checks by activity key.
  - Daily loop already includes minute refresh and rollover behavior in sidebar callbacks.

## Recommended DAG Touchpoints

- `database/schema.sql`:
  - Add durable DAG tables and indexes.
- `database/db.py`:
  - Add DAG constants, CRUD helpers, and state query/update helpers.
  - Keep functions idempotent and aligned to current connection/commit style.
- `init_db()` in `database/db.py`:
  - No structural change needed if DDL is in `schema.sql`; keep optional `_ensure_*` for future additive migrations.
- Future page integration (not implemented in this step):
  - `pages/summary.py`: show frontier/near-miss nodes and unlock feed.
  - New page candidate: `pages/skills.py` for DAG graph view and node details.
  - `components/sidebar.py`: optional "one more action" prompts based on frontier deficits.
