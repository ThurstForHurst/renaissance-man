"""Daily Journal Page."""
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
from datetime import datetime

from database.db import (
    get_brisbane_date, get_or_create_journal_entry, upsert_journal_entry,
    get_previous_journal_entries, get_daily_snapshot
)

dash.register_page(__name__, path="/journal")


def render_previous_entries(current_date: str):
    """Render collapsed previous journal entries."""
    entries = get_previous_journal_entries(current_date, limit=120)
    if not entries:
        return html.Small("No previous entries yet.", className="text-muted")

    items = []
    for entry in entries:
        body_text = (entry.get("content") or "").strip() or "(No content)"
        items.append(
            html.Details(
                [
                    html.Summary(entry["date"], className="journal-history-summary"),
                    html.Div(
                        dbc.Row([
                            dbc.Col(
                                html.Pre(body_text, className="journal-entry-body mb-0"),
                                md=9,
                                className="pe-md-4"
                            ),
                            dbc.Col(
                                render_daily_metric_cards(entry["date"]),
                                md=3
                            ),
                        ], className="g-0 align-items-start"),
                        className="journal-history-body pt-3"
                    ),
                ],
                className="journal-history-shell mb-3"
            )
        )
    return items


def _render_notes_block(notes_value):
    """Render compact notes with expandable full text."""
    if not notes_value:
        return None

    if isinstance(notes_value, list):
        cleaned = [str(n).strip() for n in notes_value if str(n).strip()]
        notes_text = " | ".join(cleaned)
    else:
        notes_text = str(notes_value).strip()

    if not notes_text:
        return None

    preview = notes_text if len(notes_text) <= 120 else f"{notes_text[:120].rstrip()}..."
    return html.Details(
        [
            html.Summary(f"Notes: {preview}", className="journal-notes-summary"),
            html.Div(notes_text, className="journal-notes-full"),
        ],
        className="journal-notes mt-2"
    )


def render_daily_metric_cards(entry_date: str):
    """Render daily snapshot cards shown on the right side."""
    snapshot = get_daily_snapshot(entry_date)
    sleep = snapshot.get("sleep")
    exercise = snapshot.get("exercise") or {}
    diet = snapshot.get("diet")
    projects = snapshot.get("projects") or {}

    if sleep:
        sleep_body = [
            html.Small(f"Score: {sleep.get('sleep_quality') or '--'}", className="d-block"),
            html.Small(
                f"Duration: {(sleep.get('duration_minutes') or 0) / 60:.1f}h",
                className="d-block text-muted"
            ),
            html.Small(f"Energy: {sleep.get('energy') or '--'}/5", className="d-block text-muted"),
        ]
    else:
        sleep_body = [html.Small("No sleep log", className="text-muted")]

    exercise_body = [
        html.Small(f"Cardio: {exercise.get('cardio_min', 0)} min", className="d-block"),
        html.Small(f"Reps: {exercise.get('resistance_reps', 0)}", className="d-block text-muted"),
    ]
    exercise_notes = _render_notes_block(exercise.get("notes"))

    if diet:
        macro_flags = []
        if diet.get("high_protein"):
            macro_flags.append("High Protein")
        if diet.get("high_carbs"):
            macro_flags.append("High Carbs")
        if diet.get("high_fat"):
            macro_flags.append("High Fat")
        macro_text = ", ".join(macro_flags) if macro_flags else "No macro tags"
        diet_body = [
            html.Small(f"Calories: {diet.get('calories') or '--'}", className="d-block"),
            html.Small(f"Steps: {diet.get('steps') or '--'}", className="d-block text-muted"),
            html.Small(f"Water: {diet.get('water_liters') or '--'}L", className="d-block text-muted"),
            html.Small(macro_text, className="d-block text-muted"),
        ]
        diet_notes = _render_notes_block(diet.get("notes"))
    else:
        diet_body = [html.Small("No diet log", className="text-muted")]
        diet_notes = None

    projects_body = [
        html.Small(f"Time: {projects.get('minutes', 0)} min", className="d-block"),
        html.Small(f"Sessions: {projects.get('sessions', 0)}", className="d-block text-muted"),
    ]
    project_notes = _render_notes_block(projects.get("notes"))
    sleep_notes = _render_notes_block((sleep or {}).get("notes"))

    cards = [
        ("Sleep", sleep_body, sleep_notes),
        ("Exercise", exercise_body, exercise_notes),
        ("Diet", diet_body, diet_notes),
        ("Projects", projects_body, project_notes),
    ]
    return [
        dbc.Card(
            dbc.CardBody([html.Small(title, className="journal-card-title d-block mb-1"), *body, notes_block]),
            className="journal-metric-card mb-2"
        )
        for title, body, notes_block in cards
    ]


def layout():
    today = get_brisbane_date()
    entry = get_or_create_journal_entry(today)
    content = entry.get("content") or ""

    return dbc.Container([
        dcc.Store(id="journal-active-date", data=today),
        dcc.Interval(id="journal-day-check", interval=60000, n_intervals=0),
        html.Div([
            html.H2("Journal", className="mb-1"),
            html.P("Capture daily reflections alongside your performance context.", className="mb-0"),
            html.Small(id="journal-date-label", children=today, className="text-muted d-block mt-1"),
        ], className="app-page-head"),
        html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Textarea(
                        id="journal-editor",
                        value=content,
                        className="journal-editor",
                        placeholder="Start writing..."
                    ),
                    html.Small(id="journal-save-status", className="text-muted d-block mt-2"),
                ], md=9, className="pe-md-4"),
                dbc.Col([
                    html.Div(id="journal-metrics-cards", children=render_daily_metric_cards(today))
                ], md=3),
            ], className="g-0 align-items-start"),
        ], className="journal-current-shell"),
        html.Div(
            id="journal-previous-entries",
            children=render_previous_entries(today),
            className="mt-4"
        ),
    ], fluid=True, className="journal-page app-page-shell")


@callback(
    Output("journal-save-status", "children"),
    Input("journal-editor", "value"),
    State("journal-active-date", "data"),
    prevent_initial_call=True
)
def autosave_journal(content, entry_date):
    """Autosave journal text as user writes."""
    if entry_date is None:
        return dash.no_update
    upsert_journal_entry(entry_date, content or "")
    return f"Saved {datetime.now().strftime('%H:%M:%S')}"


@callback(
    Output("journal-active-date", "data"),
    Output("journal-date-label", "children"),
    Output("journal-editor", "value"),
    Output("journal-previous-entries", "children"),
    Output("journal-metrics-cards", "children"),
    Output("journal-save-status", "children", allow_duplicate=True),
    Input("journal-day-check", "n_intervals"),
    State("journal-active-date", "data"),
    prevent_initial_call=True
)
def refresh_journal_for_new_day(_n, active_date):
    """At day rollover, switch to today's new open entry and refresh side cards."""
    today = get_brisbane_date()
    if today == active_date:
        return (dash.no_update, dash.no_update, dash.no_update,
                dash.no_update, dash.no_update, dash.no_update)

    new_entry = get_or_create_journal_entry(today)
    return (
        today,
        today,
        new_entry.get("content") or "",
        render_previous_entries(today),
        render_daily_metric_cards(today),
        "New day started. Journal switched to today."
    )
