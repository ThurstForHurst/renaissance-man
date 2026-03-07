"""
Finance Tracking Page (Weekly System)
"""
import json
from datetime import date, timedelta

import dash
import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback, ctx, dcc, html

from analytics.scoring import award_xp
from database.db import (
    add_asset,
    add_liability,
    delete_asset,
    delete_liability,
    delete_weekly_finance,
    get_all_assets,
    get_all_liabilities,
    get_all_weekly_entries,
    get_connection,
    get_net_worth,
    get_weekly_finance_summary,
    log_weekly_finance,
    update_asset,
    update_liability,
    update_weekly_finance,
)

dash.register_page(__name__)


def is_today_sunday(today: date | None = None) -> bool:
    today = today or date.today()
    return today.weekday() == 6


def get_current_week_range(today: date | None = None) -> tuple[date, date]:
    today = today or date.today()
    monday = today - timedelta(days=today.weekday())
    sunday = today if today.weekday() == 6 else monday + timedelta(days=6)
    return monday, sunday


def format_week_range(start_date: str, end_date: str) -> str:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    return f"{start.strftime('%b')} {start.day} - {end.strftime('%b')} {end.day}, {end.year}"


def calculate_weekly_xp(savings: float) -> int:
    bonus = max(0, min(200, int(savings // 10)))
    return 50 + bonus


def _format_week_display(monday: date, sunday: date) -> str:
    return f"Week of {monday.strftime('%b')} {monday.day} - {sunday.strftime('%b')} {sunday.day}, {sunday.year}"


def _weekly_feedback(message: str, color: str = "success", duration: int = 7000):
    return dbc.Alert(
        [html.I(className="fas fa-check-circle me-2"), message] if color == "success" else message,
        color=color,
        dismissable=True,
        duration=duration,
    )


def create_week_entry_card(entry: dict, editing_id: int | None = None):
    is_editing = editing_id == entry["id"]
    savings_class = "text-success" if entry["savings"] > 0 else "text-danger" if entry["savings"] < 0 else "text-muted"

    if is_editing:
        return dbc.Card(
            dbc.CardBody(
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Strong(format_week_range(entry["week_start_date"], entry["week_end_date"]), className="d-block mb-3 text-primary"),
                                dbc.Label("Income", className="small fw-bold"),
                                dbc.InputGroup(
                                    [dbc.InputGroupText("$"), dbc.Input(id={"type": "edit-week-income", "index": entry["id"]}, type="number", value=entry["income"], min=0, step=0.01)],
                                    size="sm",
                                    className="mb-2",
                                ),
                                dbc.Label("Essentials", className="small fw-bold"),
                                dbc.InputGroup(
                                    [dbc.InputGroupText("$"), dbc.Input(id={"type": "edit-week-essentials", "index": entry["id"]}, type="number", value=entry["essentials"], min=0, step=0.01)],
                                    size="sm",
                                    className="mb-2",
                                ),
                                dbc.Label("Discretionary", className="small fw-bold"),
                                dbc.InputGroup(
                                    [dbc.InputGroupText("$"), dbc.Input(id={"type": "edit-week-discretionary", "index": entry["id"]}, type="number", value=entry["discretionary"], min=0, step=0.01)],
                                    size="sm",
                                    className="mb-2",
                                ),
                                dbc.Label("Notes", className="small fw-bold"),
                                dbc.Textarea(
                                    id={"type": "edit-week-notes", "index": entry["id"]},
                                    value=entry.get("notes") or "",
                                    rows=2,
                                    className="mb-3",
                                ),
                            ],
                            width=9,
                        ),
                        dbc.Col(
                            dbc.ButtonGroup(
                                [
                                    dbc.Button("Save", id={"type": "confirm-week-edit", "index": entry["id"]}, color="success", size="sm"),
                                    dbc.Button("Cancel", id={"type": "cancel-week-edit", "index": entry["id"]}, color="secondary", size="sm", outline=True),
                                ],
                                vertical=True,
                                className="w-100",
                            ),
                            width=3,
                            className="d-flex align-items-end",
                        ),
                    ]
                )
            ),
            className="mb-2 shadow-sm",
        )

    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Strong(format_week_range(entry["week_start_date"], entry["week_end_date"]), className="d-block mb-2 text-primary"),
                            html.Div([html.Span("Income", className="text-muted small"), html.Span(f"${entry['income']:,.2f}", className="float-end")]),
                            html.Div([html.Span("Essentials", className="text-muted small"), html.Span(f"${entry['essentials']:,.2f}", className="float-end")]),
                            html.Div([html.Span("Discretionary", className="text-muted small"), html.Span(f"${entry['discretionary']:,.2f}", className="float-end")]),
                            html.Hr(className="my-2"),
                            html.Div(
                                [
                                    html.Strong("Savings", className="fs-5"),
                                    html.Strong(f"${entry['savings']:,.2f}", className=f"float-end fs-4 {savings_class}"),
                                ],
                                className="mb-1",
                            ),
                            html.Small(entry.get("notes") or "", className="text-muted"),
                        ],
                        width=9,
                    ),
                    dbc.Col(
                        dbc.ButtonGroup(
                            [
                                dbc.Button("Edit", id={"type": "edit-week", "index": entry["id"]}, color="warning", size="sm", outline=True),
                                dbc.Button("Delete", id={"type": "delete-week", "index": entry["id"]}, color="danger", size="sm", outline=True),
                            ],
                            vertical=True,
                            className="w-100",
                        ),
                        width=3,
                        className="d-flex align-items-center",
                    ),
                ]
            )
        ),
        className="mb-2 shadow-sm",
    )


def get_previous_weeks_initial(editing_id: int | None = None):
    entries = get_all_weekly_entries(20)
    if not entries:
        return [html.P("No weekly entries found yet.", className="text-muted text-center py-4")]
    return [create_week_entry_card(entry, editing_id) for entry in entries]


def render_finance_summary_cards():
    weekly_summary = get_weekly_finance_summary()
    return dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Average Weekly Savings", className="text-muted mb-2"),
                            html.H3(f"${weekly_summary['avg_weekly_savings']:,.2f}", className="mb-0"),
                            html.Small("Last 4 weeks", className="text-muted"),
                        ]
                    ),
                    className="shadow-sm summary-card",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("This Month Total", className="text-muted mb-2"),
                            html.H3(f"${weekly_summary['total_this_month']:,.2f}", className="mb-0"),
                            html.Small("Month to date", className="text-muted"),
                        ]
                    ),
                    className="shadow-sm summary-card",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Best Week", className="text-muted mb-2"),
                            html.H3(f"${weekly_summary['best_week_savings']:,.2f}", className="mb-0"),
                            html.Small("Personal best", className="text-muted"),
                        ]
                    ),
                    className="shadow-sm summary-card",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Weeks Logged", className="text-muted mb-2"),
                            html.H3(f"{weekly_summary['weeks_logged']} weeks", className="mb-0"),
                            html.Small("Total tracked", className="text-muted"),
                        ]
                    ),
                    className="shadow-sm summary-card",
                ),
                md=3,
            ),
        ],
        className="mb-4",
    )


def render_networth_summary():
    net_worth = get_net_worth()
    return html.Div(
        [
            html.H3(
                f"${net_worth['net_worth']:,.2f}",
                className=f"mb-2 {'text-success' if net_worth['net_worth'] >= 0 else 'text-danger'}",
            ),
            html.Small(
                [
                    f"Assets: ${net_worth['total_assets']:,.2f}",
                    html.Br(),
                    f"Liabilities: ${net_worth['total_liabilities']:,.2f}",
                ],
                className="text-muted",
            ),
        ],
        className="text-center mb-3 pb-3 border-bottom",
    )


def layout():
    sunday_open = is_today_sunday()
    monday, sunday = get_current_week_range()
    week_label = _format_week_display(monday, sunday)

    left_card_style = {"height": "100%"}
    if not sunday_open:
        left_card_style["opacity"] = 0.6

    return dbc.Container(
        [
            html.Div(
                [
                    html.H2("Finance Tracking", className="mb-1"),
                    html.P("Review weekly cash flow and keep net worth updated.", className="mb-0"),
                ],
                className="app-page-head",
            ),
            html.Div(id="finance-summary-row", children=render_finance_summary_cards()),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(html.H5("Log This Week", className="mb-0")),
                                dbc.CardBody(
                                    [
                                        dbc.Alert(
                                            f"Weekly reporting opens on Sundays. Current week: {week_label}",
                                            color="info",
                                            className="mb-3",
                                        )
                                        if not sunday_open
                                        else html.Div(),
                                        dbc.Label("Week", className="fw-bold"),
                                        dbc.Input(value=week_label, disabled=True, className="mb-3"),
                                        dbc.Label("Income This Week", className="fw-bold"),
                                        dbc.InputGroup(
                                            [
                                                dbc.InputGroupText("$"),
                                                dbc.Input(
                                                    id="weekly-income",
                                                    type="number",
                                                    min=0,
                                                    step=0.01,
                                                    placeholder="Total income",
                                                    disabled=not sunday_open,
                                                ),
                                            ],
                                            className="mb-2",
                                        ),
                                        dbc.Label("Essential Expenses", className="fw-bold"),
                                        dbc.InputGroup(
                                            [
                                                dbc.InputGroupText("$"),
                                                dbc.Input(
                                                    id="weekly-essentials",
                                                    type="number",
                                                    min=0,
                                                    step=0.01,
                                                    placeholder="Rent, groceries, bills, transport",
                                                    disabled=not sunday_open,
                                                ),
                                            ],
                                            className="mb-2",
                                        ),
                                        dbc.Label("Discretionary Expenses", className="fw-bold"),
                                        dbc.InputGroup(
                                            [
                                                dbc.InputGroupText("$"),
                                                dbc.Input(
                                                    id="weekly-discretionary",
                                                    type="number",
                                                    min=0,
                                                    step=0.01,
                                                    placeholder="Entertainment, dining out, hobbies",
                                                    disabled=not sunday_open,
                                                ),
                                            ],
                                            className="mb-3",
                                        ),
                                        html.Hr(className="my-3"),
                                        html.Div(id="weekly-savings-display", className="mb-3"),
                                        dbc.Label("Notes (optional)", className="fw-bold"),
                                        dbc.Textarea(
                                            id="weekly-notes",
                                            rows=3,
                                            placeholder="Any observations about this week...",
                                            disabled=not sunday_open,
                                            className="mb-3",
                                        ),
                                        dbc.Button("Log Week", id="weekly-submit", color="success", className="w-100", disabled=not sunday_open),
                                        html.Div(id="weekly-feedback", className="mt-3"),
                                    ]
                                ),
                            ],
                            className="shadow-sm mb-4",
                            style=left_card_style,
                        ),
                        md=7,
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader([html.H5("Net Worth", className="mb-0")]),
                                dbc.CardBody(
                                    [
                                        html.Div(id="finance-networth-summary", children=render_networth_summary()),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        html.Strong("Assets", className="text-success"),
                                                        dbc.Button(
                                                            "+",
                                                            id="add-asset-btn",
                                                            color="success",
                                                            size="sm",
                                                            outline=True,
                                                            className="float-end",
                                                            style={"padding": "0.15rem 0.5rem"},
                                                        ),
                                                    ],
                                                    className="mb-2",
                                                ),
                                                html.Div(id="assets-list", style={"maxHeight": "200px", "overflowY": "auto"}),
                                            ],
                                            className="mb-3",
                                        ),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        html.Strong("Liabilities", className="text-danger"),
                                                        dbc.Button(
                                                            "+",
                                                            id="add-liability-btn",
                                                            color="danger",
                                                            size="sm",
                                                            outline=True,
                                                            className="float-end",
                                                            style={"padding": "0.15rem 0.5rem"},
                                                        ),
                                                    ],
                                                    className="mb-2",
                                                ),
                                                html.Div(id="liabilities-list", style={"maxHeight": "200px", "overflowY": "auto"}),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            className="shadow-sm mb-4",
                            style={"height": "100%"},
                        ),
                        md=5,
                    ),
                ],
                style={"alignItems": "stretch"},
            ),
            dbc.Card(
                [
                    dbc.CardHeader([html.H5("Previous Weeks", className="mb-0")]),
                    dbc.CardBody([html.Div(id="previous-weeks-list", children=get_previous_weeks_initial())]),
                ],
                className="shadow-sm mb-4",
            ),
            dbc.Modal(
                [
                    dbc.ModalHeader("Add/Edit Asset"),
                    dbc.ModalBody(
                        [
                            dbc.Label("Name"),
                            dbc.Input(id="asset-name-input", type="text", placeholder="e.g., Tesla Stock", className="mb-2"),
                            dbc.Label("Category"),
                            dcc.Dropdown(
                                id="asset-category-input",
                                options=[
                                    {"label": "Stocks", "value": "stocks"},
                                    {"label": "Property", "value": "property"},
                                    {"label": "Vehicle", "value": "vehicle"},
                                    {"label": "Cash", "value": "cash"},
                                    {"label": "Retirement", "value": "retirement"},
                                    {"label": "Other", "value": "other"},
                                ],
                                className="mb-2",
                            ),
                            dbc.Label("Current Value"),
                            dbc.InputGroup(
                                [dbc.InputGroupText("$"), dbc.Input(id="asset-value-input", type="number", placeholder="0.00", min=0, step=0.01)],
                                className="mb-2",
                            ),
                            dbc.Label("Notes (optional)"),
                            dbc.Textarea(id="asset-notes-input", placeholder="Additional details", rows=2),
                            dcc.Store(id="asset-edit-id", data=None),
                        ]
                    ),
                    dbc.ModalFooter(
                        [
                            dbc.Button("Save", id="save-asset-btn", color="success", className="me-2"),
                            dbc.Button("Cancel", id="cancel-asset-btn", color="secondary"),
                        ]
                    ),
                ],
                id="asset-modal",
                is_open=False,
                style={"zIndex": "1200"},
            ),
            dbc.Modal(
                [
                    dbc.ModalHeader("Add/Edit Liability"),
                    dbc.ModalBody(
                        [
                            dbc.Label("Name"),
                            dbc.Input(id="liability-name-input", type="text", placeholder="e.g., Car Loan", className="mb-2"),
                            dbc.Label("Category"),
                            dcc.Dropdown(
                                id="liability-category-input",
                                options=[
                                    {"label": "Mortgage", "value": "mortgage"},
                                    {"label": "Loan", "value": "loan"},
                                    {"label": "Credit Card", "value": "credit_card"},
                                    {"label": "Student Loan", "value": "student_loan"},
                                    {"label": "Other", "value": "other"},
                                ],
                                className="mb-2",
                            ),
                            dbc.Label("Current Value"),
                            dbc.InputGroup(
                                [dbc.InputGroupText("$"), dbc.Input(id="liability-value-input", type="number", placeholder="0.00", min=0, step=0.01)],
                                className="mb-2",
                            ),
                            dbc.Label("Notes (optional)"),
                            dbc.Textarea(id="liability-notes-input", placeholder="Additional details", rows=2),
                            dcc.Store(id="liability-edit-id", data=None),
                        ]
                    ),
                    dbc.ModalFooter(
                        [
                            dbc.Button("Save", id="save-liability-btn", color="success", className="me-2"),
                            dbc.Button("Cancel", id="cancel-liability-btn", color="secondary"),
                        ]
                    ),
                ],
                id="liability-modal",
                is_open=False,
                style={"zIndex": "1200"},
            ),
            dbc.Modal(
                [
                    dbc.ModalHeader("Delete Week Entry"),
                    dbc.ModalBody(id="delete-week-modal-body"),
                    dbc.ModalFooter(
                        [
                            dbc.Button("Delete", id="confirm-delete-week", color="danger", className="me-2"),
                            dbc.Button("Cancel", id="cancel-delete-week", color="secondary"),
                        ]
                    ),
                ],
                id="delete-week-modal",
                is_open=False,
            ),
            dcc.Store(id="delete-weekly-id", data=None),
            dcc.Store(id="weekly-refresh-trigger", data=0),
            dcc.Store(id="weekly-editing-id", data=None),
            dcc.Store(id="networth-refresh-trigger", data=0),
        ],
        fluid=True,
        className="app-page-shell",
    )


@callback(
    Output("weekly-savings-display", "children"),
    Input("weekly-income", "value"),
    Input("weekly-essentials", "value"),
    Input("weekly-discretionary", "value"),
)
def calculate_savings_display(income, essentials, discretionary):
    income = float(income or 0)
    essentials = float(essentials or 0)
    discretionary = float(discretionary or 0)
    savings = income - essentials - discretionary
    text_class = "text-success" if savings > 0 else "text-danger" if savings < 0 else "text-muted"

    return html.Div(
        [
            html.Div("Savings", className="fw-bold"),
            html.Strong(f"${savings:,.2f}", className=f"fs-3 {text_class}"),
        ]
    )


@callback(
    Output("weekly-feedback", "children"),
    Output("weekly-refresh-trigger", "data"),
    Input("weekly-submit", "n_clicks"),
    State("weekly-income", "value"),
    State("weekly-essentials", "value"),
    State("weekly-discretionary", "value"),
    State("weekly-notes", "value"),
    State("weekly-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def submit_week(n_clicks, income, essentials, discretionary, notes, refresh_count):
    if not n_clicks:
        return dash.no_update, refresh_count

    if not is_today_sunday():
        return _weekly_feedback("Weekly reporting is only available on Sundays.", color="danger"), refresh_count

    if income is None or essentials is None or discretionary is None:
        return _weekly_feedback("Please fill in income, essentials, and discretionary values.", color="warning"), refresh_count

    monday, sunday = get_current_week_range()
    monday_iso = monday.isoformat()
    sunday_iso = sunday.isoformat()

    conn = get_connection()
    existing = conn.execute(
        """
        SELECT id FROM weekly_finance
        WHERE week_start_date = ? AND week_end_date = ?
        LIMIT 1
        """,
        (monday_iso, sunday_iso),
    ).fetchone()
    conn.close()

    if existing:
        return _weekly_feedback("This week has already been logged.", color="warning"), refresh_count

    income_f = float(income)
    essentials_f = float(essentials)
    discretionary_f = float(discretionary)
    savings = income_f - essentials_f - discretionary_f

    log_weekly_finance(monday_iso, sunday_iso, income_f, essentials_f, discretionary_f, (notes or "").strip())

    xp = calculate_weekly_xp(savings)
    award_xp(
        sunday_iso,
        "finance",
        "weekly_log",
        xp,
        notes=f"Income: ${income_f:,.2f}, Saved: ${savings:,.2f}",
    )

    message = f"Week logged. Savings: ${savings:,.2f} | XP: +{xp}"
    return _weekly_feedback(message, color="success"), (refresh_count or 0) + 1


@callback(
    Output("previous-weeks-list", "children"),
    Input("weekly-refresh-trigger", "data"),
    State("weekly-editing-id", "data"),
)
def render_previous_weeks(_refresh_count, editing_id):
    return get_previous_weeks_initial(editing_id)


@callback(
    Output("weekly-editing-id", "data"),
    Input({"type": "edit-week", "index": ALL}, "n_clicks"),
    Input({"type": "cancel-week-edit", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def toggle_week_edit_mode(_edit_clicks, _cancel_clicks):
    if not ctx.triggered:
        return dash.no_update

    triggered = ctx.triggered[0]
    if not triggered.get("value"):
        return dash.no_update

    try:
        payload = json.loads(triggered["prop_id"].split(".")[0])
    except Exception:
        return dash.no_update

    if payload.get("type") == "cancel-week-edit":
        return None
    return payload.get("index")


@callback(
    Output("weekly-feedback", "children", allow_duplicate=True),
    Output("weekly-refresh-trigger", "data", allow_duplicate=True),
    Output("weekly-editing-id", "data", allow_duplicate=True),
    Input({"type": "confirm-week-edit", "index": ALL}, "n_clicks"),
    State({"type": "edit-week-income", "index": ALL}, "value"),
    State({"type": "edit-week-essentials", "index": ALL}, "value"),
    State({"type": "edit-week-discretionary", "index": ALL}, "value"),
    State({"type": "edit-week-notes", "index": ALL}, "value"),
    State({"type": "confirm-week-edit", "index": ALL}, "id"),
    State("weekly-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def save_week_edit(_clicks, incomes, essentials_list, discretionary_list, notes_list, button_ids, refresh_count):
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    triggered = ctx.triggered[0]
    if not triggered.get("value"):
        return dash.no_update, dash.no_update, dash.no_update

    try:
        payload = json.loads(triggered["prop_id"].split(".")[0])
        entry_id = payload.get("index")
    except Exception:
        return dash.no_update, dash.no_update, dash.no_update

    idx = next((i for i, item in enumerate(button_ids) if item.get("index") == entry_id), None)
    if idx is None:
        return dash.no_update, dash.no_update, dash.no_update

    income = incomes[idx]
    essentials = essentials_list[idx]
    discretionary = discretionary_list[idx]
    notes = notes_list[idx] or ""
    if income is None or essentials is None or discretionary is None:
        return _weekly_feedback("Income, essentials, and discretionary are required.", color="warning"), refresh_count, entry_id

    income_f = float(income)
    essentials_f = float(essentials)
    discretionary_f = float(discretionary)

    conn = get_connection()
    previous = conn.execute("SELECT week_end_date FROM weekly_finance WHERE id = ?", (entry_id,)).fetchone()
    if not previous:
        conn.close()
        return _weekly_feedback("Entry not found.", color="danger"), refresh_count, None

    week_end_date = previous["week_end_date"]
    xp_row = conn.execute(
        """
        SELECT id, xp_gained
        FROM xp_logs
        WHERE date = ? AND domain = 'finance' AND activity = 'weekly_log'
        ORDER BY id DESC
        LIMIT 1
        """,
        (week_end_date,),
    ).fetchone()
    conn.close()

    update_weekly_finance(entry_id, income_f, essentials_f, discretionary_f, notes.strip())
    savings = income_f - essentials_f - discretionary_f
    new_xp = calculate_weekly_xp(savings)

    conn = get_connection()
    if xp_row:
        delta = new_xp - (xp_row["xp_gained"] or 0)
        conn.execute(
            """
            UPDATE xp_logs
            SET xp_gained = ?, notes = ?
            WHERE id = ?
            """,
            (new_xp, f"Income: ${income_f:,.2f}, Saved: ${savings:,.2f}", xp_row["id"]),
        )
        conn.execute(
            """
            UPDATE identity_levels
            SET xp = MAX(0, xp + ?), updated_at = datetime('now')
            WHERE domain = 'finance'
            """,
            (delta,),
        )
    else:
        conn.execute(
            """
            INSERT INTO xp_logs (date, domain, activity, xp_gained, multiplier, notes)
            VALUES (?, 'finance', 'weekly_log', ?, 1.0, ?)
            """,
            (week_end_date, new_xp, f"Income: ${income_f:,.2f}, Saved: ${savings:,.2f}"),
        )
        conn.execute(
            """
            UPDATE identity_levels
            SET xp = xp + ?, updated_at = datetime('now')
            WHERE domain = 'finance'
            """,
            (new_xp,),
        )
    conn.commit()
    conn.close()

    return _weekly_feedback("Week updated successfully.", color="success"), (refresh_count or 0) + 1, None


@callback(
    Output("delete-week-modal", "is_open"),
    Output("delete-weekly-id", "data"),
    Output("delete-week-modal-body", "children"),
    Input({"type": "delete-week", "index": ALL}, "n_clicks"),
    Input("cancel-delete-week", "n_clicks"),
    State("delete-week-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_delete_modal(_delete_clicks, _cancel_click, is_open):
    if not ctx.triggered:
        return is_open, dash.no_update, dash.no_update

    triggered = ctx.triggered[0]
    prop_id = triggered.get("prop_id", "")
    value = triggered.get("value")
    if not value:
        return is_open, dash.no_update, dash.no_update

    if prop_id.startswith("cancel-delete-week"):
        return False, None, dash.no_update

    try:
        payload = json.loads(prop_id.split(".")[0])
        entry_id = payload.get("index")
    except Exception:
        return is_open, dash.no_update, dash.no_update

    conn = get_connection()
    entry = conn.execute("SELECT * FROM weekly_finance WHERE id = ?", (entry_id,)).fetchone()
    conn.close()
    if not entry:
        return False, None, html.P("Entry not found.")

    entry_dict = dict(entry)
    body = html.Div(
        [
            html.P(f"Are you sure you want to delete the week of {format_week_range(entry_dict['week_start_date'], entry_dict['week_end_date'])}?"),
            html.Small(
                f"Income: ${entry_dict['income']:,.2f} | Essentials: ${entry_dict['essentials']:,.2f} | "
                f"Discretionary: ${entry_dict['discretionary']:,.2f} | Savings: ${entry_dict['savings']:,.2f}",
                className="text-muted",
            ),
        ]
    )
    return True, entry_id, body


@callback(
    Output("weekly-feedback", "children", allow_duplicate=True),
    Output("weekly-refresh-trigger", "data", allow_duplicate=True),
    Output("delete-week-modal", "is_open", allow_duplicate=True),
    Output("delete-weekly-id", "data", allow_duplicate=True),
    Output("weekly-editing-id", "data", allow_duplicate=True),
    Input("confirm-delete-week", "n_clicks"),
    State("delete-weekly-id", "data"),
    State("weekly-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def confirm_delete_week(n_clicks, entry_id, refresh_count):
    if not n_clicks or not entry_id:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    delete_weekly_finance(int(entry_id))
    return (
        _weekly_feedback("Weekly entry deleted. Finance XP recalculated.", color="warning"),
        (refresh_count or 0) + 1,
        False,
        None,
        None,
    )


@callback(
    Output("assets-list", "children"),
    Input("networth-refresh-trigger", "data"),
)
def display_assets(_trigger):
    assets = get_all_assets()
    if not assets:
        return html.P("No assets added yet", className="text-muted small")

    items = []
    for asset in assets:
        items.append(
            html.Div(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Div(
                                        [
                                            html.Strong(asset["name"], className="small"),
                                            html.Br(),
                                            html.Small(asset["category"].replace("_", " ").title(), className="text-muted"),
                                        ]
                                    )
                                ],
                                width=6,
                            ),
                            dbc.Col([html.Strong(f"${asset['current_value']:,.2f}", className="small")], width=4),
                            dbc.Col(
                                [
                                    dbc.ButtonGroup(
                                        [
                                            dbc.Button("Edit", id={"type": "edit-asset", "index": asset["id"]}, color="link", size="sm", className="p-0 text-warning"),
                                            dbc.Button("X", id={"type": "delete-asset", "index": asset["id"]}, color="link", size="sm", className="p-0 text-danger"),
                                        ],
                                        size="sm",
                                    )
                                ],
                                width=2,
                                className="text-end",
                            ),
                        ],
                        className="align-items-center",
                    )
                ],
                className="mb-2 pb-2 border-bottom",
            )
        )
    return items


@callback(
    Output("liabilities-list", "children"),
    Input("networth-refresh-trigger", "data"),
)
def display_liabilities(_trigger):
    liabilities = get_all_liabilities()
    if not liabilities:
        return html.P("No liabilities added yet", className="text-muted small")

    items = []
    for liability in liabilities:
        items.append(
            html.Div(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Div(
                                        [
                                            html.Strong(liability["name"], className="small"),
                                            html.Br(),
                                            html.Small(liability["category"].replace("_", " ").title(), className="text-muted"),
                                        ]
                                    )
                                ],
                                width=6,
                            ),
                            dbc.Col([html.Strong(f"${liability['current_value']:,.2f}", className="small")], width=4),
                            dbc.Col(
                                [
                                    dbc.ButtonGroup(
                                        [
                                            dbc.Button("Edit", id={"type": "edit-liability", "index": liability["id"]}, color="link", size="sm", className="p-0 text-warning"),
                                            dbc.Button("X", id={"type": "delete-liability", "index": liability["id"]}, color="link", size="sm", className="p-0 text-danger"),
                                        ],
                                        size="sm",
                                    )
                                ],
                                width=2,
                                className="text-end",
                            ),
                        ],
                        className="align-items-center",
                    )
                ],
                className="mb-2 pb-2 border-bottom",
            )
        )
    return items


@callback(
    Output("asset-modal", "is_open"),
    Output("asset-name-input", "value"),
    Output("asset-category-input", "value"),
    Output("asset-value-input", "value"),
    Output("asset-notes-input", "value"),
    Output("asset-edit-id", "data"),
    Output("networth-refresh-trigger", "data", allow_duplicate=True),
    Input("add-asset-btn", "n_clicks"),
    Input({"type": "edit-asset", "index": ALL}, "n_clicks"),
    Input("save-asset-btn", "n_clicks"),
    Input("cancel-asset-btn", "n_clicks"),
    State("asset-name-input", "value"),
    State("asset-category-input", "value"),
    State("asset-value-input", "value"),
    State("asset-notes-input", "value"),
    State("asset-edit-id", "data"),
    State("networth-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def manage_asset_modal(_add_click, _edit_clicks, _save_click, _cancel_click, name, category, value, notes, edit_id, refresh_count):
    current = refresh_count or 0
    if not ctx.triggered:
        return False, "", None, None, "", None, current

    triggered = ctx.triggered[0]
    prop_id = triggered.get("prop_id", "")
    value_clicked = triggered.get("value")
    if not value_clicked:
        return False, "", None, None, "", None, current

    if prop_id.startswith("cancel-asset-btn"):
        return False, "", None, None, "", None, current

    if prop_id.startswith("save-asset-btn"):
        if name and category and value is not None:
            today_str = date.today().isoformat()
            if edit_id is not None:
                update_asset(edit_id, name, category, float(value), today_str, notes or "")
            else:
                add_asset(name, category, float(value), today_str, notes or "")
            return False, "", None, None, "", None, current + 1
        return False, "", None, None, "", None, current

    if prop_id.startswith("add-asset-btn"):
        return True, "", None, None, "", None, current

    if "edit-asset" in prop_id:
        try:
            button_data = json.loads(prop_id.split(".")[0])
            asset_id = button_data.get("index")
        except Exception:
            return False, "", None, None, "", None, current

        assets = get_all_assets()
        asset = next((a for a in assets if a["id"] == asset_id), None)
        if asset:
            return True, asset["name"], asset["category"], asset["current_value"], asset["notes"], asset["id"], current

    return False, "", None, None, "", None, current


@callback(
    Output("liability-modal", "is_open"),
    Output("liability-name-input", "value"),
    Output("liability-category-input", "value"),
    Output("liability-value-input", "value"),
    Output("liability-notes-input", "value"),
    Output("liability-edit-id", "data"),
    Output("networth-refresh-trigger", "data", allow_duplicate=True),
    Input("add-liability-btn", "n_clicks"),
    Input({"type": "edit-liability", "index": ALL}, "n_clicks"),
    Input("save-liability-btn", "n_clicks"),
    Input("cancel-liability-btn", "n_clicks"),
    State("liability-name-input", "value"),
    State("liability-category-input", "value"),
    State("liability-value-input", "value"),
    State("liability-notes-input", "value"),
    State("liability-edit-id", "data"),
    State("networth-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def manage_liability_modal(_add_click, _edit_clicks, _save_click, _cancel_click, name, category, value, notes, edit_id, refresh_count):
    current = refresh_count or 0
    if not ctx.triggered:
        return False, "", None, None, "", None, current

    triggered = ctx.triggered[0]
    prop_id = triggered.get("prop_id", "")
    value_clicked = triggered.get("value")
    if not value_clicked:
        return False, "", None, None, "", None, current

    if prop_id.startswith("cancel-liability-btn"):
        return False, "", None, None, "", None, current

    if prop_id.startswith("save-liability-btn"):
        if name and category and value is not None:
            today_str = date.today().isoformat()
            if edit_id is not None:
                update_liability(edit_id, name, category, float(value), today_str, notes or "")
            else:
                add_liability(name, category, float(value), today_str, notes or "")
            return False, "", None, None, "", None, current + 1
        return False, "", None, None, "", None, current

    if prop_id.startswith("add-liability-btn"):
        return True, "", None, None, "", None, current

    if "edit-liability" in prop_id:
        try:
            button_data = json.loads(prop_id.split(".")[0])
            liability_id = button_data.get("index")
        except Exception:
            return False, "", None, None, "", None, current

        liabilities = get_all_liabilities()
        liability = next((l for l in liabilities if l["id"] == liability_id), None)
        if liability:
            return True, liability["name"], liability["category"], liability["current_value"], liability["notes"], liability["id"], current

    return False, "", None, None, "", None, current


@callback(
    Output("networth-refresh-trigger", "data", allow_duplicate=True),
    Input({"type": "delete-asset", "index": ALL}, "n_clicks"),
    Input({"type": "delete-liability", "index": ALL}, "n_clicks"),
    State("networth-refresh-trigger", "data"),
    prevent_initial_call=True,
)
def refresh_networth(_delete_asset_clicks, _delete_liability_clicks, current):
    if not ctx.triggered:
        return current

    triggered = ctx.triggered[0]
    prop_id = triggered.get("prop_id", "")
    value = triggered.get("value")
    if not value:
        return current

    if "delete-asset" in prop_id:
        try:
            button_data = json.loads(prop_id.split(".")[0])
            delete_asset(button_data["index"])
        except Exception:
            pass
    elif "delete-liability" in prop_id:
        try:
            button_data = json.loads(prop_id.split(".")[0])
            delete_liability(button_data["index"])
        except Exception:
            pass

    return (current or 0) + 1


@callback(
    Output("finance-summary-row", "children"),
    Output("finance-networth-summary", "children"),
    Input("weekly-refresh-trigger", "data"),
    Input("networth-refresh-trigger", "data"),
)
def refresh_finance_metric_cards(_weekly_refresh, _networth_refresh):
    return render_finance_summary_cards(), render_networth_summary()
