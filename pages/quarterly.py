"""Quarterly execution cycle dashboard."""
import dash
from dash import ALL, Input, Output, State, callback, ctx, dcc, html
import dash_bootstrap_components as dbc

from database.quarterly import (
    add_cycle_goal,
    add_milestone,
    archive_cycle_goal,
    delete_milestone,
    get_cycle_snapshot,
    get_cycles_history,
    get_or_create_current_cycle,
    log_counter_delta,
    log_measured_value,
    log_recurring_value,
    move_goal,
    move_milestone,
    rename_milestone,
    save_cycle_note,
    set_counter_value,
    set_cycle_summary_note,
    toggle_milestone_completion,
)

dash.register_page(__name__)


def _phase_badge(phase: str):
    if phase == "execution":
        return dbc.Badge("Execution", color="primary", className="quarterly-phase-badge")
    if phase == "review":
        return dbc.Badge("Review", color="warning", text_color="dark", className="quarterly-phase-badge")
    return dbc.Badge("Complete", color="secondary", className="quarterly-phase-badge")


def _segmented_progress(cycle: dict):
    phase = cycle["phase"]
    cycle_pct = cycle["cycle_pct"]
    total_weeks = int(cycle.get("total_weeks") or 1)
    current_week = int(cycle.get("current_week") or 1)
    execution_span_pct = float(cycle.get("execution_span_pct") or 0.0)
    show_review_tint = phase in ("review", "complete")
    review_bg = "#f4f1e7" if show_review_tint else "#eef2f7"
    execution_bg = "#e7edf8"

    if phase == "execution":
        phase_text = f"Week {current_week} of {total_weeks}"
    elif phase == "review":
        phase_text = f"Review phase (Week {current_week} of {total_weeks})"
    else:
        phase_text = f"Completed (Week {total_weeks} of {total_weeks})"

    return html.Div(
        [
            html.Div(
                [
                    html.Div(className="quarterly-bar-execution", style={"width": f"{execution_span_pct:.2f}%", "background": execution_bg}),
                    html.Div(className="quarterly-bar-boundary", style={"left": f"{execution_span_pct:.2f}%"}),
                    html.Div(className="quarterly-bar-review", style={"width": f"{max(0.0, 100.0 - execution_span_pct):.2f}%", "background": review_bg}),
                    html.Div(className="quarterly-bar-fill", style={"width": f"{cycle_pct:.2f}%"}),
                ],
                className="quarterly-segmented-bar",
            ),
            html.Div(
                [
                    html.Span(f"{cycle_pct:.1f}% through cycle", className="quarterly-kpi-text"),
                    html.Span(phase_text, className="quarterly-muted"),
                ],
                className="quarterly-progress-caption",
            ),
        ]
    )


def _goal_status_badge(status: str):
    color_map = {"active": "primary", "complete": "success", "paused": "warning", "dropped": "secondary"}
    return dbc.Badge(status.title(), color=color_map.get(status, "secondary"), className="ms-2")


def _counter_card(goal: dict, can_progress: bool):
    events = goal["derived"].get("events", [])
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.H5(goal["title"], className="mb-1"),
                                html.Div([dbc.Badge("Counter", color="light", text_color="dark", className="border"), _goal_status_badge(goal["status"])]),
                            ]
                        ),
                        html.Div(
                            [
                                html.Span(
                                    f"{float(goal.get('current_value') or 0):g} / {float(goal.get('target_value') or 0):g}",
                                    className="quarterly-big-number",
                                ),
                                html.Small(goal.get("unit") or "", className="quarterly-muted ms-2"),
                            ],
                            className="text-end",
                        ),
                    ],
                    className="d-flex justify-content-between align-items-start",
                ),
                html.P(goal.get("description") or "No description provided.", className="quarterly-muted mb-2"),
                dbc.Progress(value=goal["derived"]["progress_pct"], className="mb-3", style={"height": "10px"}),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.ButtonGroup(
                                [
                                    dbc.Button("-1", id={"type": "quarterly-counter-minus", "goal_id": goal["id"]}, color="secondary", size="sm", disabled=not can_progress),
                                    dbc.Button("+1", id={"type": "quarterly-counter-plus", "goal_id": goal["id"]}, color="primary", size="sm", disabled=not can_progress),
                                ]
                            ),
                            md=4,
                        ),
                        dbc.Col(
                            dbc.InputGroup(
                                [
                                    dbc.Input(id={"type": "quarterly-counter-set-input", "goal_id": goal["id"]}, type="number", value=goal.get("current_value") or 0),
                                    dbc.Button("Set", id={"type": "quarterly-counter-set-btn", "goal_id": goal["id"]}, color="dark", disabled=not can_progress),
                                ],
                                size="sm",
                            ),
                            md=8,
                        ),
                    ],
                    className="g-2 mb-2",
                ),
                html.Div(
                    [
                        html.Small("Recent updates", className="quarterly-muted d-block mb-1"),
                        html.Ul([html.Li(f"{e['date']}: {e['delta']:+g} -> {e['new_value']:g}") for e in events[:4]] if events else [html.Li("No updates yet.")], className="quarterly-history-list"),
                    ]
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _binary_card(goal: dict, can_progress: bool):
    today_status = goal["derived"].get("today_status")
    today_text = "Compliant" if today_status == 1 else "Failed" if today_status == 0 else "Not logged"
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div([html.H5(goal["title"], className="mb-1"), dbc.Badge("Binary Recurring", color="light", text_color="dark", className="border")]),
                        html.Div(_goal_status_badge(goal["status"])),
                    ],
                    className="d-flex justify-content-between align-items-start",
                ),
                html.P(goal.get("description") or "Daily compliance goal.", className="quarterly-muted mb-2"),
                dbc.Row(
                    [
                        dbc.Col(html.Div([html.Small("Today", className="quarterly-muted d-block"), html.Strong(today_text)]), md=3),
                        dbc.Col(html.Div([html.Small("Current Streak", className="quarterly-muted d-block"), html.Strong(f"{goal['derived']['current_streak']} days")]), md=3),
                        dbc.Col(html.Div([html.Small("Week Compliance", className="quarterly-muted d-block"), html.Strong(f"{goal['derived']['week_compliance_pct']:.1f}%")]), md=3),
                        dbc.Col(html.Div([html.Small("Cycle Compliance", className="quarterly-muted d-block"), html.Strong(f"{goal['derived']['cycle_compliance_pct']:.1f}%")]), md=3),
                    ],
                    className="g-2 mb-2",
                ),
                dbc.ButtonGroup(
                    [
                        dbc.Button("Mark Today Compliant", id={"type": "quarterly-binary-yes", "goal_id": goal["id"]}, color="success", size="sm", disabled=not can_progress),
                        dbc.Button("Mark Today Failed", id={"type": "quarterly-binary-no", "goal_id": goal["id"]}, color="danger", size="sm", outline=True, disabled=not can_progress),
                    ]
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _milestone_row(goal_id: int, milestone: dict, can_progress: bool, can_structural: bool):
    return dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(dbc.Checkbox(id={"type": "quarterly-milestone-toggle", "goal_id": goal_id, "milestone_id": milestone["id"]}, value=bool(milestone["is_completed"]), label=milestone["title"], disabled=not can_progress), md=5),
                        dbc.Col(
                            dbc.InputGroup(
                                [
                                    dbc.Input(id={"type": "quarterly-milestone-title-input", "goal_id": goal_id, "milestone_id": milestone["id"]}, value=milestone["title"], disabled=not can_structural),
                                    dbc.Button("Rename", id={"type": "quarterly-milestone-rename-btn", "goal_id": goal_id, "milestone_id": milestone["id"]}, color="secondary", size="sm", disabled=not can_structural),
                                ],
                                size="sm",
                            ),
                            md=5,
                        ),
                        dbc.Col(
                            dbc.ButtonGroup(
                                [
                                    dbc.Button("Up", id={"type": "quarterly-milestone-up", "goal_id": goal_id, "milestone_id": milestone["id"]}, color="light", size="sm", disabled=not can_structural),
                                    dbc.Button("Down", id={"type": "quarterly-milestone-down", "goal_id": goal_id, "milestone_id": milestone["id"]}, color="light", size="sm", disabled=not can_structural),
                                    dbc.Button("Delete", id={"type": "quarterly-milestone-delete", "goal_id": goal_id, "milestone_id": milestone["id"]}, color="danger", size="sm", outline=True, disabled=not can_structural),
                                ],
                                size="sm",
                            ),
                            md=2,
                            className="text-end",
                        ),
                    ],
                    className="g-2",
                )
            ]
        ),
        className="quarterly-subcard mb-2",
    )


def _milestone_card(goal: dict, can_progress: bool, can_structural: bool):
    milestones = goal["derived"].get("milestones", [])
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div([html.H5(goal["title"], className="mb-1"), dbc.Badge("Milestone", color="light", text_color="dark", className="border")]),
                        html.Div(_goal_status_badge(goal["status"])),
                    ],
                    className="d-flex justify-content-between align-items-start",
                ),
                html.P(goal.get("description") or "Project-style milestone progression.", className="quarterly-muted mb-2"),
                html.Div(f"{goal['derived']['completed']} / {goal['derived']['total']} milestones completed", className="quarterly-muted mb-1"),
                dbc.Progress(value=goal["derived"]["progress_pct"], className="mb-3", style={"height": "10px"}),
                html.Div([_milestone_row(goal["id"], m, can_progress, can_structural) for m in milestones]),
                dbc.InputGroup(
                    [
                        dbc.Input(id={"type": "quarterly-add-milestone-input", "goal_id": goal["id"]}, placeholder="Add milestone"),
                        dbc.Button("Add", id={"type": "quarterly-add-milestone-btn", "goal_id": goal["id"]}, color="dark", disabled=not can_structural),
                    ],
                    size="sm",
                ),
                html.Small("Milestone structure is locked during execution." if not can_structural else "Review phase: structure editing enabled.", className="quarterly-muted mt-2 d-block"),
            ]
        ),
        className="quarterly-goal-card",
    )


def _measured_card(goal: dict, can_progress: bool):
    d = goal["derived"]
    entries = d.get("entries", [])
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div([html.H5(goal["title"], className="mb-1"), dbc.Badge("Measured", color="light", text_color="dark", className="border")]),
                        html.Div(_goal_status_badge(goal["status"])),
                    ],
                    className="d-flex justify-content-between align-items-start",
                ),
                html.P(goal.get("description") or "Measured value tracked over time.", className="quarterly-muted mb-2"),
                dbc.Row(
                    [
                        dbc.Col(html.Div([html.Small("Baseline", className="quarterly-muted d-block"), html.Strong(f"{goal.get('baseline_value') or '--'} {goal.get('unit') or ''}")]), md=3),
                        dbc.Col(html.Div([html.Small("Current", className="quarterly-muted d-block"), html.Strong(f"{d.get('current') if d.get('current') is not None else '--'} {goal.get('unit') or ''}")]), md=3),
                        dbc.Col(html.Div([html.Small("Target", className="quarterly-muted d-block"), html.Strong(f"{goal.get('target_value') or '--'} {goal.get('unit') or ''}")]), md=3),
                        dbc.Col(html.Div([html.Small("Net Change", className="quarterly-muted d-block"), html.Strong(f"{d.get('net_change') if d.get('net_change') is not None else '--'} {goal.get('unit') or ''}")]), md=3),
                    ],
                    className="g-2 mb-2",
                ),
                dbc.Progress(value=d.get("progress_pct", 0), className="mb-3", style={"height": "10px"}),
                dbc.InputGroup(
                    [
                        dbc.Input(id={"type": "quarterly-measured-input", "goal_id": goal["id"]}, type="number", step=0.01, placeholder="Log value"),
                        dbc.Button("Log", id={"type": "quarterly-measured-log-btn", "goal_id": goal["id"]}, color="dark", disabled=not can_progress),
                    ],
                    size="sm",
                    className="mb-2",
                ),
                html.Small("Recent entries: " + (", ".join(f"{e['date']} ({e['value']})" for e in entries[:3]) if entries else "none"), className="quarterly-muted"),
            ]
        ),
        className="quarterly-goal-card",
    )


def _render_goal_card(goal: dict, cycle: dict):
    can_progress = bool(cycle["can_progress_edit"])
    can_structural = bool(cycle["can_structural_edit"])
    if goal["is_archived"]:
        return dbc.Card(dbc.CardBody([html.H6(goal["title"], className="mb-1"), html.Small("Archived goal", className="quarterly-muted")]), className="quarterly-goal-card quarterly-archived-goal")

    if goal["goal_type"] == "counter":
        goal_body = _counter_card(goal, can_progress)
    elif goal["goal_type"] == "binary_recurring":
        goal_body = _binary_card(goal, can_progress)
    elif goal["goal_type"] == "milestone":
        goal_body = _milestone_card(goal, can_progress, can_structural)
    else:
        goal_body = _measured_card(goal, can_progress)

    controls = dbc.ButtonGroup(
        [
            dbc.Button("Up", id={"type": "quarterly-goal-move-up", "goal_id": goal["id"]}, color="light", size="sm", disabled=not can_structural),
            dbc.Button("Down", id={"type": "quarterly-goal-move-down", "goal_id": goal["id"]}, color="light", size="sm", disabled=not can_structural),
            dbc.Button("Archive", id={"type": "quarterly-goal-archive", "goal_id": goal["id"]}, color="danger", size="sm", outline=True, disabled=not can_structural),
        ],
        size="sm",
        className="mb-2",
    )
    return html.Div([controls, goal_body], className="mb-3")


def _add_goal_panel(cycle: dict):
    disabled = not cycle["can_structural_edit"]
    return dbc.Card(
        dbc.CardBody(
            [
                html.H5("Add Goal", className="mb-2"),
                html.Small("Available in review phase." if disabled else "Define a new goal for this quarter.", className="quarterly-muted d-block mb-2"),
                dbc.Row(
                    [
                        dbc.Col(dbc.Input(id="quarterly-add-title", placeholder="Goal title"), md=6),
                        dbc.Col(
                            dcc.Dropdown(
                                id="quarterly-add-type",
                                options=[
                                    {"label": "Counter", "value": "counter"},
                                    {"label": "Binary recurring", "value": "binary_recurring"},
                                    {"label": "Milestone", "value": "milestone"},
                                    {"label": "Measured", "value": "measured"},
                                ],
                                value="counter",
                                clearable=False,
                            ),
                            md=6,
                        ),
                    ],
                    className="g-2 mb-2",
                ),
                dbc.Textarea(id="quarterly-add-description", placeholder="Description", className="mb-2"),
                dbc.Row(
                    [
                        dbc.Col(dbc.Input(id="quarterly-add-target", type="number", placeholder="Target value"), md=3),
                        dbc.Col(dbc.Input(id="quarterly-add-current", type="number", placeholder="Current value"), md=3),
                        dbc.Col(dbc.Input(id="quarterly-add-baseline", type="number", placeholder="Baseline (measured)"), md=3),
                        dbc.Col(dbc.Input(id="quarterly-add-unit", placeholder="Unit"), md=3),
                    ],
                    className="g-2 mb-2",
                ),
                dbc.Row(
                    [
                        dbc.Col(dcc.Dropdown(id="quarterly-add-direction", options=[{"label": "Increase", "value": "increase"}, {"label": "Decrease", "value": "decrease"}], value="increase", clearable=False), md=4),
                        dbc.Col(dbc.Input(id="quarterly-add-category", placeholder="Category"), md=4),
                        dbc.Col(dbc.Input(id="quarterly-add-notes", placeholder="Notes"), md=4),
                    ],
                    className="g-2 mb-2",
                ),
                dbc.Textarea(id="quarterly-add-milestones", placeholder="Milestones (one per line, for milestone goals)", className="mb-2", style={"minHeight": "90px"}),
                dbc.Button("Save Goal", id="quarterly-add-goal-btn", color="dark", disabled=disabled),
            ]
        ),
        className="quarterly-side-card",
    )


def _render_cycle_content(cycle_id: int):
    snapshot = get_cycle_snapshot(cycle_id)
    cycle, goals, notes = snapshot["cycle"], snapshot["goals"], snapshot["notes"]
    return html.Div(
        [
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Div(
                            [
                                html.Div([html.H3(cycle["title"], className="mb-1"), html.Small(f"{cycle['start_date']} to {cycle['end_date']}", className="quarterly-muted d-block")]),
                                _phase_badge(cycle["phase"]),
                            ],
                            className="d-flex justify-content-between align-items-start mb-2",
                        ),
                        _segmented_progress(cycle),
                        dbc.Textarea(id="quarterly-summary-note", value=cycle.get("summary_note") or "", placeholder="Quarter purpose / summary", className="mt-3", style={"minHeight": "72px"}, disabled=not cycle["can_structural_edit"]),
                        dbc.Button("Save Purpose Note", id="quarterly-save-summary-note-btn", color="secondary", size="sm", className="mt-2", disabled=not cycle["can_structural_edit"]),
                    ]
                ),
                className="quarterly-header-card mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div([html.H5("Goals", className="mb-2"), html.Small("Progress updates are allowed in execution. Structural edits unlock in review.", className="quarterly-muted")], className="mb-3"),
                            html.Div([_render_goal_card(g, cycle) for g in goals] if goals else dbc.Alert("No goals yet.", color="light")),
                        ],
                        md=8,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                dbc.CardBody([html.H5("Cycle Context", className="mb-2"), html.P(f"Quarter: {cycle['title']}", className="mb-1"), html.P(f"Phase: {cycle['phase'].title()}", className="mb-1"), html.P(f"Execution ends: {cycle['execution_end_date']}", className="mb-1"), html.P(f"Cycle progress: {cycle['cycle_pct']:.1f}%", className="mb-0")]),
                                className="quarterly-side-card mb-3",
                            ),
                            _add_goal_panel(cycle),
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.H5("Review Notes", className="mb-2"),
                                        dbc.Textarea(id="quarterly-note-input", placeholder="Add review note", className="mb-2"),
                                        dbc.Button("Save Note", id="quarterly-save-note-btn", color="dark", size="sm", className="mb-2"),
                                        html.Hr(),
                                        html.Ul([html.Li(f"{n['created_at']}: {n['content']}") for n in notes[:8]] if notes else [html.Li("No notes yet.")], className="quarterly-history-list"),
                                    ]
                                ),
                                className="quarterly-side-card mt-3",
                            ),
                        ],
                        md=4,
                    ),
                ],
                className="g-3",
            ),
        ]
    )


def layout():
    current = get_or_create_current_cycle()
    cycles = get_cycles_history()
    options = [{"label": c["title"], "value": c["id"]} for c in cycles]
    return dbc.Container(
        [
            html.Div([html.H2("Execution Cycle", className="mb-1"), html.P("Fixed calendar-quarter planning and execution command center.", className="quarterly-muted mb-0")], className="app-page-head"),
            dbc.Row([dbc.Col(dcc.Dropdown(id="quarterly-cycle-select", options=options, value=current["id"], clearable=False), md=4), dbc.Col(html.Div(id="quarterly-message"), md=8)], className="g-2 mb-3"),
            dcc.Store(id="quarterly-refresh", data=0),
            html.Div(id="quarterly-content", children=_render_cycle_content(current["id"])),
        ],
        fluid=True,
        className="quarterly-page app-page-shell",
    )


@callback(
    Output("quarterly-cycle-select", "options"),
    Input("quarterly-refresh", "data"),
)
def refresh_quarterly_options(_refresh):
    cycles = get_cycles_history()
    return [{"label": c["title"], "value": c["id"]} for c in cycles]


@callback(
    Output("quarterly-content", "children"),
    Input("quarterly-cycle-select", "value"),
    Input("quarterly-refresh", "data"),
)
def refresh_quarterly_content(selected_cycle_id, _refresh):
    selected = selected_cycle_id or get_or_create_current_cycle()["id"]
    return _render_cycle_content(selected)


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input("quarterly-add-goal-btn", "n_clicks"),
    State("quarterly-cycle-select", "value"),
    State("quarterly-add-title", "value"),
    State("quarterly-add-type", "value"),
    State("quarterly-add-description", "value"),
    State("quarterly-add-target", "value"),
    State("quarterly-add-current", "value"),
    State("quarterly-add-baseline", "value"),
    State("quarterly-add-direction", "value"),
    State("quarterly-add-unit", "value"),
    State("quarterly-add-category", "value"),
    State("quarterly-add-notes", "value"),
    State("quarterly-add-milestones", "value"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_add_goal(
    n_clicks,
    cycle_id,
    title,
    goal_type,
    description,
    target,
    current_value,
    baseline,
    direction,
    unit,
    category,
    notes,
    milestones_text,
    refresh_count,
):
    if not n_clicks:
        return dash.no_update, dash.no_update
    try:
        milestones = [line.strip() for line in (milestones_text or "").splitlines() if line.strip()]
        add_cycle_goal(
            cycle_id=int(cycle_id),
            title=title or "",
            goal_type=goal_type or "",
            description=description or "",
            target_value=target,
            current_value=current_value,
            baseline_value=baseline,
            target_direction=direction or "",
            unit=unit or "",
            category=category or "",
            notes=notes or "",
            milestones=milestones,
        )
        return dbc.Alert("Goal added.", color="success", duration=2500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-counter-plus", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-counter-minus", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-counter-set-btn", "goal_id": ALL}, "n_clicks"),
    State({"type": "quarterly-counter-set-input", "goal_id": ALL}, "value"),
    State({"type": "quarterly-counter-set-input", "goal_id": ALL}, "id"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_counter_updates(_plus, _minus, _set, set_values, set_ids, refresh_count):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        goal_id = int(triggered["goal_id"])
        if triggered["type"] == "quarterly-counter-plus":
            log_counter_delta(goal_id, 1)
        elif triggered["type"] == "quarterly-counter-minus":
            log_counter_delta(goal_id, -1)
        elif triggered["type"] == "quarterly-counter-set-btn":
            mapping = {int(item["goal_id"]): value for item, value in zip(set_ids or [], set_values or [])}
            if goal_id not in mapping:
                return dash.no_update, dash.no_update
            set_counter_value(goal_id, float(mapping[goal_id]))
        return dbc.Alert("Counter updated.", color="success", duration=1500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-binary-yes", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-binary-no", "goal_id": ALL}, "n_clicks"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_binary_updates(_yes, _no, refresh_count):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        from database.db import get_brisbane_date

        log_recurring_value(int(triggered["goal_id"]), get_brisbane_date(), triggered["type"] == "quarterly-binary-yes")
        return dbc.Alert("Recurring status saved.", color="success", duration=1500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-milestone-toggle", "goal_id": ALL, "milestone_id": ALL}, "value"),
    State({"type": "quarterly-milestone-toggle", "goal_id": ALL, "milestone_id": ALL}, "id"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_milestone_toggles(values, ids, refresh_count):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        state_map = {
            (int(item["goal_id"]), int(item["milestone_id"])): bool(v)
            for item, v in zip(ids or [], values or [])
        }
        key = (int(triggered["goal_id"]), int(triggered["milestone_id"]))
        if key not in state_map:
            return dash.no_update, dash.no_update
        toggle_milestone_completion(key[0], key[1], state_map[key])
        return dbc.Alert("Milestone updated.", color="success", duration=1200), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-add-milestone-btn", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-milestone-delete", "goal_id": ALL, "milestone_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-milestone-up", "goal_id": ALL, "milestone_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-milestone-down", "goal_id": ALL, "milestone_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-milestone-rename-btn", "goal_id": ALL, "milestone_id": ALL}, "n_clicks"),
    State({"type": "quarterly-add-milestone-input", "goal_id": ALL}, "value"),
    State({"type": "quarterly-add-milestone-input", "goal_id": ALL}, "id"),
    State({"type": "quarterly-milestone-title-input", "goal_id": ALL, "milestone_id": ALL}, "value"),
    State({"type": "quarterly-milestone-title-input", "goal_id": ALL, "milestone_id": ALL}, "id"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_milestone_structure(
    _add,
    _delete,
    _up,
    _down,
    _rename,
    add_values,
    add_ids,
    rename_values,
    rename_ids,
    refresh_count,
):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        t = triggered["type"]
        goal_id = int(triggered["goal_id"])
        if t == "quarterly-add-milestone-btn":
            val_map = {int(i["goal_id"]): v for i, v in zip(add_ids or [], add_values or [])}
            add_milestone(goal_id, val_map.get(goal_id) or "")
        elif t == "quarterly-milestone-delete":
            delete_milestone(goal_id, int(triggered["milestone_id"]))
        elif t == "quarterly-milestone-up":
            move_milestone(goal_id, int(triggered["milestone_id"]), "up")
        elif t == "quarterly-milestone-down":
            move_milestone(goal_id, int(triggered["milestone_id"]), "down")
        elif t == "quarterly-milestone-rename-btn":
            key_map = {(int(i["goal_id"]), int(i["milestone_id"])): v for i, v in zip(rename_ids or [], rename_values or [])}
            rename_milestone(goal_id, int(triggered["milestone_id"]), key_map.get((goal_id, int(triggered["milestone_id"]))) or "")
        return dbc.Alert("Milestone structure updated.", color="success", duration=1200), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-measured-log-btn", "goal_id": ALL}, "n_clicks"),
    State({"type": "quarterly-measured-input", "goal_id": ALL}, "value"),
    State({"type": "quarterly-measured-input", "goal_id": ALL}, "id"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_measured_logs(_log_clicks, values, ids, refresh_count):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        goal_id = int(triggered["goal_id"])
        mapping = {int(i["goal_id"]): v for i, v in zip(ids or [], values or [])}
        if goal_id not in mapping or mapping[goal_id] is None:
            return dbc.Alert("Enter a value before logging.", color="warning", duration=1600), dash.no_update
        log_measured_value(goal_id, float(mapping[goal_id]))
        return dbc.Alert("Measurement logged.", color="success", duration=1500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input({"type": "quarterly-goal-move-up", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-goal-move-down", "goal_id": ALL}, "n_clicks"),
    Input({"type": "quarterly-goal-archive", "goal_id": ALL}, "n_clicks"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_goal_structure(_up, _down, _archive, refresh_count):
    triggered = ctx.triggered_id
    if not triggered:
        return dash.no_update, dash.no_update
    try:
        goal_id = int(triggered["goal_id"])
        if triggered["type"] == "quarterly-goal-move-up":
            move_goal(goal_id, "up")
        elif triggered["type"] == "quarterly-goal-move-down":
            move_goal(goal_id, "down")
        elif triggered["type"] == "quarterly-goal-archive":
            archive_cycle_goal(goal_id)
        return dbc.Alert("Goal structure updated.", color="success", duration=1400), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input("quarterly-save-note-btn", "n_clicks"),
    State("quarterly-cycle-select", "value"),
    State("quarterly-note-input", "value"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_save_note(n_clicks, cycle_id, note, refresh_count):
    if not n_clicks:
        return dash.no_update, dash.no_update
    try:
        save_cycle_note(int(cycle_id), note or "")
        return dbc.Alert("Review note saved.", color="success", duration=1500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update


@callback(
    Output("quarterly-message", "children", allow_duplicate=True),
    Output("quarterly-refresh", "data", allow_duplicate=True),
    Input("quarterly-save-summary-note-btn", "n_clicks"),
    State("quarterly-cycle-select", "value"),
    State("quarterly-summary-note", "value"),
    State("quarterly-refresh", "data"),
    prevent_initial_call=True,
)
def handle_save_summary_note(n_clicks, cycle_id, summary_note, refresh_count):
    if not n_clicks:
        return dash.no_update, dash.no_update
    try:
        set_cycle_summary_note(int(cycle_id), summary_note or "")
        return dbc.Alert("Cycle purpose note updated.", color="success", duration=1500), (refresh_count or 0) + 1
    except Exception as exc:
        return dbc.Alert(str(exc), color="danger"), dash.no_update
