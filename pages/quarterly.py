"""Quarterly execution cycle dashboard."""
from datetime import date, datetime, timedelta

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


GOAL_TYPE_META = {
    "counter": {
        "label": "Counter",
        "summary": "Use this when success is a running total you build across the quarter.",
    },
    "binary_recurring": {
        "label": "Binary Recurring",
        "summary": "Use this for daily yes-or-no compliance, like a habit you either hit or miss.",
    },
    "milestone": {
        "label": "Milestone",
        "summary": "Use this for project-style work where progress happens through clear checkpoints.",
    },
    "measured": {
        "label": "Measured",
        "summary": "Use this when the quarter is about moving a baseline toward a target value.",
    },
}

PHASE_META = {
    "execution": {
        "label": "Execution",
        "summary": "Run the plan, log progress, and keep the scope stable.",
    },
    "review": {
        "label": "Review",
        "summary": "Reshape the quarter, tighten the challenge list, and capture lessons.",
    },
    "complete": {
        "label": "Complete",
        "summary": "This cycle is closed and preserved for reference.",
    },
}


def _format_date(value: str) -> str:
    try:
        return date.fromisoformat(value).strftime("%b %d, %Y")
    except Exception:
        return value or "--"


def _format_timestamp(value: str) -> str:
    if not value:
        return "--"
    try:
        normalized = value.replace(" ", "T") if "T" not in value and " " in value else value
        return datetime.fromisoformat(normalized).strftime("%b %d, %Y %H:%M")
    except Exception:
        return value


def _format_number(value) -> str:
    if value is None or value == "":
        return "--"
    try:
        return f"{float(value):g}"
    except Exception:
        return str(value)


def _goal_type_label(goal_type: str) -> str:
    return GOAL_TYPE_META.get(goal_type, {}).get("label", "Challenge")


def _phase_badge(phase: str):
    meta = PHASE_META.get(phase, PHASE_META["complete"])
    return html.Span(meta["label"], className=f"quarterly-phase-badge quarterly-phase-{phase}")


def _goal_type_badge(goal_type: str):
    return html.Span(_goal_type_label(goal_type), className=f"quarterly-tag quarterly-tag-{goal_type}")


def _goal_status_badge(status: str):
    cleaned = (status or "active").strip().lower()
    return html.Span(cleaned.title(), className=f"quarterly-status quarterly-status-{cleaned}")


def _segmented_progress(cycle: dict):
    phase = cycle["phase"]
    cycle_pct = cycle["cycle_pct"]
    total_weeks = int(cycle.get("total_weeks") or 1)
    current_week = int(cycle.get("current_week") or 1)
    execution_span_pct = float(cycle.get("execution_span_pct") or 0.0)
    show_review_tint = phase in ("review", "complete")
    review_bg = "#f5efe3" if show_review_tint else "#edf2f8"
    execution_bg = "#e4edf9"

    if phase == "execution":
        phase_text = f"Week {current_week} of {total_weeks}"
    elif phase == "review":
        phase_text = f"Review window · Week {current_week} of {total_weeks}"
    else:
        phase_text = f"Completed · Week {total_weeks} of {total_weeks}"

    review_start = _format_date(cycle["execution_end_date"])
    execution_end = _format_date(
        (date.fromisoformat(cycle["execution_end_date"]) - timedelta(days=1)).isoformat()
    )
    quarter_end = _format_date(cycle["end_date"])

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        className="quarterly-bar-execution",
                        style={"width": f"{execution_span_pct:.2f}%", "background": execution_bg},
                    ),
                    html.Div(
                        className="quarterly-bar-boundary",
                        style={"left": f"{execution_span_pct:.2f}%"},
                    ),
                    html.Div(
                        className="quarterly-bar-review",
                        style={
                            "width": f"{max(0.0, 100.0 - execution_span_pct):.2f}%",
                            "background": review_bg,
                        },
                    ),
                    html.Div(
                        className="quarterly-bar-fill",
                        style={"width": f"{cycle_pct:.2f}%"},
                    ),
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
            html.Div(
                [
                    html.Span(f"Execution: through {execution_end}", className="quarterly-progress-legend-item"),
                    html.Span(f"Review: {review_start} to {quarter_end}", className="quarterly-progress-legend-item"),
                ],
                className="quarterly-progress-legend",
            ),
        ]
    )


def _metric_card(label: str, value: str, detail: str, tone: str = "default"):
    return html.Div(
        [
            html.Span(label, className="quarterly-metric-label"),
            html.Strong(value, className="quarterly-metric-value"),
            html.Span(detail, className="quarterly-metric-detail"),
        ],
        className=f"quarterly-metric-card quarterly-metric-card-{tone}",
    )


def _challenge_metric(label: str, value: str, detail: str):
    return html.Div(
        [
            html.Span(label, className="quarterly-inline-stat-label"),
            html.Strong(value, className="quarterly-inline-stat-value"),
            html.Span(detail, className="quarterly-inline-stat-detail"),
        ],
        className="quarterly-inline-stat",
    )


def _goal_progress(value: float, label: str):
    return html.Div(
        [
            html.Div(
                [
                    html.Span(label, className="quarterly-progress-label"),
                    html.Span(f"{value:.0f}%", className="quarterly-progress-value"),
                ],
                className="quarterly-goal-progress-head",
            ),
            dbc.Progress(value=value, className="quarterly-goal-progress-bar"),
        ],
        className="quarterly-goal-progress",
    )


def _goal_actions(goal: dict, can_structural: bool):
    if not can_structural:
        return html.Span("Structure locked", className="quarterly-goal-lock-note")
    return dbc.ButtonGroup(
        [
            dbc.Button(
                "Up",
                id={"type": "quarterly-goal-move-up", "goal_id": goal["id"]},
                color="light",
                size="sm",
            ),
            dbc.Button(
                "Down",
                id={"type": "quarterly-goal-move-down", "goal_id": goal["id"]},
                color="light",
                size="sm",
            ),
            dbc.Button(
                "Archive",
                id={"type": "quarterly-goal-archive", "goal_id": goal["id"]},
                color="danger",
                size="sm",
                outline=True,
            ),
        ],
        size="sm",
        className="quarterly-goal-actions",
    )


def _goal_header(goal: dict, can_structural: bool):
    tags = [_goal_type_badge(goal["goal_type"]), _goal_status_badge(goal["status"])]
    if goal.get("category"):
        tags.append(html.Span(goal["category"], className="quarterly-tag quarterly-tag-muted"))

    return html.Div(
        [
            html.Div(
                [
                    html.Div(tags, className="quarterly-goal-tags"),
                    html.H4(goal["title"], className="quarterly-goal-title"),
                ],
                className="quarterly-goal-headline",
            ),
            _goal_actions(goal, can_structural),
        ],
        className="quarterly-goal-header",
    )


def _goal_description(goal: dict, fallback: str):
    body = [html.P(goal.get("description") or fallback, className="quarterly-goal-description")]
    if goal.get("notes"):
        body.append(
            html.Div(
                [html.Span("Setup note", className="quarterly-goal-note-label"), html.Span(goal["notes"])],
                className="quarterly-goal-note",
            )
        )
    return body


def _history_rows(items, empty_text: str):
    if not items:
        return html.Div(empty_text, className="quarterly-empty-log")
    return html.Div(items, className="quarterly-history-stack")


def _counter_card(goal: dict, can_progress: bool, can_structural: bool):
    events = goal["derived"].get("events", [])
    target = float(goal.get("target_value") or 0.0)
    current = float(goal.get("current_value") or 0.0)
    remaining = max(target - current, 0.0)

    return dbc.Card(
        dbc.CardBody(
            [
                _goal_header(goal, can_structural),
                *_goal_description(goal, "Track a running total across the quarter."),
                html.Div(
                    [
                        _challenge_metric("Current", _format_number(current), goal.get("unit") or "Logged total"),
                        _challenge_metric("Target", _format_number(target), goal.get("unit") or "Quarter target"),
                        _challenge_metric("Remaining", _format_number(remaining), goal.get("unit") or "Left to finish"),
                    ],
                    className="quarterly-inline-stats",
                ),
                _goal_progress(
                    goal["derived"]["progress_pct"],
                    f"{_format_number(current)} / {_format_number(target)} {goal.get('unit') or ''}".strip(),
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        "-1",
                                        id={"type": "quarterly-counter-minus", "goal_id": goal["id"]},
                                        color="secondary",
                                        size="sm",
                                        disabled=not can_progress,
                                    ),
                                    dbc.Button(
                                        "+1",
                                        id={"type": "quarterly-counter-plus", "goal_id": goal["id"]},
                                        color="primary",
                                        size="sm",
                                        disabled=not can_progress,
                                    ),
                                ]
                            ),
                            md=4,
                        ),
                        dbc.Col(
                            dbc.InputGroup(
                                [
                                    dbc.Input(
                                        id={"type": "quarterly-counter-set-input", "goal_id": goal["id"]},
                                        type="number",
                                        value=goal.get("current_value") or 0,
                                        disabled=not can_progress,
                                    ),
                                    dbc.Button(
                                        "Set",
                                        id={"type": "quarterly-counter-set-btn", "goal_id": goal["id"]},
                                        color="dark",
                                        disabled=not can_progress,
                                    ),
                                ],
                                size="sm",
                            ),
                            md=8,
                        ),
                    ],
                    className="g-2 mb-3",
                ),
                html.Div(
                    [
                        html.Div("Recent updates", className="quarterly-subsection-label"),
                        _history_rows(
                            [
                                html.Div(
                                    [
                                        html.Span(_format_date(event["date"]), className="quarterly-history-date"),
                                        html.Span(f"{event['delta']:+g} -> {event['new_value']:g}"),
                                    ],
                                    className="quarterly-history-row",
                                )
                                for event in events[:4]
                            ],
                            "No updates yet.",
                        ),
                    ]
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _binary_card(goal: dict, can_progress: bool, can_structural: bool):
    today_status = goal["derived"].get("today_status")
    today_text = "Compliant" if today_status == 1 else "Failed" if today_status == 0 else "Not logged"

    return dbc.Card(
        dbc.CardBody(
            [
                _goal_header(goal, can_structural),
                *_goal_description(goal, "Track whether you kept the commitment today."),
                html.Div(
                    [
                        _challenge_metric("Today", today_text, "Latest daily result"),
                        _challenge_metric(
                            "Streak",
                            f"{goal['derived']['current_streak']} days",
                            "Current consecutive wins",
                        ),
                        _challenge_metric(
                            "Week",
                            f"{goal['derived']['week_compliance_pct']:.1f}%",
                            "Last 7 days",
                        ),
                        _challenge_metric(
                            "Cycle",
                            f"{goal['derived']['cycle_compliance_pct']:.1f}%",
                            "Quarter-to-date compliance",
                        ),
                    ],
                    className="quarterly-inline-stats",
                ),
                _goal_progress(goal["derived"]["cycle_compliance_pct"], "Cycle compliance"),
                dbc.ButtonGroup(
                    [
                        dbc.Button(
                            "Mark Today Compliant",
                            id={"type": "quarterly-binary-yes", "goal_id": goal["id"]},
                            color="success",
                            size="sm",
                            disabled=not can_progress,
                        ),
                        dbc.Button(
                            "Mark Today Failed",
                            id={"type": "quarterly-binary-no", "goal_id": goal["id"]},
                            color="danger",
                            size="sm",
                            outline=True,
                            disabled=not can_progress,
                        ),
                    ]
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _milestone_row(goal_id: int, milestone: dict, can_progress: bool, can_structural: bool):
    is_completed = bool(milestone["is_completed"])
    detail = (
        f"Completed {_format_timestamp(milestone['completed_at'])}"
        if is_completed and milestone.get("completed_at")
        else "Checked off"
        if is_completed
        else "Open milestone"
    )

    if not can_structural:
        return html.Div(
            [
                html.Div(
                    [
                        dbc.Checkbox(
                            id={
                                "type": "quarterly-milestone-toggle",
                                "goal_id": goal_id,
                                "milestone_id": milestone["id"],
                            },
                            value=is_completed,
                            disabled=not can_progress,
                            className="mt-1",
                        ),
                        html.Div(
                            [
                                html.Div(milestone["title"], className="quarterly-milestone-title"),
                                html.Small(detail, className="quarterly-muted"),
                            ],
                            className="flex-grow-1",
                        ),
                        html.Span(
                            "Done" if is_completed else "Open",
                            className=f"quarterly-mode-pill {'is-open' if is_completed else 'is-locked'}",
                        ),
                    ],
                    className="d-flex gap-2 align-items-start",
                )
            ],
            className=f"quarterly-subcard quarterly-milestone-row {'is-complete' if is_completed else ''}",
        )

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            [
                                dbc.Checkbox(
                                    id={
                                        "type": "quarterly-milestone-toggle",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    value=is_completed,
                                    disabled=not can_progress,
                                    className="mt-1",
                                ),
                                html.Div(
                                    [
                                        html.Div(milestone["title"], className="quarterly-milestone-title"),
                                        html.Small(detail, className="quarterly-muted"),
                                    ],
                                    className="flex-grow-1",
                                ),
                            ],
                            className="d-flex gap-2 align-items-start",
                        ),
                        md=5,
                    ),
                    dbc.Col(
                        dbc.InputGroup(
                            [
                                dbc.Input(
                                    id={
                                        "type": "quarterly-milestone-title-input",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    value=milestone["title"],
                                    disabled=not can_structural,
                                ),
                                dbc.Button(
                                    "Rename",
                                    id={
                                        "type": "quarterly-milestone-rename-btn",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    color="secondary",
                                    size="sm",
                                ),
                            ],
                            size="sm",
                        ),
                        md=5,
                    ),
                    dbc.Col(
                        dbc.ButtonGroup(
                            [
                                dbc.Button(
                                    "Up",
                                    id={
                                        "type": "quarterly-milestone-up",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    color="light",
                                    size="sm",
                                ),
                                dbc.Button(
                                    "Down",
                                    id={
                                        "type": "quarterly-milestone-down",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    color="light",
                                    size="sm",
                                ),
                                dbc.Button(
                                    "Delete",
                                    id={
                                        "type": "quarterly-milestone-delete",
                                        "goal_id": goal_id,
                                        "milestone_id": milestone["id"],
                                    },
                                    color="danger",
                                    size="sm",
                                    outline=True,
                                ),
                            ],
                            size="sm",
                        ),
                        md=2,
                        className="text-end",
                    ),
                ],
                className="g-2 align-items-center",
            )
        ],
        className=f"quarterly-subcard quarterly-milestone-row {'is-complete' if is_completed else ''}",
    )


def _milestone_card(goal: dict, can_progress: bool, can_structural: bool):
    milestones = goal["derived"].get("milestones", [])
    completed = int(goal["derived"]["completed"])
    total = int(goal["derived"]["total"])
    remaining = max(total - completed, 0)

    add_group = (
        dbc.InputGroup(
            [
                dbc.Input(
                    id={"type": "quarterly-add-milestone-input", "goal_id": goal["id"]},
                    placeholder="Add milestone",
                ),
                dbc.Button(
                    "Add",
                    id={"type": "quarterly-add-milestone-btn", "goal_id": goal["id"]},
                    color="dark",
                ),
            ],
            size="sm",
            className="mt-3",
        )
        if can_structural
        else None
    )

    return dbc.Card(
        dbc.CardBody(
            [
                _goal_header(goal, can_structural),
                *_goal_description(goal, "Move the project by clearing concrete milestones."),
                html.Div(
                    [
                        _challenge_metric("Complete", str(completed), "Milestones finished"),
                        _challenge_metric("Remaining", str(remaining), "Still open"),
                        _challenge_metric("Total", str(total), "Full challenge scope"),
                    ],
                    className="quarterly-inline-stats",
                ),
                _goal_progress(goal["derived"]["progress_pct"], f"{completed} of {total} complete"),
                html.Div(
                    [_milestone_row(goal["id"], milestone, can_progress, can_structural) for milestone in milestones],
                    className="quarterly-milestone-stack",
                ),
                add_group,
                html.Small(
                    "Milestone structure opens in review."
                    if not can_structural
                    else "Review mode: reorder, rename, or trim milestones here.",
                    className="quarterly-muted mt-2 d-block",
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _measured_card(goal: dict, can_progress: bool, can_structural: bool):
    derived = goal["derived"]
    entries = derived.get("entries", [])

    return dbc.Card(
        dbc.CardBody(
            [
                _goal_header(goal, can_structural),
                *_goal_description(
                    goal,
                    f"Shift the measured value by {goal.get('target_direction') or 'change'} over the quarter.",
                ),
                html.Div(
                    [
                        _challenge_metric(
                            "Baseline",
                            _format_number(goal.get("baseline_value")),
                            goal.get("unit") or "Starting point",
                        ),
                        _challenge_metric(
                            "Current",
                            _format_number(derived.get("current")),
                            goal.get("unit") or "Latest log",
                        ),
                        _challenge_metric(
                            "Target",
                            _format_number(goal.get("target_value")),
                            goal.get("unit") or "Quarter target",
                        ),
                        _challenge_metric(
                            "Net Change",
                            _format_number(derived.get("net_change")),
                            goal.get("unit") or "From baseline",
                        ),
                    ],
                    className="quarterly-inline-stats",
                ),
                _goal_progress(
                    derived.get("progress_pct", 0.0),
                    f"{(goal.get('target_direction') or 'increase').title()} toward target",
                ),
                dbc.InputGroup(
                    [
                        dbc.Input(
                            id={"type": "quarterly-measured-input", "goal_id": goal["id"]},
                            type="number",
                            step=0.01,
                            placeholder="Log value",
                            disabled=not can_progress,
                        ),
                        dbc.Button(
                            "Log",
                            id={"type": "quarterly-measured-log-btn", "goal_id": goal["id"]},
                            color="dark",
                            disabled=not can_progress,
                        ),
                    ],
                    size="sm",
                    className="mb-2",
                ),
                html.Div(
                    [
                        html.Div("Recent entries", className="quarterly-subsection-label"),
                        _history_rows(
                            [
                                html.Div(
                                    [
                                        html.Span(_format_date(entry["date"]), className="quarterly-history-date"),
                                        html.Span(f"{_format_number(entry['value'])} {goal.get('unit') or ''}".strip()),
                                    ],
                                    className="quarterly-history-row",
                                )
                                for entry in entries[:4]
                            ],
                            "No entries logged yet.",
                        ),
                    ]
                ),
            ]
        ),
        className="quarterly-goal-card",
    )


def _archived_goal_card(goal: dict):
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        _goal_type_badge(goal["goal_type"]),
                                        html.Span("Archived", className="quarterly-status quarterly-status-dropped"),
                                    ],
                                    className="quarterly-goal-tags",
                                ),
                                html.H5(goal["title"], className="quarterly-goal-title mb-1"),
                            ]
                        ),
                        _goal_status_badge(goal["status"]),
                    ],
                    className="d-flex justify-content-between align-items-start",
                ),
                html.P(goal.get("description") or "Archived challenge.", className="quarterly-goal-description mb-0"),
            ]
        ),
        className="quarterly-goal-card quarterly-archived-goal",
    )


def _render_goal_card(goal: dict, cycle: dict):
    can_progress = bool(cycle["can_progress_edit"])
    can_structural = bool(cycle["can_structural_edit"])
    if goal["is_archived"]:
        return _archived_goal_card(goal)
    if goal["goal_type"] == "counter":
        return _counter_card(goal, can_progress, can_structural)
    if goal["goal_type"] == "binary_recurring":
        return _binary_card(goal, can_progress, can_structural)
    if goal["goal_type"] == "milestone":
        return _milestone_card(goal, can_progress, can_structural)
    return _measured_card(goal, can_progress, can_structural)


def _mode_row(label: str, detail: str, is_open: bool):
    return html.Div(
        [
            html.Div(
                [
                    html.Div(label, className="quarterly-mode-label"),
                    html.Div(detail, className="quarterly-mode-detail"),
                ]
            ),
            html.Span("Open" if is_open else "Locked", className=f"quarterly-mode-pill {'is-open' if is_open else 'is-locked'}"),
        ],
        className="quarterly-mode-row",
    )


def _workflow_card(cycle: dict, active_goal_count: int, note_count: int):
    phase_meta = PHASE_META.get(cycle["phase"], PHASE_META["complete"])
    view_text = "Current quarter" if cycle["is_current"] else "Historical view"
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div("Operating mode", className="quarterly-side-eyebrow"),
                html.H5(f"{phase_meta['label']} · {view_text}", className="mb-1"),
                html.P(phase_meta["summary"], className="quarterly-muted mb-3"),
                _mode_row(
                    "Challenge structure",
                    "Change titles, order, and scope during review only.",
                    cycle["can_structural_edit"],
                ),
                _mode_row(
                    "Progress tracking",
                    "Log counters, compliance, milestones, and measurements when active.",
                    cycle["can_progress_edit"],
                ),
                _mode_row("Review notes", "Capture decisions and reflections at any time.", True),
                html.Hr(className="quarterly-divider"),
                html.Div(
                    [
                        _challenge_metric("Challenges", str(active_goal_count), "Active this cycle"),
                        _challenge_metric("Notes", str(note_count), "Saved reflections"),
                    ],
                    className="quarterly-inline-stats quarterly-inline-stats-compact",
                ),
            ]
        ),
        className="quarterly-side-card mb-3",
    )


def _goal_type_directory():
    return html.Div(
        [
            html.Div(
                [
                    html.Div(meta["label"], className="quarterly-type-card-title"),
                    html.Div(meta["summary"], className="quarterly-type-card-copy"),
                ],
                className="quarterly-type-card",
            )
            for meta in GOAL_TYPE_META.values()
        ],
        className="quarterly-type-grid",
    )


def _empty_state(cycle: dict):
    if not cycle["is_current"]:
        title = "No challenges were recorded for this cycle."
        copy = "This historical quarter is empty, but it stays available for reference."
    elif cycle["can_structural_edit"]:
        title = "This quarter has no challenges yet."
        copy = "You are in review right now, so this is the moment to define the few commitments that matter."
    elif cycle["can_progress_edit"]:
        title = "No challenges are set for this quarter."
        copy = "Execution is underway, so structure is locked. Use review notes to shape the next cycle."
    else:
        title = "No challenges were completed in this cycle."
        copy = "The quarter is closed, and this record is preserved as-is."

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div("Challenge list", className="quarterly-side-eyebrow"),
                html.H4(title, className="mb-2"),
                html.P(copy, className="quarterly-muted mb-4"),
                _goal_type_directory(),
            ]
        ),
        className="quarterly-empty-card",
    )


def _add_goal_panel(cycle: dict):
    disabled = not cycle["can_structural_edit"]
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div("Challenge builder", className="quarterly-side-eyebrow"),
                html.H5("Add Challenge", className="mb-1"),
                html.P(
                    "Review mode is open, so you can design or tighten the quarter."
                    if not disabled
                    else "Challenge setup opens during review. The form stays visible here so the workflow remains clear.",
                    className="quarterly-muted mb-3",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Small("Title", className="quarterly-form-label"),
                                dbc.Input(
                                    id="quarterly-add-title",
                                    placeholder="What is the challenge?",
                                    disabled=disabled,
                                ),
                            ],
                            md=7,
                        ),
                        dbc.Col(
                            [
                                html.Small("Tracking method", className="quarterly-form-label"),
                                dcc.Dropdown(
                                    id="quarterly-add-type",
                                    options=[
                                        {"label": meta["label"], "value": goal_type}
                                        for goal_type, meta in GOAL_TYPE_META.items()
                                    ],
                                    value="counter",
                                    clearable=False,
                                    disabled=disabled,
                                    className="quarterly-dropdown",
                                ),
                            ],
                            md=5,
                        ),
                    ],
                    className="g-2 mb-3",
                ),
                html.Div(id="quarterly-add-type-help", className="quarterly-type-help mb-3"),
                html.Div(
                    [
                        html.Small("Description", className="quarterly-form-label"),
                        dbc.Textarea(
                            id="quarterly-add-description",
                            placeholder="What would successful execution look like?",
                            className="mb-3",
                            disabled=disabled,
                            style={"minHeight": "88px"},
                        ),
                    ]
                ),
                html.Div(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Small("Target", className="quarterly-form-label"),
                                    dbc.Input(
                                        id="quarterly-add-target",
                                        type="number",
                                        placeholder="Target value",
                                        disabled=disabled,
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Starting / current", className="quarterly-form-label"),
                                    dbc.Input(
                                        id="quarterly-add-current",
                                        type="number",
                                        placeholder="Current value",
                                        disabled=disabled,
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Unit", className="quarterly-form-label"),
                                    dbc.Input(
                                        id="quarterly-add-unit",
                                        placeholder="kg, books, sessions, etc.",
                                        disabled=disabled,
                                    ),
                                ],
                                md=4,
                            ),
                        ],
                        className="g-2",
                    ),
                    id="quarterly-add-value-row",
                    className="mb-3",
                ),
                html.Div(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Small("Baseline", className="quarterly-form-label"),
                                    dbc.Input(
                                        id="quarterly-add-baseline",
                                        type="number",
                                        placeholder="Baseline value",
                                        disabled=disabled,
                                    ),
                                ]
                            )
                        ],
                        className="g-2",
                    ),
                    id="quarterly-add-baseline-wrap",
                    className="mb-3",
                ),
                html.Div(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Small("Direction", className="quarterly-form-label"),
                                    dcc.Dropdown(
                                        id="quarterly-add-direction",
                                        options=[
                                            {"label": "Increase", "value": "increase"},
                                            {"label": "Decrease", "value": "decrease"},
                                        ],
                                        value="increase",
                                        clearable=False,
                                        disabled=disabled,
                                        className="quarterly-dropdown",
                                    ),
                                ],
                                md=12,
                            )
                        ],
                        className="g-2",
                    ),
                    id="quarterly-add-direction-row",
                    className="mb-3",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Small("Category", className="quarterly-form-label"),
                                dbc.Input(
                                    id="quarterly-add-category",
                                    placeholder="Optional grouping",
                                    disabled=disabled,
                                ),
                            ],
                            md=5,
                        ),
                        dbc.Col(
                            [
                                html.Small("Setup notes", className="quarterly-form-label"),
                                dbc.Input(
                                    id="quarterly-add-notes",
                                    placeholder="Guardrails, scope, or reminders",
                                    disabled=disabled,
                                ),
                            ],
                            md=7,
                        ),
                    ],
                    className="g-2 mb-3",
                ),
                html.Div(
                    [
                        html.Small("Milestones", className="quarterly-form-label"),
                        dbc.Textarea(
                            id="quarterly-add-milestones",
                            placeholder="One milestone per line",
                            className="mb-2",
                            disabled=disabled,
                            style={"minHeight": "110px"},
                        ),
                        html.Small("Keep milestones concrete so it is obvious when one is done.", className="quarterly-muted"),
                    ],
                    id="quarterly-add-milestone-wrap",
                    className="mb-3",
                ),
                dbc.Button(
                    "Save Challenge",
                    id="quarterly-add-goal-btn",
                    color="dark",
                    disabled=disabled,
                    className="w-100",
                ),
            ]
        ),
        className="quarterly-side-card",
    )


def _notes_panel(notes: list):
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div("Reflection log", className="quarterly-side-eyebrow"),
                html.H5("Review Notes", className="mb-1"),
                html.P(
                    "Capture lessons, scope changes, and what should carry into the next quarter.",
                    className="quarterly-muted mb-3",
                ),
                dbc.Textarea(
                    id="quarterly-note-input",
                    placeholder="Add a review note",
                    className="mb-2",
                    style={"minHeight": "96px"},
                ),
                dbc.Button("Save Note", id="quarterly-save-note-btn", color="dark", size="sm", className="mb-3"),
                html.Div(
                    [
                        html.Div("Saved notes", className="quarterly-subsection-label"),
                        _history_rows(
                            [
                                html.Div(
                                    [
                                        html.Div(_format_timestamp(note["created_at"]), className="quarterly-history-date"),
                                        html.Div(note["content"]),
                                    ],
                                    className="quarterly-note-item",
                                )
                                for note in notes[:8]
                            ],
                            "No notes yet.",
                        ),
                    ]
                ),
            ]
        ),
        className="quarterly-side-card mt-3",
    )


def _cycle_metrics(cycle: dict, goals: list):
    active_goals = [goal for goal in goals if not goal["is_archived"]]
    archived_goals = [goal for goal in goals if goal["is_archived"]]

    if cycle["phase"] == "execution":
        boundary_label = "Review starts"
        boundary_value = _format_date(cycle["execution_end_date"])
        boundary_detail = "Structural edits open then"
        tone = "warm"
    elif cycle["phase"] == "review":
        boundary_label = "Quarter closes"
        boundary_value = _format_date(cycle["end_date"])
        boundary_detail = "Use this window to tighten the next cycle"
        tone = "accent"
    else:
        boundary_label = "Closed"
        boundary_value = _format_date(cycle["end_date"])
        boundary_detail = "Historical record"
        tone = "muted"

    return dbc.Row(
        [
            dbc.Col(
                _metric_card(
                    "Cycle Progress",
                    f"{cycle['cycle_pct']:.1f}%",
                    f"Week {cycle['current_week']} of {cycle['total_weeks']}",
                    tone="primary",
                ),
                md=6,
                xl=3,
            ),
            dbc.Col(
                _metric_card(
                    "Challenges",
                    str(len(active_goals)),
                    f"{len(archived_goals)} archived",
                    tone="default",
                ),
                md=6,
                xl=3,
            ),
            dbc.Col(
                _metric_card(
                    "Phase",
                    PHASE_META.get(cycle["phase"], PHASE_META["complete"])["label"],
                    PHASE_META.get(cycle["phase"], PHASE_META["complete"])["summary"],
                    tone="default",
                ),
                md=6,
                xl=3,
            ),
            dbc.Col(
                _metric_card(boundary_label, boundary_value, boundary_detail, tone=tone),
                md=6,
                xl=3,
            ),
        ],
        className="g-3 mb-3",
    )


def _quarter_header(cycle: dict):
    review_start = _format_date(cycle["execution_end_date"])
    date_window = f"{_format_date(cycle['start_date'])} to {_format_date(cycle['end_date'])}"
    phase_copy = PHASE_META.get(cycle["phase"], PHASE_META["complete"])["summary"]

    mandate_placeholder = (
        "Use this note to define the quarter's focus, boundaries, and standard for success."
        if cycle["can_structural_edit"]
        else "Quarter mandate"
    )

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div("Current cycle" if cycle["is_current"] else "Selected cycle", className="quarterly-side-eyebrow"),
                                html.H3(cycle["title"], className="mb-1"),
                                html.Div(date_window, className="quarterly-muted"),
                            ]
                        ),
                        _phase_badge(cycle["phase"]),
                    ],
                    className="quarterly-header-top",
                ),
                html.P(phase_copy, className="quarterly-header-copy"),
                _segmented_progress(cycle),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div("Quarter mandate", className="quarterly-subsection-label"),
                                html.Div(
                                    f"Execution window flips into review on {review_start}.",
                                    className="quarterly-muted mb-2",
                                ),
                                dbc.Textarea(
                                    id="quarterly-summary-note",
                                    value=cycle.get("summary_note") or "",
                                    placeholder=mandate_placeholder,
                                    className="quarterly-summary-note",
                                    style={"minHeight": "96px"},
                                    disabled=not cycle["can_structural_edit"],
                                ),
                            ],
                            className="flex-grow-1",
                        ),
                        dbc.Button(
                            "Save Purpose Note",
                            id="quarterly-save-summary-note-btn",
                            color="secondary",
                            size="sm",
                            className="align-self-start",
                            disabled=not cycle["can_structural_edit"],
                        ),
                    ],
                    className="quarterly-mandate-block",
                ),
                dbc.Alert(
                    "You are viewing a historical cycle. Challenge edits and progress tracking are locked here, but notes remain available for reflection.",
                    color="secondary",
                    className="mt-3 mb-0",
                )
                if not cycle["is_current"]
                else None,
            ]
        ),
        className="quarterly-header-card mb-3",
    )


def _build_cycle_options(cycles: list, current_cycle_id: int):
    options = []
    for cycle in cycles:
        suffix = "Current" if int(cycle["id"]) == int(current_cycle_id) else "History"
        options.append({"label": f"{cycle['title']} · {suffix}", "value": cycle["id"]})
    return options


def _render_cycle_content(cycle_id: int):
    snapshot = get_cycle_snapshot(cycle_id)
    cycle, goals, notes = snapshot["cycle"], snapshot["goals"], snapshot["notes"]
    active_goals = [goal for goal in goals if not goal["is_archived"]]
    archived_goals = [goal for goal in goals if goal["is_archived"]]

    return html.Div(
        [
            _quarter_header(cycle),
            _cycle_metrics(cycle, goals),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Div("Challenge board", className="quarterly-side-eyebrow"),
                                    html.H4("Quarter Challenges", className="mb-1"),
                                    html.P(
                                        "A tight set of quarter commitments with one tracking method per challenge.",
                                        className="quarterly-muted mb-3",
                                    ),
                                ]
                            ),
                            html.Div(
                                [_render_goal_card(goal, cycle) for goal in active_goals],
                                className="quarterly-goal-stack",
                            )
                            if active_goals
                            else _empty_state(cycle),
                            html.Details(
                                [
                                    html.Summary(
                                        [
                                            html.Span("Archived challenges", className="quarterly-details-title"),
                                            html.Span(str(len(archived_goals)), className="quarterly-details-count"),
                                        ],
                                        className="quarterly-details-summary",
                                    ),
                                    html.Div(
                                        [_render_goal_card(goal, cycle) for goal in archived_goals],
                                        className="quarterly-goal-stack quarterly-archived-stack",
                                    )
                                    if archived_goals
                                    else html.Div("No archived challenges.", className="quarterly-empty-log"),
                                ],
                                className="quarterly-details-card mt-3",
                            ),
                        ],
                        lg=8,
                    ),
                    dbc.Col(
                        [
                            _workflow_card(cycle, len(active_goals), len(notes)),
                            _add_goal_panel(cycle),
                            _notes_panel(notes),
                        ],
                        lg=4,
                    ),
                ],
                className="g-3",
            ),
        ]
    )


def layout():
    current = get_or_create_current_cycle()
    cycles = get_cycles_history()
    options = _build_cycle_options(cycles, current["id"])

    return dbc.Container(
        [
            html.Div(
                [
                    html.H2("Execution Cycle", className="mb-1"),
                    html.P(
                        "Set the quarter in review, execute with simple tracking, and keep past cycles accessible without clutter.",
                        className="quarterly-muted mb-0",
                    ),
                ],
                className="app-page-head",
            ),
            html.Details(
                [
                    html.Summary(
                        [
                            html.Span("Quarter history and navigation", className="quarterly-details-title"),
                            html.Span(current["title"], className="quarterly-details-current"),
                        ],
                        className="quarterly-details-summary",
                    ),
                    html.Div(
                        [
                            html.P(
                                "Keep the page anchored to the current quarter by default. Use this when you need to review another cycle.",
                                className="quarterly-muted mb-2",
                            ),
                            dcc.Dropdown(
                                id="quarterly-cycle-select",
                                options=options,
                                value=current["id"],
                                clearable=False,
                                className="quarterly-dropdown",
                            ),
                        ],
                        className="quarterly-history-browser-body",
                    ),
                ],
                className="quarterly-details-card mb-3",
            ),
            html.Div(id="quarterly-message", className="quarterly-message-slot mb-3"),
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
    current = get_or_create_current_cycle()
    cycles = get_cycles_history()
    return _build_cycle_options(cycles, current["id"])


@callback(
    Output("quarterly-content", "children"),
    Input("quarterly-cycle-select", "value"),
    Input("quarterly-refresh", "data"),
)
def refresh_quarterly_content(selected_cycle_id, _refresh):
    selected = selected_cycle_id or get_or_create_current_cycle()["id"]
    return _render_cycle_content(selected)


@callback(
    Output("quarterly-add-type-help", "children"),
    Output("quarterly-add-value-row", "style"),
    Output("quarterly-add-baseline-wrap", "style"),
    Output("quarterly-add-direction-row", "style"),
    Output("quarterly-add-milestone-wrap", "style"),
    Input("quarterly-add-type", "value"),
)
def sync_add_goal_form(goal_type):
    cleaned = goal_type or "counter"
    meta = GOAL_TYPE_META.get(cleaned, GOAL_TYPE_META["counter"])

    help_box = html.Div(
        [
            html.Span(meta["label"], className="quarterly-type-help-title"),
            html.Span(meta["summary"], className="quarterly-type-help-copy"),
        ]
    )

    hidden = {"display": "none"}
    visible = {}
    value_row_style = visible if cleaned in {"counter", "measured"} else hidden
    baseline_style = visible if cleaned == "measured" else hidden
    direction_style = visible if cleaned == "measured" else hidden
    milestone_style = visible if cleaned == "milestone" else hidden
    return help_box, value_row_style, baseline_style, direction_style, milestone_style


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
        return dbc.Alert("Challenge added.", color="success", duration=2500), (refresh_count or 0) + 1
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
        trigger_type = triggered["type"]
        goal_id = int(triggered["goal_id"])
        if trigger_type == "quarterly-add-milestone-btn":
            val_map = {int(item["goal_id"]): value for item, value in zip(add_ids or [], add_values or [])}
            add_milestone(goal_id, val_map.get(goal_id) or "")
        elif trigger_type == "quarterly-milestone-delete":
            delete_milestone(goal_id, int(triggered["milestone_id"]))
        elif trigger_type == "quarterly-milestone-up":
            move_milestone(goal_id, int(triggered["milestone_id"]), "up")
        elif trigger_type == "quarterly-milestone-down":
            move_milestone(goal_id, int(triggered["milestone_id"]), "down")
        elif trigger_type == "quarterly-milestone-rename-btn":
            key_map = {
                (int(item["goal_id"]), int(item["milestone_id"])): value
                for item, value in zip(rename_ids or [], rename_values or [])
            }
            rename_milestone(
                goal_id,
                int(triggered["milestone_id"]),
                key_map.get((goal_id, int(triggered["milestone_id"]))) or "",
            )
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
        mapping = {int(item["goal_id"]): value for item, value in zip(ids or [], values or [])}
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
        return dbc.Alert("Challenge structure updated.", color="success", duration=1400), (refresh_count or 0) + 1
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
