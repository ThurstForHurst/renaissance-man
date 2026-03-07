"""
Routines Management Page
"""
import dash
from dash import ALL, Input, Output, State, callback, dcc, html
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

from database.db import (
    add_routine_item,
    delete_routine_item,
    get_connection,
    get_routine_items,
    get_routine_templates,
    update_routine_item,
)

dash.register_page(__name__)


def render_routine_editors():
    """Render all routine editors."""
    templates = get_routine_templates()
    return [create_routine_editor(template) for template in templates]


def get_parent_options(template_id, exclude_item_id=None):
    """Return dropdown options for valid parent tasks (top-level only)."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, item_text
        FROM routine_items
        WHERE template_id = ? AND active = 1 AND parent_item_id IS NULL
        ORDER BY order_index
        """,
        (template_id,),
    ).fetchall()
    conn.close()

    options = [{"label": "None (Top-level task)", "value": "__none__"}]
    for row in rows:
        if exclude_item_id is not None and int(row["id"]) == int(exclude_item_id):
            continue
        options.append({"label": row["item_text"], "value": int(row["id"])})
    return options


def layout():
    return dbc.Container(
        [
            html.Div([
                html.H2("Manage Routines", className="mb-1"),
                html.P("Maintain your reusable daily routine templates.", className="mb-0"),
            ], className="app-page-head"),
            dbc.Alert(
                [
                    html.I(className="fas fa-info-circle me-2"),
                    "Manage your daily routine templates and items. Changes will apply starting tomorrow.",
                ],
                color="info",
                className="mb-4",
            ),
            html.Div(id="routines-feedback"),
            dcc.Store(id="routines-refresh", data=0),
            dcc.Store(id="routine-edit-item-id"),
            dcc.Store(id="routine-edit-template-id"),
            html.Div(id="routines-editors", children=render_routine_editors()),
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle("Add Routine Task", id="routine-item-modal-title")),
                    dbc.ModalBody(
                        [
                            dbc.Label("Task"),
                            dbc.Input(id="routine-item-text-input", type="text", placeholder="Enter task text"),
                            dbc.Label("XP Value", className="mt-3"),
                            dbc.Input(id="routine-item-xp-input", type="number", min=1, step=1, value=1),
                            dbc.Label("Parent Task", className="mt-3"),
                            dcc.Dropdown(
                                id="routine-parent-item-input",
                                options=[{"label": "None (Top-level task)", "value": "__none__"}],
                                value="__none__",
                                clearable=False,
                            ),
                        ]
                    ),
                    dbc.ModalFooter(
                        [
                            dbc.Button("Cancel", id="routine-item-cancel-btn", color="secondary", outline=True),
                            dbc.Button("Add Task", id="routine-item-save-btn", color="primary"),
                        ]
                    ),
                ],
                id="routine-item-modal",
                is_open=False,
                zIndex=3000,
                style={"zIndex": "3000", "position": "fixed"},
                backdrop_style={"zIndex": "2990"},
                dialog_style={"marginTop": "150px"},
                content_style={"zIndex": "3010", "position": "relative"},
                class_name="routine-item-modal",
                dialog_class_name="routine-item-modal-dialog",
                content_class_name="routine-item-modal-content",
                backdrop_class_name="routine-item-modal-backdrop",
            ),
        ],
        fluid=True,
        className="app-page-shell",
    )


def create_routine_editor(template):
    """Create an editor section for a routine template."""
    items = get_routine_items(template["id"])
    children_by_parent = {}
    for item in items:
        parent_id = item["parent_item_id"]
        if parent_id is not None:
            children_by_parent.setdefault(parent_id, []).append(item)

    for child_items in children_by_parent.values():
        child_items.sort(key=lambda x: x["order_index"])

    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.H4(f'{template["name"]} Routine', className="mb-0"),
                    html.Small(
                        f'Available at: {template["time_available"]} | Bonus XP: {template["bonus_xp"]}',
                        className="text-muted",
                    ),
                ]
            ),
            dbc.CardBody(
                [
                    html.H6("Items:", className="mb-3"),
                    html.Div(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            html.Div(
                                                                [
                                                                    html.Strong(item["item_text"]),
                                                                    html.Span(
                                                                        f' ({item["xp_value"]} XP)',
                                                                        className="text-muted ms-2",
                                                                    ),
                                                                ]
                                                            )
                                                        ],
                                                        width=8,
                                                    ),
                                                    dbc.Col(
                                                        [
                                                            dbc.ButtonGroup(
                                                                [
                                                                    dbc.Button(
                                                                        "Update",
                                                                        id={"type": "edit-item-btn", "item": item["id"]},
                                                                        color="warning",
                                                                        size="sm",
                                                                        outline=True,
                                                                    ),
                                                                    dbc.Button(
                                                                        "Add Subtask",
                                                                        id={
                                                                            "type": "add-subtask-btn",
                                                                            "item": item["id"],
                                                                            "template": template["id"],
                                                                        },
                                                                        color="secondary",
                                                                        size="sm",
                                                                        outline=True,
                                                                    ),
                                                                    dbc.Button(
                                                                        "X",
                                                                        id={"type": "delete-item-btn", "item": item["id"]},
                                                                        color="danger",
                                                                        size="sm",
                                                                        outline=True,
                                                                    ),
                                                                ],
                                                                size="sm",
                                                                className="float-end",
                                                            )
                                                        ],
                                                        width=4,
                                                    ),
                                                ]
                                            ),
                                            html.Div(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                [
                                                                    html.Div(
                                                                        [
                                                                            html.Span("-> ", className="text-muted"),
                                                                            html.Span(child["item_text"]),
                                                                            html.Span(
                                                                                f' ({child["xp_value"]} XP)',
                                                                                className="text-muted ms-2",
                                                                            ),
                                                                        ]
                                                                    )
                                                                ],
                                                                width=8,
                                                            ),
                                                            dbc.Col(
                                                                [
                                                                    dbc.ButtonGroup(
                                                                        [
                                                                            dbc.Button(
                                                                                "Update",
                                                                                id={"type": "edit-item-btn", "item": child["id"]},
                                                                                color="warning",
                                                                                size="sm",
                                                                                outline=True,
                                                                            ),
                                                                            dbc.Button(
                                                                                "X",
                                                                                id={"type": "delete-item-btn", "item": child["id"]},
                                                                                color="danger",
                                                                                size="sm",
                                                                                outline=True,
                                                                            ),
                                                                        ],
                                                                        size="sm",
                                                                        className="float-end",
                                                                    )
                                                                ],
                                                                width=4,
                                                            ),
                                                        ],
                                                        className="mt-2",
                                                    )
                                                    for child in children_by_parent.get(item["id"], [])
                                                ],
                                                className="ms-4 mt-2",
                                            ),
                                        ]
                                    )
                                ],
                                className="mb-2",
                            )
                            for item in items
                            if item["parent_item_id"] is None
                        ]
                    ),
                    html.Hr(),
                    dbc.Button(
                        "+ Add Item",
                        id={"type": "add-item-btn", "template": template["id"]},
                        color="success",
                        size="sm",
                        outline=True,
                    ),
                ]
            ),
        ],
        className="mb-4",
    )


@callback(Output("routines-editors", "children"), Input("routines-refresh", "data"))
def refresh_routine_editors(_refresh):
    return render_routine_editors()


@callback(
    Output("routine-item-modal", "is_open"),
    Output("routine-item-modal-title", "children"),
    Output("routine-item-text-input", "value"),
    Output("routine-item-xp-input", "value"),
    Output("routine-parent-item-input", "options"),
    Output("routine-parent-item-input", "value"),
    Output("routine-item-save-btn", "children"),
    Output("routine-edit-item-id", "data"),
    Output("routine-edit-template-id", "data"),
    Output("routines-feedback", "children", allow_duplicate=True),
    Output("routines-refresh", "data", allow_duplicate=True),
    Input({"type": "add-item-btn", "template": ALL}, "n_clicks"),
    Input({"type": "add-subtask-btn", "item": ALL, "template": ALL}, "n_clicks"),
    Input({"type": "edit-item-btn", "item": ALL}, "n_clicks"),
    Input("routine-item-save-btn", "n_clicks"),
    Input("routine-item-cancel-btn", "n_clicks"),
    State("routine-item-text-input", "value"),
    State("routine-item-xp-input", "value"),
    State("routine-parent-item-input", "value"),
    State("routine-edit-item-id", "data"),
    State("routine-edit-template-id", "data"),
    State("routines-refresh", "data"),
    prevent_initial_call=True,
)
def manage_routine_item_modal(
    _add_clicks,
    _add_subtask_clicks,
    _edit_clicks,
    _save_click,
    _cancel_click,
    item_text,
    xp_value,
    parent_item_value,
    edit_item_id,
    template_id,
    refresh_count,
):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id
    current_refresh = refresh_count or 0

    if not triggered_id or not ctx.triggered:
        raise PreventUpdate

    triggered_value = ctx.triggered[0].get("value")
    if isinstance(triggered_value, list):
        if not any(v for v in triggered_value if v):
            raise PreventUpdate
    elif not triggered_value:
        raise PreventUpdate

    if triggered_id == "routine-item-cancel-btn":
        return (
            False,
            "Add Routine Task",
            "",
            1,
            [{"label": "None (Top-level task)", "value": "__none__"}],
            "__none__",
            "Add Task",
            None,
            None,
            dash.no_update,
            current_refresh,
        )

    if isinstance(triggered_id, dict) and triggered_id.get("type") == "add-item-btn":
        options = get_parent_options(triggered_id["template"])
        return (
            True,
            "Add Routine Task",
            "",
            1,
            options,
            "__none__",
            "Add Task",
            None,
            triggered_id["template"],
            dash.no_update,
            current_refresh,
        )

    if isinstance(triggered_id, dict) and triggered_id.get("type") == "add-subtask-btn":
        options = get_parent_options(triggered_id["template"])
        return (
            True,
            "Add Subtask",
            "",
            1,
            options,
            int(triggered_id["item"]),
            "Add Subtask",
            None,
            triggered_id["template"],
            dash.no_update,
            current_refresh,
        )

    if isinstance(triggered_id, dict) and triggered_id.get("type") == "edit-item-btn":
        item_id = triggered_id["item"]
        conn = get_connection()
        item = conn.execute(
            """
            SELECT id, template_id, item_text, xp_value, parent_item_id
            FROM routine_items
            WHERE id = ? AND active = 1
            """,
            (item_id,),
        ).fetchone()
        conn.close()

        if item is None:
            feedback = dbc.Alert("Could not find that routine task.", color="danger", dismissable=True, duration=3000)
            return (
                False,
                "Add Routine Task",
                "",
                1,
                [{"label": "None (Top-level task)", "value": "__none__"}],
                "__none__",
                "Add Task",
                None,
                None,
                feedback,
                current_refresh,
            )

        options = get_parent_options(item["template_id"], exclude_item_id=item["id"])
        parent_value = item["parent_item_id"] if item["parent_item_id"] is not None else "__none__"
        return (
            True,
            "Update Routine Task",
            item["item_text"],
            item["xp_value"],
            options,
            parent_value,
            "Update",
            item["id"],
            item["template_id"],
            dash.no_update,
            current_refresh,
        )

    if triggered_id == "routine-item-save-btn":
        text = (item_text or "").strip()
        if not text:
            feedback = dbc.Alert("Task text is required.", color="warning", dismissable=True, duration=3000)
            return (
                True,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                edit_item_id,
                template_id,
                feedback,
                current_refresh,
            )

        try:
            xp_int = int(xp_value)
        except (TypeError, ValueError):
            feedback = dbc.Alert("XP must be a whole number.", color="warning", dismissable=True, duration=3000)
            return (
                True,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                edit_item_id,
                template_id,
                feedback,
                current_refresh,
            )

        if xp_int < 1:
            feedback = dbc.Alert("XP must be at least 1.", color="warning", dismissable=True, duration=3000)
            return (
                True,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                edit_item_id,
                template_id,
                feedback,
                current_refresh,
            )

        parent_item_id = None if parent_item_value in (None, "__none__") else int(parent_item_value)

        if template_id is None:
            feedback = dbc.Alert("Missing routine template context.", color="danger", dismissable=True, duration=3000)
            return (
                True,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                edit_item_id,
                template_id,
                feedback,
                current_refresh,
            )

        conn = get_connection()
        if parent_item_id is not None:
            parent_row = conn.execute(
                """
                SELECT id, parent_item_id
                FROM routine_items
                WHERE id = ? AND template_id = ? AND active = 1
                """,
                (parent_item_id, int(template_id)),
            ).fetchone()
            if parent_row is None:
                conn.close()
                feedback = dbc.Alert("Selected parent task no longer exists.", color="danger", dismissable=True, duration=3000)
                return (
                    True,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    edit_item_id,
                    template_id,
                    feedback,
                    current_refresh,
                )
            if parent_row["parent_item_id"] is not None:
                conn.close()
                feedback = dbc.Alert("Subtasks cannot have subtasks.", color="warning", dismissable=True, duration=3000)
                return (
                    True,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    edit_item_id,
                    template_id,
                    feedback,
                    current_refresh,
                )
            if edit_item_id and int(edit_item_id) == int(parent_item_id):
                conn.close()
                feedback = dbc.Alert("A task cannot be its own parent.", color="warning", dismissable=True, duration=3000)
                return (
                    True,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    edit_item_id,
                    template_id,
                    feedback,
                    current_refresh,
                )

        if edit_item_id:
            child_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM routine_items
                WHERE parent_item_id = ? AND active = 1
                """,
                (int(edit_item_id),),
            ).fetchone()["count"]
            if child_count > 0 and parent_item_id is not None:
                conn.close()
                feedback = dbc.Alert(
                    "This task already has subtasks and cannot become a subtask.",
                    color="warning",
                    dismissable=True,
                    duration=3000,
                )
                return (
                    True,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    edit_item_id,
                    template_id,
                    feedback,
                    current_refresh,
                )
            update_routine_item(int(edit_item_id), text, xp_int)
            conn.execute(
                """
                UPDATE routine_items
                SET parent_item_id = ?
                WHERE id = ?
                """,
                (parent_item_id, int(edit_item_id)),
            )
            conn.commit()
            feedback = dbc.Alert("Routine task updated.", color="success", dismissable=True, duration=2500)
        else:
            add_routine_item(int(template_id), text, xp_int, parent_item_id=parent_item_id)
            feedback = dbc.Alert("Routine task added.", color="success", dismissable=True, duration=2500)
        conn.close()

        return (
            False,
            "Add Routine Task",
            "",
            1,
            [{"label": "None (Top-level task)", "value": "__none__"}],
            "__none__",
            "Add Task",
            None,
            None,
            feedback,
            current_refresh + 1,
        )

    raise PreventUpdate


@callback(
    Output("routines-feedback", "children", allow_duplicate=True),
    Output("routines-refresh", "data", allow_duplicate=True),
    Input({"type": "delete-item-btn", "item": ALL}, "n_clicks"),
    State("routines-refresh", "data"),
    prevent_initial_call=True,
)
def delete_routine_task(_delete_clicks, refresh_count):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id
    if not ctx.triggered:
        raise PreventUpdate

    triggered_value = ctx.triggered[0].get("value")
    if isinstance(triggered_value, list):
        if not any(v for v in triggered_value if v):
            raise PreventUpdate
    elif not triggered_value:
        raise PreventUpdate

    if not isinstance(triggered_id, dict) or triggered_id.get("type") != "delete-item-btn":
        raise PreventUpdate

    item_id = int(triggered_id["item"])
    conn = get_connection()
    child_rows = conn.execute(
        """
        SELECT id FROM routine_items
        WHERE parent_item_id = ? AND active = 1
        """,
        (item_id,),
    ).fetchall()
    conn.close()

    for child in child_rows:
        delete_routine_item(int(child["id"]))
    delete_routine_item(item_id)
    feedback = dbc.Alert("Routine task removed.", color="info", dismissable=True, duration=2500)
    return feedback, (refresh_count or 0) + 1
