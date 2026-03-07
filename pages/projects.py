"""
Projects Tracking Page
"""
import dash
from dash import html, dcc, callback, Input, Output, State, ctx, ALL, MATCH
import dash_bootstrap_components as dbc
from database.db import (
    create_project, update_project, archive_project, log_project_session, 
    get_active_projects, get_project_summary, get_connection,
    get_all_tasks, add_task, set_task_completed, delete_task, get_brisbane_date
)
from analytics.scoring import award_xp, calculate_project_xp

dash.register_page(__name__)

TIER_CONFIG = {
    1: {'name': 'Tier 1', 'subtitle': 'Immediate', 'accent': '#B02A37'},
    2: {'name': 'Tier 2', 'subtitle': 'High Priority', 'accent': '#9A6700'},
    3: {'name': 'Tier 3', 'subtitle': 'Medium Priority', 'accent': '#0A58CA'},
    4: {'name': 'Tier 4', 'subtitle': 'Backlog', 'accent': '#495057'}
}

def build_project_summary_row():
    """Build summary cards row from latest DB values."""
    summary = get_project_summary()
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Active Projects", className="text-muted mb-2"),
                    html.H3(f"{summary['active_projects']}", className="mb-0")
                ])
            ], className="shadow-sm summary-card")
        ], md=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Time This Week", className="text-muted mb-2"),
                    html.H3(f"{summary['time_this_week']} min", className="mb-0")
                ])
            ], className="shadow-sm summary-card")
        ], md=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Projects Touched (this week)", className="text-muted mb-2"),
                    html.H3(f"{summary['touched_this_week']}", className="mb-0")
                ])
            ], className="shadow-sm summary-card")
        ], md=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Tier 1 Projects", className="text-muted mb-2"),
                    html.H3(f"{summary['tier1_projects']}", className="mb-0")
                ])
            ], className="shadow-sm summary-card")
        ], md=3),
    ], className="mb-4")

def build_projects_board():
    """Build tiered project cards from latest DB values."""
    projects = get_active_projects()
    projects_by_tier = {1: [], 2: [], 3: [], 4: []}
    for p in projects:
        tier = p.get('tier', 4)
        projects_by_tier[tier].append(p)

    return html.Div([
        create_tier_section(tier, projects_by_tier[tier])
        for tier in [1, 2, 3, 4]
    ])

def create_project_card(project):
    """Create a clean, professional horizontal project card."""
    days_inactive = int(project['days_since_touched']) if project['days_since_touched'] else 0
    inactivity_tone = "danger" if days_inactive > 14 else "secondary"
    thesis_text = project['thesis'] if project['thesis'] else "No description provided."

    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.H6(project['name'], className="mb-1"),
                                    dbc.Badge(project['category'], color="light", text_color="dark", className="border"),
                                ],
                                className="d-flex justify-content-between align-items-start",
                            ),
                            html.P(thesis_text, className="text-muted small mb-2"),
                            html.Div(
                                [
                                    dbc.Badge(f"{project['session_count']} sessions", color="light", text_color="dark", className="me-2 border"),
                                    dbc.Badge(f"{project['total_minutes'] or 0} min", color="light", text_color="dark", className="me-2 border"),
                                    dbc.Badge(f"{days_inactive}d inactive", color=inactivity_tone),
                                ],
                                className="d-flex flex-wrap",
                            ),
                        ],
                        width=8,
                    ),
                    dbc.Col(
                        dbc.ButtonGroup(
                            [
                                dbc.Button(
                                    "Log",
                                    id={'type': 'log-session-btn', 'index': project['id']},
                                    color="primary",
                                    size="sm",
                                    outline=True,
                                ),
                                dbc.Button(
                                    "Edit",
                                    id={'type': 'edit-project-btn', 'index': project['id']},
                                    color="warning",
                                    size="sm",
                                    outline=True,
                                ),
                                dbc.Button(
                                    "Archive",
                                    id={'type': 'delete-project-btn', 'index': project['id']},
                                    color="danger",
                                    size="sm",
                                    outline=True,
                                ),
                            ],
                            className="w-100",
                            vertical=True,
                        ),
                        width=4,
                        className="text-end d-flex align-items-start",
                    ),
                ]
            )
        ),
        className="mb-2 shadow-sm project-card-clean",
    )

def layout():
    tasks = get_all_tasks()

    return dbc.Container([
        html.Div([
            html.H2("Projects", className="mb-1"),
            html.P("Plan, prioritize, and execute meaningful project work.", className="mb-0"),
        ], className="app-page-head"),

        # Summary cards
        html.Div(id='projects-summary-row', children=build_project_summary_row()),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Project Portfolio", className="mb-0"),
                                html.Small("Prioritized by tier", className="text-muted")
                            ], width=8),
                            dbc.Col([
                                dbc.Button("New Project", id='new-project-btn', color="dark", size="sm", className="float-end")
                            ], width=4, className="d-flex align-items-center")
                        ], className="g-2")
                    ], className="bg-white"),
                    dbc.CardBody(
                        html.Div(id='projects-board', children=build_projects_board()),
                        className="pt-3"
                    ),
                ], className="shadow-sm border-0")
            ], md=8),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Task Inbox", className="mb-0"),
                        html.Small(f"{len(tasks)} tracked", className="text-muted")
                    ]),
                    dbc.CardBody([
                        html.Div(id='tasks-list', children=create_tasks_list(tasks)),
                        html.Hr(),
                        dbc.Input(
                            id='new-task-input',
                            placeholder="Add a task...",
                            className="mb-2"
                        ),
                        dbc.Button(
                            "Add Task",
                            id='add-task-btn',
                            color="dark",
                            size="sm",
                            className="w-100"
                        )
                    ])
                ], className="shadow-sm border-0", style={'position': 'sticky', 'top': '120px'})
            ], md=4),
        ]),

        # All the modals (session, edit, new project, delete)
        create_session_modal(),
        create_edit_modal(),
        create_new_project_modal(),
        create_delete_modal(),

        # Stores
        dcc.Store(id='selected-project-id', data=None),
        dcc.Store(id='delete-project-id', data=None),
        dcc.Store(id='projects-refresh-trigger', data=0),

    ], fluid=True, className="app-page-shell")

def create_tasks_list(tasks):
    """Create the tasks list display."""
    if not tasks:
        return html.P("No tasks yet. Add one above!", className="text-muted text-center py-3")

    task_items = []
    for task in tasks:
        task_items.append(
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Checkbox(
                                id={'type': 'task-check', 'task': task['id']},
                                label=task['text'],
                                value=bool(task['completed']),
                                className="task-inbox-check mb-0"
                            )
                        ], width=9),
                        dbc.Col([
                            dbc.Button(
                                "Delete",
                                id={'type': 'delete-task-btn', 'task': task['id']},
                                color="danger",
                                size="sm",
                                outline=True,
                                className="w-100"
                            )
                        ], width=3)
                    ])
                ], className="py-2")
            ], className="mb-2")
        )

    return task_items

def create_tier_section(tier: int, projects: list):
    """Create a collapsible section for a tier."""
    config = TIER_CONFIG[tier]

    return dbc.Card([
        dbc.CardHeader([
            dbc.Row([
                dbc.Col([
                    html.H5(
                        f"{config['name']} - {config['subtitle']} ({len(projects)})",
                        className="mb-0"
                    )
                ], width=10),
                dbc.Col([
                    dbc.Button(
                        "Toggle",
                        id={'type': 'collapse-tier', 'tier': tier},
                        color="link",
                        size="sm",
                        className="float-end text-decoration-none"
                    )
                ], width=2)
            ])
        ], style={
            'backgroundColor': '#f8f9fa',
            'color': '#212529',
            'borderLeft': f"4px solid {config['accent']}",
            'cursor': 'pointer'
        }),
        dbc.Collapse([
            dbc.CardBody([
                html.Div([
                    create_project_card(p) for p in projects
                ]) if projects else html.P("No projects in this tier", className="text-muted text-center py-3")
            ])
        ], id={'type': 'tier-collapse', 'tier': tier}, is_open=True)
    ], className="mb-3 border-0 shadow-sm")

def create_session_modal():
    """Create session logging modal."""
    return dbc.Modal([
        dbc.ModalHeader(html.H5("Log Session", id='session-modal-title')),
        dbc.ModalBody([
            dbc.Label("Date"),
            dcc.DatePickerSingle(
                id='session-date',
                date=get_brisbane_date(),
                display_format='YYYY-MM-DD',
                className="mb-3"
            ),
            
            dbc.Label("Duration (minutes)"),
            dbc.Input(
                id='session-duration',
                type='number',
                value=60,
                min=1,
                className="mb-3"
            ),
            
            dbc.Label("What did you accomplish?"),
            dbc.Textarea(
                id='session-output',
                placeholder="Describe your progress...",
                rows=3,
                className="mb-3"
            ),
            
            dbc.Checklist(
                id='session-completed',
                options=[{'label': ' Project completed (bonus XP!)', 'value': 'completed'}],
                value=[],
                className="mb-3"
            ),
            
            html.Div(id='session-feedback')
        ]),
        dbc.ModalFooter([
            dbc.Button("Log Session", id='submit-session-btn', color="primary"),
            dbc.Button("Cancel", id='cancel-session-btn', color="secondary")
        ])
    ], id='session-modal',
       is_open=False,
       size="lg",
       zIndex=3000,
       style={'zIndex': '3000', 'position': 'fixed'},
       backdrop_style={'zIndex': '2990'},
       dialog_style={'marginTop': '130px'},
       content_style={'zIndex': '3010', 'position': 'relative'},
       class_name='session-modal',
       dialog_class_name='session-modal-dialog',
       content_class_name='session-modal-content',
       backdrop_class_name='session-modal-backdrop')

def create_edit_modal():
    """Create edit project modal."""
    return dbc.Modal([
        dbc.ModalHeader("Edit Project"),
        dbc.ModalBody([
            dbc.Label("Project Name"),
            dbc.Input(id='edit-project-name', className="mb-3"),
            
            dbc.Label("Category"),
            dcc.Dropdown(
                id='edit-project-category',
                options=[
                    {'label': 'PhD', 'value': 'PhD'},
                    {'label': 'Work', 'value': 'Work'},
                    {'label': 'Personal', 'value': 'Personal'},
                ],
                className="mb-3"
            ),
            
            dbc.Label("Tier"),
            dcc.Dropdown(
                id='edit-project-tier',
                options=[
                    {'label': 'Tier 1: Immediate', 'value': 1},
                    {'label': 'Tier 2: High Priority', 'value': 2},
                    {'label': 'Tier 3: Medium Priority', 'value': 3},
                    {'label': 'Tier 4: Backlog', 'value': 4},
                ],
                className="mb-3"
            ),
            
            dbc.Label("Importance (for XP)"),
            dcc.Slider(
                id='edit-project-importance',
                min=1, max=3, step=1, value=2,
                marks={1: 'Low', 2: 'Med', 3: 'High'},
                className="mb-3"
            ),
            
            dbc.Label("Description/Thesis"),
            dbc.Textarea(
                id='edit-project-thesis',
                rows=2,
                className="mb-3"
            ),
            
            dbc.Label("Status"),
            dcc.Dropdown(
                id='edit-project-status',
                options=[
                    {'label': 'Active', 'value': 'active'},
                    {'label': 'Paused', 'value': 'paused'},
                    {'label': 'Completed', 'value': 'completed'},
                ],
                className="mb-3"
            ),
        ]),
        dbc.ModalFooter([
            dbc.Button("Save Changes", id='save-edit-btn', color="success"),
            dbc.Button("Cancel", id='cancel-edit-btn', color="secondary")
        ])
    ], id='edit-modal',
       is_open=False,
       size="lg",
       zIndex=3000,
       style={'zIndex': '3000', 'position': 'fixed'},
       backdrop_style={'zIndex': '2990'},
       dialog_style={'marginTop': '130px'},
       content_style={'zIndex': '3010', 'position': 'relative'},
       class_name='edit-modal',
       dialog_class_name='edit-modal-dialog',
       content_class_name='edit-modal-content',
       backdrop_class_name='edit-modal-backdrop')

def create_new_project_modal():
    """Create new project modal."""
    return dbc.Modal([
        dbc.ModalHeader("Create New Project"),
        dbc.ModalBody([
            dbc.Label("Project Name"),
            dbc.Input(id='new-project-name', placeholder="e.g., PhD Thesis", className="mb-3"),
            
            dbc.Label("Category"),
            dcc.Dropdown(
                id='new-project-category',
                options=[
                    {'label': 'PhD', 'value': 'PhD'},
                    {'label': 'Work', 'value': 'Work'},
                    {'label': 'Personal', 'value': 'Personal'},
                ],
                value='Personal',
                className="mb-3"
            ),
            
            dbc.Label("Tier"),
            dcc.Dropdown(
                id='new-project-tier',
                options=[
                    {'label': 'Tier 1: Immediate', 'value': 1},
                    {'label': 'Tier 2: High Priority', 'value': 2},
                    {'label': 'Tier 3: Medium Priority', 'value': 3},
                    {'label': 'Tier 4: Backlog', 'value': 4},
                ],
                value=3,
                className="mb-3"
            ),
            
            dbc.Label("Importance (for XP calculation)"),
            dcc.Slider(
                id='new-project-importance',
                min=1, max=3, step=1, value=2,
                marks={1: 'Low', 2: 'Med', 3: 'High'},
                className="mb-3"
            ),
            
            dbc.Label("Description/Goal"),
            dbc.Textarea(
                id='new-project-thesis',
                placeholder="What is this project about?",
                rows=2,
                className="mb-3"
            )
        ]),
        dbc.ModalFooter([
            dbc.Button("Create", id='create-project-submit', color="success"),
            dbc.Button("Cancel", id='create-project-cancel', color="secondary")
        ])
    ], id='new-project-modal',
       is_open=False,
       size="lg",
       zIndex=3000,
       style={'zIndex': '3000', 'position': 'fixed'},
       backdrop_style={'zIndex': '2990'},
       dialog_style={'marginTop': '130px'},
       content_style={'zIndex': '3010', 'position': 'relative'},
       class_name='new-project-modal',
       dialog_class_name='new-project-modal-dialog',
       content_class_name='new-project-modal-content',
       backdrop_class_name='new-project-modal-backdrop')

def create_delete_modal():
    """Create delete confirmation modal."""
    return dbc.Modal([
        dbc.ModalHeader("Archive Project"),
        dbc.ModalBody([
            html.P("Are you sure you want to archive this project?"),
            html.P(id='delete-confirm-text', className="fw-bold"),
            html.Small("This will hide the project but preserve all session history.", className="text-muted")
        ]),
        dbc.ModalFooter([
            dbc.Button("Yes, Archive", id='confirm-delete-btn', color="danger"),
            dbc.Button("Cancel", id='cancel-delete-btn', color="secondary")
        ])
    ], id='delete-modal',
       is_open=False,
       zIndex=3000,
       style={'zIndex': '3000', 'position': 'fixed'},
       backdrop_style={'zIndex': '2990'},
       dialog_style={'marginTop': '130px'},
       content_style={'zIndex': '3010', 'position': 'relative'},
       class_name='delete-modal',
       dialog_class_name='delete-modal-dialog',
       content_class_name='delete-modal-content',
       backdrop_class_name='delete-modal-backdrop')

# Task callbacks
@callback(
    Output('tasks-list', 'children'),
    Output('task-sync-signal', 'data', allow_duplicate=True),
    Input('add-task-btn', 'n_clicks'),
    Input({'type': 'task-check', 'task': ALL}, 'value'),
    Input({'type': 'delete-task-btn', 'task': ALL}, 'n_clicks'),
    State('new-task-input', 'value'),
    State({'type': 'task-check', 'task': ALL}, 'id'),
    prevent_initial_call=True
)
def handle_tasks(add_click, check_values, delete_clicks, new_task_text, check_ids):
    """Handle task add, toggle, and delete."""
    if not ctx.triggered:
        return dash.no_update, dash.no_update
    
    triggered = ctx.triggered[0]
    triggered_id = triggered['prop_id']
    triggered_value = triggered.get('value')
    if triggered_value is None:
        return dash.no_update, dash.no_update
    sync_payload = dash.no_update
    
    # Handle add task
    if 'add-task-btn' in triggered_id and new_task_text:
        add_task(new_task_text)
        from datetime import datetime, timezone
        sync_payload = {'event': 'task_added', 'ts': datetime.now(timezone.utc).isoformat()}
    
    # Handle task checkbox toggle
    elif 'task-check' in triggered_id:
        import json
        task_data = json.loads(triggered_id.split('.')[0])
        task_id = task_data['task']
        checked_state = None
        for i, item in enumerate(check_ids or []):
            if item.get('task') == task_id:
                checked_state = bool(check_values[i])
                break
        if checked_state is None:
            return dash.no_update, dash.no_update
        set_task_completed(task_id, checked_state)
        from datetime import datetime, timezone
        sync_payload = {
            'event': 'task_checked',
            'task_id': task_id,
            'checked': checked_state,
            'ts': datetime.now(timezone.utc).isoformat()
        }
    
    # Handle task delete
    elif 'delete-task-btn' in triggered_id:
        import json
        task_data = json.loads(triggered_id.split('.')[0])
        delete_task(task_data['task'])
        from datetime import datetime, timezone
        sync_payload = {'event': 'task_deleted', 'ts': datetime.now(timezone.utc).isoformat()}
    
    # Refresh tasks list
    tasks = get_all_tasks()
    return create_tasks_list(tasks), sync_payload


@callback(
    Output('tasks-list', 'children', allow_duplicate=True),
    Input('task-sync-signal', 'data'),
    prevent_initial_call=True
)
def sync_tasks_list(sync_signal):
    """Refresh projects task list when sidebar updates tasks."""
    if not sync_signal:
        return dash.no_update
    return create_tasks_list(get_all_tasks())

# Clear input after adding task
@callback(
    Output('new-task-input', 'value'),
    Input('add-task-btn', 'n_clicks'),
    prevent_initial_call=True
)
def clear_task_input(n):
    return ""

# (Keep all the existing project callbacks from before - toggle tier, new project, session modal, etc.)
# I'll include them all below for completeness:

# Toggle tier collapse
@callback(
    Output({'type': 'tier-collapse', 'tier': MATCH}, 'is_open'),
    Input({'type': 'collapse-tier', 'tier': MATCH}, 'n_clicks'),
    State({'type': 'tier-collapse', 'tier': MATCH}, 'is_open'),
    prevent_initial_call=True
)
def toggle_tier(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open

# New project modal
@callback(
    Output('new-project-modal', 'is_open'),
    Input('new-project-btn', 'n_clicks'),
    Input('create-project-submit', 'n_clicks'),
    Input('create-project-cancel', 'n_clicks'),
    State('new-project-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_new_project(open_n, submit_n, cancel_n, is_open):
    return not is_open

# Create project
@callback(
    Output('new-project-modal', 'is_open', allow_duplicate=True),
    Output('projects-refresh-trigger', 'data', allow_duplicate=True),
    Input('create-project-submit', 'n_clicks'),
    State('new-project-name', 'value'),
    State('new-project-category', 'value'),
    State('new-project-tier', 'value'),
    State('new-project-importance', 'value'),
    State('new-project-thesis', 'value'),
    State('projects-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def create_new_project(n_clicks, name, category, tier, importance, thesis, refresh_count):
    if not n_clicks or not name:
        return dash.no_update, dash.no_update
    
    create_project(name, category, tier or 3, importance, thesis or "")
    return False, (refresh_count or 0) + 1

# Open session modal
@callback(
    Output('session-modal', 'is_open'),
    Output('session-modal-title', 'children'),
    Output('selected-project-id', 'data'),
    Input({'type': 'log-session-btn', 'index': ALL}, 'n_clicks'),
    Input('cancel-session-btn', 'n_clicks'),
    Input('submit-session-btn', 'n_clicks'),
    State('session-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_session_modal(log_clicks, cancel_n, submit_n, is_open):
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update
    
    triggered = ctx.triggered[0]
    triggered_id = triggered['prop_id']
    triggered_value = triggered.get('value')

    # Ignore synthetic triggers caused by dynamic list re-renders.
    if triggered_value is None or triggered_value == 0:
        return dash.no_update, dash.no_update, dash.no_update
    
    if 'log-session-btn' in triggered_id:
        import json
        button_id = json.loads(triggered_id.split('.')[0])
        project_id = button_id['index']
        
        conn = get_connection()
        project = conn.execute("SELECT name FROM projects WHERE id = ?", (project_id,)).fetchone()
        conn.close()
        
        return True, f"Log Session: {project['name']}", project_id
    
    return False, dash.no_update, None

# Log session
@callback(
    Output('session-feedback', 'children'),
    Output('session-modal', 'is_open', allow_duplicate=True),
    Output('projects-refresh-trigger', 'data', allow_duplicate=True),
    Input('submit-session-btn', 'n_clicks'),
    State('selected-project-id', 'data'),
    State('session-date', 'date'),
    State('session-duration', 'value'),
    State('session-output', 'value'),
    State('session-completed', 'value'),
    State('projects-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def log_session(n_clicks, project_id, date_val, duration, output, completed, refresh_count):
    if not n_clicks or not project_id:
        return dash.no_update, dash.no_update, dash.no_update
    
    is_completed = 'completed' in (completed or [])
    
    conn = get_connection()
    project = conn.execute("SELECT importance FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    
    importance = project['importance']
    
    log_project_session(project_id, date_val, duration, output or "", is_completed)
    
    xp = calculate_project_xp(duration, importance, is_completed)
    if output:
        xp += 20
    
    award_xp(date_val, 'projects', f'session_{project_id}', xp, notes=f"{duration}min session")
    
    feedback = dbc.Alert([
        html.I(className="fas fa-check-circle me-2"),
        html.Strong("Session logged! "),
        f"+{xp} XP",
        " complete." if is_completed else ""
    ], color="success", dismissable=True)
    
    return feedback, False, (refresh_count or 0) + 1

# Open edit modal
@callback(
    Output('edit-modal', 'is_open'),
    Output('edit-project-name', 'value'),
    Output('edit-project-category', 'value'),
    Output('edit-project-tier', 'value'),
    Output('edit-project-importance', 'value'),
    Output('edit-project-thesis', 'value'),
    Output('edit-project-status', 'value'),
    Output('selected-project-id', 'data', allow_duplicate=True),
    Input({'type': 'edit-project-btn', 'index': ALL}, 'n_clicks'),
    Input('cancel-edit-btn', 'n_clicks'),
    Input('save-edit-btn', 'n_clicks'),
    State('edit-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_edit_modal(edit_clicks, cancel_n, save_n, is_open):
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    triggered = ctx.triggered[0]
    triggered_id = triggered['prop_id']
    triggered_value = triggered.get('value')

    # Ignore synthetic triggers caused by dynamic list re-renders.
    if triggered_value is None or triggered_value == 0:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    if 'edit-project-btn' in triggered_id:
        import json
        button_id = json.loads(triggered_id.split('.')[0])
        project_id = button_id['index']
        
        conn = get_connection()
        project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        conn.close()
        
        tier_value = project['tier'] if project['tier'] is not None else 3
        return (True, project['name'], project['category'], tier_value,
                project['importance'], project['thesis'], project['status'], project_id)
    
    return False, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, None

# Save edited project
@callback(
    Output('edit-modal', 'is_open', allow_duplicate=True),
    Output('projects-refresh-trigger', 'data', allow_duplicate=True),
    Input('save-edit-btn', 'n_clicks'),
    State('selected-project-id', 'data'),
    State('edit-project-name', 'value'),
    State('edit-project-category', 'value'),
    State('edit-project-tier', 'value'),
    State('edit-project-importance', 'value'),
    State('edit-project-thesis', 'value'),
    State('edit-project-status', 'value'),
    State('projects-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def save_edited_project(n_clicks, project_id, name, category, tier, importance, thesis, status, refresh_count):
    if not n_clicks or not project_id:
        return dash.no_update, dash.no_update
    
    update_project(project_id, name, category, tier, importance, thesis, status)
    return False, (refresh_count or 0) + 1

# Open delete confirmation
@callback(
    Output('delete-modal', 'is_open'),
    Output('delete-confirm-text', 'children'),
    Output('delete-project-id', 'data'),
    Input({'type': 'delete-project-btn', 'index': ALL}, 'n_clicks'),
    Input('cancel-delete-btn', 'n_clicks'),
    Input('confirm-delete-btn', 'n_clicks'),
    State('delete-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_delete_modal(delete_clicks, cancel_n, confirm_n, is_open):
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update
    
    triggered = ctx.triggered[0]
    triggered_id = triggered['prop_id']
    triggered_value = triggered.get('value')

    # Ignore synthetic triggers caused by dynamic list re-renders.
    if triggered_value is None or triggered_value == 0:
        return dash.no_update, dash.no_update, dash.no_update
    
    if 'delete-project-btn' in triggered_id:
        import json
        button_id = json.loads(triggered_id.split('.')[0])
        project_id = button_id['index']
        
        conn = get_connection()
        project = conn.execute("SELECT name FROM projects WHERE id = ?", (project_id,)).fetchone()
        conn.close()
        
        return True, project['name'], project_id
    
    return False, dash.no_update, None

# Confirm delete
@callback(
    Output('delete-modal', 'is_open', allow_duplicate=True),
    Output('projects-refresh-trigger', 'data', allow_duplicate=True),
    Input('confirm-delete-btn', 'n_clicks'),
    State('delete-project-id', 'data'),
    State('projects-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def confirm_delete_project(n_clicks, project_id, refresh_count):
    if not n_clicks or not project_id:
        return dash.no_update, dash.no_update
    
    archive_project(project_id)
    return False, (refresh_count or 0) + 1

@callback(
    Output('projects-summary-row', 'children'),
    Output('projects-board', 'children'),
    Input('projects-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def refresh_projects_display(_refresh):
    """Refresh project summary cards and tier lists after project mutations."""
    if not ctx.triggered:
        return dash.no_update, dash.no_update
    return build_project_summary_row(), build_projects_board()
