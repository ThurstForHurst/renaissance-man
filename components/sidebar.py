"""
Sidebar component for daily routines
"""
import dash
from dash import html, dcc, callback, Input, Output, State, ctx, ALL
import dash_bootstrap_components as dbc
from datetime import datetime, date, time, timezone, timedelta
from database.db import (
    get_routine_templates, get_routine_items, get_today_routine_progress,
    update_routine_item_progress, submit_routine, is_routine_submitted,
    get_connection, get_brisbane_date, generate_daily_quests,
    get_daily_quests, complete_quest, uncomplete_quest,
    get_all_tasks, add_task, set_task_completed, clear_completed_tasks
)

SIDEBAR_OPEN_WIDTH_PX = 320
SIDEBAR_COLLAPSED_WIDTH_PX = 44

def is_routine_available(time_available: str) -> bool:
    """Check if a routine is currently available based on time."""
    try:
        # Brisbane timezone (UTC+10, no DST)
        brisbane_tz = timezone(timedelta(hours=10))
        now = datetime.now(brisbane_tz).time()
        
        # Handle both HH:MM and HH:MM:SS formats
        if len(time_available.split(':')) == 2:
            available_time = datetime.strptime(time_available, '%H:%M').time()
        else:
            available_time = datetime.strptime(time_available, '%H:%M:%S').time()
        
        return now >= available_time
    except Exception as e:
        print(f"Error parsing time '{time_available}': {e}")
        return True  # Default to available if there's an error

def create_routine_item_checkbox(item: dict, checked: bool, parent_checked: bool = True):
    """Create a checkbox for a routine item."""
    is_sub_item = item['parent_item_id'] is not None
    
    return html.Div([
        dbc.Checkbox(
            id={'type': 'routine-item-check', 'item': item['id']},
            label=f"{item['item_text']} (+{item['xp_value']} XP)",
            value=checked,
            disabled=False,
            className="sidebar-routine-check mb-1" if not is_sub_item else "sidebar-routine-check sidebar-routine-subcheck mb-1 ms-4 small"
        )
    ])

def create_routine_card(template: dict, items: list, progress: dict, submitted: bool):
    """Create a card for a routine."""
    available = is_routine_available(template['time_available'])
    
    # Separate parent and child items
    parent_items = [item for item in items if item['parent_item_id'] is None]
    
    # Check if all items completed
    all_completed = all(progress.get(item['id'], False) for item in items)
    
    # Calculate current XP
    current_xp = sum(item['xp_value'] for item in items if progress.get(item['id'], False))
    if all_completed:
        current_xp += template['bonus_xp']
    
    if not available:
        # Show unavailable state - MUST have feedback div for consistency
        return dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.H6(f"{template['name']} Routine", className="text-muted"),
                    html.Small(f"Available at {template['time_available']}", className="text-muted")
                ]),
                html.Div(id={'type': 'routine-feedback', 'template': template['id']}, className="mt-2")
            ])
        ], className="mb-2 opacity-50 sidebar-routine-card")
    
    if submitted:
        # Show submitted state (can unsubmit before midnight)
        return dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.H6(f"{template['name']} Routine (Submitted)", className="text-success"),
                    html.Small(f"Submitted! {current_xp} XP (awarded at midnight)", className="text-muted d-block mb-2"),
                    dbc.Button(
                        "Unsubmit",
                        id={'type': 'unsubmit-routine', 'template': template['id']},
                        color="warning",
                        size="sm",
                        outline=True,
                        className="w-100"
                    ),
                    html.Div(id={'type': 'routine-feedback', 'template': template['id']}, className="mt-2")
                ])
            ])
        ], className="mb-2 border-success sidebar-routine-card")
    
    # Build checklist
    checklist_items = []
    for parent in parent_items:
        parent_checked = progress.get(parent['id'], False)
        checklist_items.append(
            create_routine_item_checkbox(parent, parent_checked)
        )
        
        # Add child items if this parent has any
        child_items = [item for item in items if item['parent_item_id'] == parent['id']]
        if child_items:
            for child in child_items:
                child_checked = progress.get(child['id'], False)
                checklist_items.append(
                    create_routine_item_checkbox(child, child_checked, parent_checked)
                )
    
    return dbc.Card([
        dbc.CardBody([
            html.H6(f"{template['name']} Routine", className="mb-2"),
            html.Div(checklist_items),
            html.Hr(className="my-2"),
            html.Div([
                html.Small(f"Current: {current_xp} XP", className="text-muted me-3"),
                html.Small(f"Bonus: +{template['bonus_xp']} XP (all items)", className="text-success")
            ], className="mb-2"),
            dbc.Button(
                "Submit Routine",
                id={'type': 'submit-routine', 'template': template['id']},
                color="primary",
                size="sm",
                className="w-100"
            ),
            html.Div(id={'type': 'routine-feedback', 'template': template['id']}, className="mt-2")
        ])
    ], className="mb-2 shadow-sm sidebar-routine-card")

def create_quest_checkbox(quest: dict):
    """Create a checkbox for a quest."""
    difficulty_colors = {
        'easy': 'success',
        'medium': 'warning',
        'hard': 'danger'
    }
    color = difficulty_colors.get((quest.get('difficulty') or '').lower(), 'secondary')

    return dbc.Card([
        dbc.CardBody([
            dbc.Checkbox(
                id={'type': 'sidebar-quest-check', 'quest': quest['id']},
                label=html.Span([
                    html.Span(quest['quest_text'], className="d-block"),
                    html.Small([
                        f"+{quest['xp_reward']} XP ",
                        dbc.Badge((quest.get('difficulty') or 'n/a').title(), color=color, className="ms-1")
                    ], className="text-muted")
                ]),
                value=bool(quest.get('completed')),
                className="mb-0 small"
            )
        ], className="py-2")
    ], className="mb-2 sidebar-quest-card")


def create_sidebar_task_checkbox(task: dict):
    """Create a compact sidebar checkbox for project tasks."""
    return dbc.Card([
        dbc.CardBody([
            dbc.Checkbox(
                id={'type': 'sidebar-task-check', 'task': task['id']},
                label=task['text'],
                value=bool(task.get('completed')),
                className="sidebar-task-check mb-0 small"
            )
        ], className="py-2")
    ], className="mb-2 sidebar-task-card")


def get_open_routine_count(today: str) -> int:
    """Count available routines that are not yet submitted for today."""
    templates = get_routine_templates()
    open_count = 0
    for template in templates:
        if not is_routine_available(template['time_available']):
            continue
        if not is_routine_submitted(today, template['id']):
            open_count += 1
    return open_count

def render_sidebar():
    """Render the sidebar with all routines."""
    today = get_brisbane_date()
    generate_daily_quests(today)
    quests = get_daily_quests(today)
    tasks = get_all_tasks()
    templates = get_routine_templates()
    
    routine_cards = []
    for template in templates:
        items = get_routine_items(template['id'])
        progress = get_today_routine_progress(today, template['id'])
        submitted = is_routine_submitted(today, template['id'])
        
        routine_cards.append(
            create_routine_card(template, items, progress, submitted)
        )
    
    return html.Div([
        # Sidebar content (visible when open)
        html.Div([
            html.Div([
                html.Div([
                    html.H5("Daily Routines", className="mb-1"),
                    html.Small("Track routines and quests", className="text-muted d-block"),
                    html.Div(routine_cards, className="sidebar-stack mt-2"),
                    dbc.Button(
                        "Update Routines",
                        href="/routines",
                        color="dark",
                        size="sm",
                        outline=True,
                        className="w-100 sidebar-manage-routines-btn"
                    ),
                ], className="sidebar-section"),
                html.Div([
                    html.H6("Daily Quests", className="mb-1"),
                    html.Div([create_quest_checkbox(q) for q in quests], className="sidebar-stack mt-2"),
                ], className="sidebar-section"),
                html.Div([
                    html.H6("Project Tasks", className="mb-1"),
                    html.Small(f"{len(tasks)} tracked", className="text-muted d-block"),
                    html.Div(
                        [create_sidebar_task_checkbox(t) for t in tasks] if tasks else [
                            html.Small("No tasks yet.", className="text-muted d-block mt-2")
                        ],
                        className="sidebar-stack mt-2"
                    ),
                    dbc.InputGroup([
                        dbc.Input(
                            id='sidebar-task-input',
                            placeholder="Quick add task...",
                            size="sm"
                        ),
                        dbc.Button(
                            "Add",
                            id='sidebar-task-add-btn',
                            color="dark",
                            size="sm"
                        ),
                    ], className="sidebar-task-input-group")
                ], className="sidebar-section")
            ], style={'padding': '1rem'})
        ], id='sidebar-content', className='sidebar-content-shell'),
        
        # Toggle button
        html.Div([
            dbc.Button(
                "<<",
                id='sidebar-toggle',
                color="light",
                size="sm",
                className="border-0 sidebar-toggle-btn",
                style={
                    'position': 'absolute',
                    'right': '5px',
                    'top': '10px',
                    'fontSize': '1.2rem',
                    'padding': '0.25rem 0.5rem',
                    'zIndex': '1001'
                }
            )
        ])
    ], style={
        'position': 'fixed',
        'top': '100px',
        'left': '0',
        'width': f'{SIDEBAR_OPEN_WIDTH_PX}px',
        'height': 'calc(100vh - 100px)',
        'backgroundColor': '#f8f9fa',
        'borderRight': '1px solid #dee2e6',
        'overflowY': 'auto',
        'zIndex': '1000',
        'transition': 'all 0.3s ease'
    }, id='routines-sidebar', className='routines-sidebar-shell open')

def render_collapsed_sidebar():
    """Render collapsed sidebar (just the toggle button)."""
    # Get all templates to create hidden feedback divs
    templates = get_routine_templates()
    today = get_brisbane_date()
    open_routines = get_open_routine_count(today)
    generate_daily_quests(today)
    quests = get_daily_quests(today)
    
    # Create hidden feedback divs to maintain callback consistency
    hidden_feedback = [
        html.Div(id={'type': 'routine-feedback', 'template': t['id']}, style={'display': 'none'})
        for t in templates
    ]
    hidden_quests = [
        dbc.Checkbox(
            id={'type': 'sidebar-quest-check', 'quest': q['id']},
            value=bool(q.get('completed')),
            style={'display': 'none'}
        )
        for q in quests
    ]
    
    return html.Div([
        dbc.Button(
            [
                ">>",
                html.Span(
                    str(open_routines),
                    className='sidebar-open-count'
                ) if open_routines > 0 else None
            ],
            id='sidebar-toggle',
            color="light",
            size="sm",
            className="border sidebar-collapsed-toggle",
            style={
                'fontSize': '1.2rem',
                'padding': '0.5rem',
                'width': '100%',
                'position': 'relative',
                'overflow': 'visible'
            }
        ),
        # Hidden feedback divs so callback outputs match
        *hidden_feedback,
        *hidden_quests
    ], style={
        'position': 'fixed',
        'top': '100px',
        'left': '0',
        'width': f'{SIDEBAR_COLLAPSED_WIDTH_PX}px',
        'height': 'calc(100vh - 100px)',
        'backgroundColor': '#f8f9fa',
        'borderRight': '1px solid #dee2e6',
        'zIndex': '1200',
        'overflow': 'visible',
        'transition': 'all 0.3s ease'
    }, id='routines-sidebar', className='routines-sidebar-shell collapsed')

# Toggle sidebar
@callback(
    Output('sidebar-container', 'children'),
    Output('sidebar-state', 'data'),
    Output('main-content', 'style'),
    Input('sidebar-toggle', 'n_clicks'),
    State('sidebar-state', 'data'),
    prevent_initial_call=True
)
def toggle_sidebar(n_clicks, is_open):
    """Toggle sidebar open/closed."""
    if n_clicks is None:
        return dash.no_update, dash.no_update, dash.no_update
    
    new_state = not is_open
    
    if new_state:
        # Open
        return render_sidebar(), True, {'marginLeft': f'{SIDEBAR_OPEN_WIDTH_PX}px', 'width': f'calc(100% - {SIDEBAR_OPEN_WIDTH_PX}px)', 'paddingTop': '24px', 'paddingBottom': '3rem', 'transition': 'all 0.3s ease'}
    else:
        # Closed
        return render_collapsed_sidebar(), False, {'marginLeft': f'{SIDEBAR_COLLAPSED_WIDTH_PX}px', 'width': f'calc(100% - {SIDEBAR_COLLAPSED_WIDTH_PX}px)', 'paddingTop': '24px', 'paddingBottom': '3rem', 'transition': 'all 0.3s ease'}

# Handle checkbox changes
@callback(
    Output({'type': 'routine-item-check', 'item': ALL}, 'value', allow_duplicate=True),
    Input({'type': 'routine-item-check', 'item': ALL}, 'value'),
    State({'type': 'routine-item-check', 'item': ALL}, 'id'),
    prevent_initial_call=True
)
def handle_item_check(values, ids):
    """Handle checking/unchecking routine items."""
    if not ctx.triggered:
        return values
    
    today = get_brisbane_date()
    
    # Get which item was clicked
    triggered_id = ctx.triggered[0]['prop_id']
    
    # Parse the triggered ID to get the item_id
    import json
    try:
        triggered_dict = json.loads(triggered_id.split('.')[0])
        triggered_item_id = triggered_dict['item']
    except:
        return values
    
    # Update database for the triggered item
    for i, item_id_dict in enumerate(ids):
        if item_id_dict['item'] == triggered_item_id:
            item_id = item_id_dict['item']
            
            # Get the template_id for this item
            conn = get_connection()
            item = conn.execute("SELECT template_id, parent_item_id FROM routine_items WHERE id = ?", (item_id,)).fetchone()
            conn.close()
            
            if item:
                update_routine_item_progress(today, item['template_id'], item_id, values[i])
                
                # If this is a parent item being unchecked, uncheck all children
                if not values[i] and item['parent_item_id'] is None:
                    conn = get_connection()
                    children = conn.execute("""
                        SELECT id FROM routine_items WHERE parent_item_id = ? AND active = 1
                    """, (item_id,)).fetchall()
                    conn.close()
                    
                    for child in children:
                        update_routine_item_progress(today, item['template_id'], child['id'], False)
                        # Update the values array for children
                        for j, child_id_dict in enumerate(ids):
                            if child_id_dict['item'] == child['id']:
                                values[j] = False
            break
    
    return values

@callback(
    Output({'type': 'routine-feedback', 'template': ALL}, 'children'),
    Output('sidebar-container', 'children', allow_duplicate=True),
    Input({'type': 'submit-routine', 'template': ALL}, 'n_clicks'),
    Input({'type': 'unsubmit-routine', 'template': ALL}, 'n_clicks'),
    State('sidebar-state', 'data'),
    prevent_initial_call=True
)
def handle_routine_submission(submit_clicks, unsubmit_clicks, is_open):
    """Handle routine submission and unsubmission."""
    from database.db import unsubmit_routine, get_brisbane_date
    
    # Get ALL templates to ensure correct number of outputs
    templates = get_routine_templates()
    all_template_ids = [t['id'] for t in templates]
    
    if not ctx.triggered:
        if is_open:
            return [None] * len(all_template_ids), render_sidebar()
        else:
            return [None] * len(all_template_ids), render_collapsed_sidebar()
    
    # Check if this is a real click (not None or 0)
    triggered = ctx.triggered[0]
    triggered_value = triggered.get('value')
    
    # Guard against false triggers from page refresh/sidebar render
    if triggered_value is None or triggered_value == 0:
        if is_open:
            return [None] * len(all_template_ids), render_sidebar()
        else:
            return [None] * len(all_template_ids), render_collapsed_sidebar()
    
    # Find which button was clicked
    triggered_id = triggered['prop_id']
    
    import json
    button_data = json.loads(triggered_id.split('.')[0])
    template_id = button_data['template']
    
    today = get_brisbane_date()
    
    # Handle unsubmit
    if 'unsubmit-routine' in triggered_id:
        unsubmit_routine(today, template_id)
        
        feedback = [
            dbc.Alert("Routine unsubmitted", color="info", dismissable=True, duration=3000) 
            if tid == template_id else None 
            for tid in all_template_ids
        ]
        return feedback, render_sidebar() if is_open else render_collapsed_sidebar()
    
    # Handle submit
    if 'submit-routine' in triggered_id:
        # Check if already submitted
        if is_routine_submitted(today, template_id):
            feedback = [
                dbc.Alert("Already submitted today!", color="warning", dismissable=True) 
                if tid == template_id else None 
                for tid in all_template_ids
            ]
            return feedback, render_sidebar() if is_open else render_collapsed_sidebar()
        
        # Submit (XP will be awarded at midnight)
        total_xp = submit_routine(today, template_id)
        
        # Create feedback
        feedback = [
            dbc.Alert(
                f"Routine submitted! {total_xp} XP will be awarded at midnight",
                color="success",
                dismissable=True,
                duration=4000
            ) if tid == template_id else None 
            for tid in all_template_ids
        ]
        
        return feedback, render_sidebar() if is_open else render_collapsed_sidebar()
    
    # Default
    return [None] * len(all_template_ids), render_sidebar() if is_open else render_collapsed_sidebar()

@callback(
    Output({'type': 'sidebar-quest-check', 'quest': ALL}, 'value', allow_duplicate=True),
    Output('quest-sync-signal', 'data', allow_duplicate=True),
    Input({'type': 'sidebar-quest-check', 'quest': ALL}, 'value'),
    State({'type': 'sidebar-quest-check', 'quest': ALL}, 'id'),
    prevent_initial_call=True
)
def handle_quest_check(values, ids):
    """Handle quest check-offs from the sidebar and apply/rollback XP."""
    if not ctx.triggered:
        return values, dash.no_update

    triggered = ctx.triggered[0]
    triggered_value = triggered.get('value')
    if triggered_value is None:
        return values, dash.no_update

    import json
    try:
        triggered_dict = json.loads(triggered['prop_id'].split('.')[0])
        triggered_quest_id = triggered_dict['quest']
    except Exception:
        return values, dash.no_update

    updated_checked = None
    for i, item in enumerate(ids):
        if item['quest'] == triggered_quest_id:
            if values[i]:
                complete_quest(triggered_quest_id)
            else:
                uncomplete_quest(triggered_quest_id)
            updated_checked = bool(values[i])
            break

    if updated_checked is None:
        return values, dash.no_update

    from datetime import datetime, timezone
    return values, {
        'quest_id': triggered_quest_id,
        'checked': updated_checked,
        'ts': datetime.now(timezone.utc).isoformat()
    }


# Auto-refresh sidebar every minute
@callback(
    Output('sidebar-container', 'children', allow_duplicate=True),
    Output('sidebar-refresh-signature', 'data'),
    Output('task-sync-signal', 'data', allow_duplicate=True),
    Input('sidebar-refresh-interval', 'n_intervals'),
    State('sidebar-state', 'data'),
    State('sidebar-refresh-signature', 'data'),
    prevent_initial_call=True
)
def refresh_sidebar(n, is_open, current_signature):
    """Refresh sidebar to update routine availability and award pending XP."""
    from database.db import (
        award_pending_routine_xp, generate_daily_quests, get_brisbane_date,
        get_routine_templates, submit_journal_entries_before
    )
    
    # Award XP for any submitted routines from previous days
    award_pending_routine_xp()
    today = get_brisbane_date()
    generate_daily_quests(today)
    previous_day = (current_signature or "").split("|")[0] if current_signature else None
    task_sync_payload = dash.no_update
    if previous_day and previous_day != today:
        clear_completed_tasks()
        submit_journal_entries_before(today)
        from datetime import datetime, timezone
        task_sync_payload = {'event': 'midnight_task_cleanup', 'ts': datetime.now(timezone.utc).isoformat()}

    # Only re-render when day or routine availability state changes.
    templates = get_routine_templates()
    availability_signature = "|".join(
        f"{t['id']}:{int(is_routine_available(t['time_available']))}"
        for t in templates
    )
    new_signature = f"{today}|{availability_signature}"

    if new_signature == (current_signature or ""):
        return dash.no_update, current_signature, task_sync_payload

    if is_open:
        return render_sidebar(), new_signature, task_sync_payload
    return render_collapsed_sidebar(), new_signature, task_sync_payload


@callback(
    Output('sidebar-container', 'children', allow_duplicate=True),
    Output('sidebar-task-input', 'value'),
    Output('task-sync-signal', 'data', allow_duplicate=True),
    Input('sidebar-task-add-btn', 'n_clicks'),
    Input('sidebar-task-input', 'n_submit'),
    Input({'type': 'sidebar-task-check', 'task': ALL}, 'value'),
    State('sidebar-task-input', 'value'),
    State({'type': 'sidebar-task-check', 'task': ALL}, 'id'),
    State('sidebar-state', 'data'),
    prevent_initial_call=True
)
def handle_sidebar_tasks(add_clicks, submit_add, check_values, new_task_text, check_ids, is_open):
    """Handle add and toggle actions for sidebar project tasks."""
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    triggered = ctx.triggered[0]
    triggered_id = triggered['prop_id']
    triggered_value = triggered.get('value')
    if triggered_value is None:
        return dash.no_update, dash.no_update, dash.no_update

    cleared_input = dash.no_update
    sync_payload = dash.no_update

    if 'sidebar-task-add-btn' in triggered_id or 'sidebar-task-input.n_submit' in triggered_id:
        text = (new_task_text or "").strip()
        if text:
            add_task(text)
            cleared_input = ""
            from datetime import datetime, timezone
            sync_payload = {'event': 'task_added', 'ts': datetime.now(timezone.utc).isoformat()}
        else:
            return dash.no_update, dash.no_update, dash.no_update
    elif 'sidebar-task-check' in triggered_id:
        import json
        try:
            task_data = json.loads(triggered_id.split('.')[0])
            task_id = task_data['task']
            checked_state = None
            for i, item in enumerate(check_ids or []):
                if item.get('task') == task_id:
                    checked_state = bool(check_values[i])
                    break
            if checked_state is None:
                return dash.no_update, dash.no_update, dash.no_update
            set_task_completed(task_id, checked_state)
            from datetime import datetime, timezone
            sync_payload = {
                'event': 'task_checked',
                'task_id': task_id,
                'checked': checked_state,
                'ts': datetime.now(timezone.utc).isoformat()
            }
        except Exception:
            return dash.no_update, dash.no_update, dash.no_update

    return (render_sidebar() if is_open else render_collapsed_sidebar()), cleared_input, sync_payload


@callback(
    Output('sidebar-container', 'children', allow_duplicate=True),
    Input('task-sync-signal', 'data'),
    State('sidebar-state', 'data'),
    prevent_initial_call=True
)
def sync_sidebar_tasks(sync_signal, is_open):
    """Refresh sidebar task section when tasks change elsewhere."""
    if not sync_signal:
        return dash.no_update
    return render_sidebar() if is_open else render_collapsed_sidebar()
