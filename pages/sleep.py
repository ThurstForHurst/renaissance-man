"""
Sleep Tracking Page
"""
import dash
from dash import html, dcc, callback, Input, Output, State, ALL, ctx
import dash_bootstrap_components as dbc
from datetime import datetime, date, timedelta
import plotly.graph_objs as go
from database.db import log_sleep, get_sleep_logs, get_sleep_score, get_connection, get_brisbane_date
from analytics.scoring import award_xp, calculate_sleep_xp

dash.register_page(__name__)

def check_on_time(actual_time: str, target_time: str, tolerance_min: int = 30) -> bool:
    """Check if actual time is within tolerance of target on a 24h clock."""
    actual = datetime.strptime(actual_time, "%H:%M")
    target = datetime.strptime(target_time, "%H:%M")

    actual_min = actual.hour * 60 + actual.minute
    target_min = target.hour * 60 + target.minute

    raw_diff = abs(actual_min - target_min)
    circular_diff = min(raw_diff, 1440 - raw_diff)

    return circular_diff <= tolerance_min


def _score_color(score_value: float) -> str:
    """Map composite score to a display color."""
    if score_value >= 85:
        return "#198754"
    if score_value >= 70:
        return "#0d6efd"
    if score_value >= 55:
        return "#fd7e14"
    return "#dc3545"


def _build_sleep_trend_figure(sleep_df):
    """Build a polished 30-day sleep trend chart."""
    fig = go.Figure()

    if len(sleep_df) == 0:
        fig.update_layout(
            title='No sleep data yet',
            template='plotly_white',
            height=400,
            margin=dict(l=30, r=30, t=50, b=30)
        )
        return fig

    fig.add_trace(go.Scatter(
        x=sleep_df['date'],
        y=sleep_df['duration_minutes'] / 60,
        mode='lines+markers',
        name='Sleep Duration',
        line=dict(color='#0d6efd', width=3, shape='spline', smoothing=0.55),
        marker=dict(size=6, color='#0d6efd'),
        hovertemplate='Date: %{x|%b %d}<br>Duration: %{y:.2f} hrs<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=sleep_df['date'],
        y=sleep_df['energy'],
        mode='lines+markers',
        name='Energy',
        yaxis='y2',
        line=dict(color='#198754', width=2.5, shape='spline', smoothing=0.45),
        marker=dict(size=5, color='#198754'),
        hovertemplate='Date: %{x|%b %d}<br>Energy: %{y:.1f}/5<extra></extra>'
    ))

    fig.update_layout(
        title='Last 30 Days',
        template='plotly_white',
        height=400,
        margin=dict(l=36, r=36, t=54, b=36),
        hovermode='x unified',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='#ffffff',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        xaxis=dict(
            title='',
            type='date',
            tickformat='%b %d',
            hoverformat='%b %d',
            showgrid=True,
            gridcolor='rgba(100, 116, 139, 0.14)',
            zeroline=False
        ),
        yaxis=dict(
            title='Hours',
            rangemode='tozero',
            showgrid=True,
            gridcolor='rgba(100, 116, 139, 0.14)',
            zeroline=False
        ),
        yaxis2=dict(
            title='Energy',
            overlaying='y',
            side='right',
            range=[0, 5.5],
            showgrid=False,
            zeroline=False
        ),
    )

    return fig


def render_sleep_score_display(selected_date: str):
    """Render sleep score details plus a 7-day score strip (today + previous 6)."""
    if not selected_date:
        selected_date = get_brisbane_date()

    score = get_sleep_score(selected_date)
    today = date.fromisoformat(get_brisbane_date())
    strip_dates = [today - timedelta(days=offset) for offset in range(6, -1, -1)]

    day_boxes = []
    for day in strip_dates:
        day_iso = day.isoformat()
        day_score = get_sleep_score(day_iso)
        is_today = day == today
        has_entry = day_score is not None

        if has_entry:
            value_text = f"{int(round(day_score['composite']))}"
            box_bg = "#ffffff"
            border_color = _score_color(day_score["composite"])
            text_color = border_color
        else:
            value_text = "--"
            box_bg = "#e9ecef" if is_today else "#f8f9fa"
            border_color = "#ced4da"
            text_color = "#6c757d"

        border_tint = f"{border_color}99" if isinstance(border_color, str) and border_color.startswith("#") and len(border_color) == 7 else border_color
        box_style = {
            "width": "100%",
            "height": "88px",
            "borderRadius": "10px",
            "border": f"1.5px solid {border_tint}",
            "backgroundColor": box_bg,
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
            "justifyContent": "center",
            "fontWeight": "700",
            "color": text_color,
            "transition": "all 0.2s ease",
        }
        if is_today and has_entry:
            box_style["boxShadow"] = "0 10px 20px rgba(13, 110, 253, 0.25)"
            box_style["transform"] = "translateY(-2px)"
        if day_iso == selected_date:
            box_style["border"] = "3px solid #0d6efd"

        day_boxes.append(
            html.Button(
                [
                    html.Small(day.strftime("%a"), className="text-muted"),
                    html.Div(value_text),
                ],
                id={"type": "sleep-score-day", "date": day_iso},
                n_clicks=0,
                style=box_style,
                className="p-0",
            )
        )

    if score:
        composite = float(score["composite"])
        score_tone = _score_color(composite)
        score_label = "Excellent" if composite >= 85 else "Good" if composite >= 70 else "Fair" if composite >= 55 else "Needs Work"

        return html.Div(
            [
                html.Div(
                    day_boxes,
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "repeat(7, minmax(0, 1fr))",
                        "gap": "8px",
                        "width": "100%",
                    },
                    className="mb-3",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Small("Current Sleep Score", className="text-muted d-block"),
                                html.Div(
                                    [
                                        html.Span(f"{int(round(composite))}", style={"fontSize": "2rem", "fontWeight": "700", "lineHeight": "1"}),
                                        html.Span("/100", className="ms-1 text-muted"),
                                    ],
                                    style={"display": "flex", "alignItems": "baseline"},
                                ),
                                dbc.Badge(score_label, color="light", text_color="dark", class_name="mt-2"),
                            ]
                        ),
                        html.Div(
                            dbc.Progress(
                                value=composite,
                                style={"height": "10px", "borderRadius": "999px"},
                                color="success" if composite >= 85 else "info" if composite >= 70 else "warning" if composite >= 55 else "danger",
                            ),
                            className="mt-2",
                        ),
                    ],
                    style={
                        "padding": "14px 16px",
                        "borderRadius": "12px",
                        "border": f"1px solid {score_tone}33",
                        "boxShadow": "0 8px 20px rgba(0,0,0,0.08)",
                        "background": "linear-gradient(180deg, #ffffff 0%, #f8fbff 100%)",
                    },
                    className="mb-3",
                ),
                dbc.Row(
                    [
                        dbc.Col(html.Div([html.Small("Duration", className="text-muted d-block"), html.Strong(f"{int(round(score['duration_score']))}")]), xs=6, md=3),
                        dbc.Col(html.Div([html.Small("Continuity", className="text-muted d-block"), html.Strong(f"{int(round(score['continuity_score']))}")]), xs=6, md=3),
                        dbc.Col(html.Div([html.Small("Onset", className="text-muted d-block"), html.Strong(f"{int(round(score['onset_score']))}")]), xs=6, md=3),
                        dbc.Col(html.Div([html.Small("Timing", className="text-muted d-block"), html.Strong(f"{int(round(score['timing_score']))}")]), xs=6, md=3),
                        dbc.Col(html.Div([html.Small("Restoration", className="text-muted d-block"), html.Strong(f"{int(round(score['restoration_score']))}")]), xs=6, md=3, className="mt-2"),
                        dbc.Col(html.Div([html.Small("Energy", className="text-muted d-block"), html.Strong(f"{int(round(score['energy_score']))}")]), xs=6, md=3, className="mt-2"),
                        dbc.Col(html.Div([html.Small("Mood", className="text-muted d-block"), html.Strong(f"{int(round(score['mood_score']))}")]), xs=6, md=3, className="mt-2"),
                        dbc.Col(html.Div(), xs=6, md=3, className="mt-2"),
                    ],
                    className="g-2",
                ),
            ]
        )

    return html.Div(
        [
            html.Div(
                day_boxes,
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(7, minmax(0, 1fr))",
                    "gap": "8px",
                    "width": "100%",
                },
                className="mb-3",
            ),
            html.P("No score available yet", className="mb-0"),
        ]
    )


def render_sleep_summary_cards():
    """Render top sleep metric cards from current DB state."""
    today = get_brisbane_date()
    sleep_df_7 = get_sleep_logs(7)
    sleep_df_30 = get_sleep_logs(30)
    logged_dates = set(sleep_df_30['date']) if len(sleep_df_30) > 0 else set()

    avg_score_7 = round(float(sleep_df_7['sleep_quality'].mean()), 1) if len(sleep_df_7) > 0 else None
    avg_duration_min_7 = int(round(float(sleep_df_7['duration_minutes'].mean()))) if len(sleep_df_7) > 0 else None

    current_streak = 0
    cursor_day = date.fromisoformat(today)
    while cursor_day.isoformat() in logged_dates:
        current_streak += 1
        cursor_day -= timedelta(days=1)

    today_logged = today in logged_dates

    return dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Average Sleep Score", className="text-muted mb-2"),
                            html.H3(f"{avg_score_7}/100" if avg_score_7 is not None else "--", className="mb-0"),
                            html.Small("Last 7 days", className="text-muted"),
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
                            html.H6("Average Duration", className="text-muted mb-2"),
                            html.H3(
                                f"{avg_duration_min_7 // 60}h {avg_duration_min_7 % 60}m" if avg_duration_min_7 is not None else "--",
                                className="mb-0",
                            ),
                            html.Small("Last 7 days", className="text-muted"),
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
                            html.H6("Current Streak", className="text-muted mb-2"),
                            html.H3(f"{current_streak} days", className="mb-0"),
                            html.Small("Consecutive logged days", className="text-muted"),
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
                            html.H6("Today", className="text-muted mb-2"),
                            html.H3("Logged" if today_logged else "Pending", className=f"mb-0 {'text-success' if today_logged else 'text-muted'}"),
                            html.Small("Daily entry status", className="text-muted"),
                        ]
                    ),
                    className="shadow-sm summary-card",
                ),
                md=3,
            ),
        ],
        className="mb-4",
    )

def layout():
    today = get_brisbane_date()
    
    # Get target times from config
    conn = get_connection()
    config = conn.execute("""
        SELECT key, value FROM app_config 
        WHERE key IN ('target_bedtime', 'target_wake_time')
    """).fetchall()
    conn.close()
    
    target_bed = next((c['value'] for c in config if c['key'] == 'target_bedtime'), '22:30')
    target_wake = next((c['value'] for c in config if c['key'] == 'target_wake_time'), '06:30')

    return dbc.Container([
        html.Div([
            html.H2("Sleep Tracking", className="mb-1"),
            html.P("Track nightly sleep quality, consistency, and recovery signals.", className="mb-0"),
        ], className="app-page-head"),
        dcc.Store(id='sleep-score-selected-date', data=today),
        html.Div(id='sleep-summary-row', children=render_sleep_summary_cards()),
        
        dbc.Row([
            # Input Form
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H4("Log Sleep", className="mb-0"),
                        html.Small("Capture tonight's timing and how you felt on waking.", className="text-muted")
                    ], style={"background": "linear-gradient(180deg, #ffffff 0%, #f8fbff 100%)"}),
                    dbc.CardBody([
                        html.Div([
                            html.H6("Timing", className="mb-2"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Date", className="small text-muted mb-1"),
                                    dcc.DatePickerSingle(
                                        id='sleep-date',
                                        date=today,
                                        display_format='YYYY-MM-DD',
                                        className="mb-0"
                                    ),
                                ], md=4),
                                dbc.Col([
                                    dbc.Label(f"Bedtime (Target {target_bed})", className="small text-muted mb-1"),
                                    dbc.Input(
                                        id='sleep-bedtime',
                                        type='time',
                                        value=target_bed,
                                        className="mb-0"
                                    ),
                                ], md=4),
                                dbc.Col([
                                    dbc.Label(f"Wake Time (Target {target_wake})", className="small text-muted mb-1"),
                                    dbc.Input(
                                        id='sleep-waketime',
                                        type='time',
                                        value=target_wake,
                                        className="mb-0"
                                    ),
                                ], md=4),
                            ], className="g-2"),
                        ], className="p-3 mb-3", style={"border": "1px solid #dbe7f6", "borderRadius": "12px", "background": "#fbfdff"}),

                        html.Div([
                            html.H6("How You Felt", className="mb-2"),
                            dbc.Label("Wake Mood", className="small text-muted"),
                            dcc.Slider(
                                id='sleep-wake-mood',
                                min=1, max=5, step=1, value=3,
                                marks={i: str(i) for i in range(1, 6)},
                                className="mb-3"
                            ),
                            dbc.Label("Energy Level", className="small text-muted"),
                            dcc.Slider(
                                id='sleep-energy',
                                min=1, max=5, step=1, value=3,
                                marks={i: str(i) for i in range(1, 6)},
                                className="mb-3"
                            ),
                            dbc.Label("How Rested Do You Feel?", className="small text-muted"),
                            dcc.Slider(
                                id='sleep-rested',
                                min=1, max=5, step=1, value=3,
                                marks={
                                    1: 'Exhausted',
                                    2: 'Tired',
                                    3: 'Okay',
                                    4: 'Good',
                                    5: 'Fully Rested'
                                },
                                className="mb-0"
                            ),
                        ], className="p-3 mb-3", style={"border": "1px solid #dbe7f6", "borderRadius": "12px", "background": "#fbfdff"}),

                        html.Div([
                            html.H6("Night Details", className="mb-2"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Times Woken During Night", className="small text-muted mb-1"),
                                    dcc.Dropdown(
                                        id='sleep-wakings',
                                        options=[
                                            {'label': '0 times', 'value': 0},
                                            {'label': '1 time', 'value': 1},
                                            {'label': '2 times', 'value': 2},
                                            {'label': '3 times', 'value': 3},
                                            {'label': '4+ times', 'value': 4},
                                        ],
                                        value=0,
                                        clearable=False,
                                        className="mb-0"
                                    ),
                                ], md=4),
                                dbc.Col([
                                    dbc.Label("Falling Asleep Was...", className="small text-muted mb-1"),
                                    dcc.Dropdown(
                                        id='sleep-onset',
                                        options=[
                                            {'label': 'Easy', 'value': 'easy'},
                                            {'label': 'Normal', 'value': 'normal'},
                                            {'label': 'Difficult', 'value': 'difficult'},
                                        ],
                                        value='normal',
                                        clearable=False,
                                        className="mb-0"
                                    ),
                                ], md=4),
                                dbc.Col([
                                    dbc.Label("Wake Method", className="small text-muted mb-1"),
                                    dcc.Dropdown(
                                        id='sleep-wake-method',
                                        options=[
                                            {'label': 'Before 6:30 AM', 'value': 'before_630'},
                                            {'label': 'On time', 'value': 'on_time'},
                                            {'label': 'Slept in', 'value': 'slept_in'},
                                        ],
                                        value='on_time',
                                        clearable=False,
                                        className="mb-0"
                                    ),
                                ], md=4),
                            ], className="g-2"),
                        ], className="p-3 mb-3", style={"border": "1px solid #dbe7f6", "borderRadius": "12px", "background": "#fbfdff"}),

                        dbc.Label("Notes (optional)", className="small text-muted mb-1"),
                        dbc.Textarea(
                            id='sleep-notes',
                            placeholder="Any notes about last night's sleep...",
                            className="mb-3",
                            style={"minHeight": "90px"}
                        ),

                        dbc.Button(
                            "Log Sleep",
                            id='sleep-submit',
                            color="primary",
                            className="w-100",
                            style={"height": "44px", "fontWeight": "600", "boxShadow": "0 8px 18px rgba(13,110,253,0.22)"}
                        ),

                        html.Div(id='sleep-feedback', className="mt-3")
                    ], style={"background": "linear-gradient(180deg, #ffffff 0%, #f8fbff 100%)"})
                ], className="shadow-sm", style={"borderRadius": "14px", "border": "1px solid #dbe7f6"})
            ], md=6),
            
            # Visualization
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Sleep Trends")),
                    dbc.CardBody([
                        dcc.Graph(id='sleep-chart')
                    ])
                ], className="mb-3"),
                
                dbc.Card([
                    dbc.CardHeader(html.H4("Sleep Score")),
                    dbc.CardBody(id='sleep-score-display', children=render_sleep_score_display(today))
                ])
            ], md=6)
        ])
        
    ], fluid=True, className="app-page-shell")

@callback(
    Output('sleep-feedback', 'children'),
    Output('sleep-chart', 'figure'),
    Output('sleep-score-display', 'children'),
    Output('sleep-summary-row', 'children'),
    Input('sleep-submit', 'n_clicks'),
    State('sleep-date', 'date'),
    State('sleep-bedtime', 'value'),
    State('sleep-waketime', 'value'),
    State('sleep-wake-mood', 'value'),
    State('sleep-energy', 'value'),
    State('sleep-wakings', 'value'),
    State('sleep-onset', 'value'),
    State('sleep-wake-method', 'value'),
    State('sleep-rested', 'value'),
    State('sleep-notes', 'value'),
    State('sleep-score-selected-date', 'data'),
    prevent_initial_call=True
)
def log_sleep_entry(n_clicks, date_val, bedtime, waketime, wake_mood, energy, 
                    wakings_count, sleep_onset, wake_method, rested_level, notes, selected_score_date):
    """Log sleep entry and award XP."""
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    # Get targets
    conn = get_connection()
    config = conn.execute("""
        SELECT key, value FROM app_config 
        WHERE key IN ('target_bedtime', 'target_wake_time')
    """).fetchall()
    conn.close()
    
    target_bed = next((c['value'] for c in config if c['key'] == 'target_bedtime'), '22:30')
    target_wake = next((c['value'] for c in config if c['key'] == 'target_wake_time'), '06:30')
    
    # Sleep quality is computed from measurable inputs in the DB layer.
    duration = log_sleep(
        date=date_val,
        bedtime=bedtime,
        wake_time=waketime,
        wake_mood=wake_mood,
        energy=energy,
        wakings_count=wakings_count or 0,
        sleep_onset=sleep_onset or 'normal',
        wake_method=wake_method or 'on_time',
        rested_level=rested_level or 3,
        notes=notes or ""
    )
    
    # Calculate and award XP
    bedtime_on_time = check_on_time(bedtime, target_bed)
    wake_on_time = check_on_time(waketime, target_wake)
    
    xp = calculate_sleep_xp(bedtime_on_time, wake_on_time)
    
    if xp > 0:
        conn = get_connection()
        existing = conn.execute(
            """
            SELECT id
            FROM xp_logs
            WHERE domain = ? AND activity = ?
            LIMIT 1
            """,
            ("sleep", f"sleep_log_{date_val}")
        ).fetchone()
        conn.close()
        if not existing:
            award_xp(date_val, 'sleep', f'sleep_log_{date_val}', xp, notes=f"Bed: {bedtime}, Wake: {waketime}")
    
    # Create feedback message
    feedback = dbc.Alert([
        html.H5("Sleep Logged Successfully!", className="alert-heading"),
        html.P(f"Duration: {duration // 60}h {duration % 60}m"),
        html.P(f"XP Earned: +{xp} XP"),
        html.Hr(),
        html.P([
            "[On time] Bedtime on time" if bedtime_on_time else "[Late] Bedtime late",
            html.Br(),
            "[On time] Wake time on time" if wake_on_time else "[Off target] Wake time off"
        ])
    ], color="success")
    
    # Update chart
    sleep_df = get_sleep_logs(30)
    fig = _build_sleep_trend_figure(sleep_df)
    
    # Update sleep score panel.
    score_display = render_sleep_score_display(selected_score_date or date_val)
    
    return feedback, fig, score_display, render_sleep_summary_cards()


@callback(
    Output('sleep-score-selected-date', 'data'),
    Input({'type': 'sleep-score-day', 'date': ALL}, 'n_clicks'),
    Input('sleep-date', 'date'),
    State('sleep-score-selected-date', 'data'),
    prevent_initial_call=True
)
def update_selected_sleep_score_date(day_clicks, picker_date, current_selected_date):
    """Update selected score date via day-strip click or date picker change."""
    if not ctx.triggered:
        return current_selected_date

    triggered = ctx.triggered[0]
    prop_id = triggered.get('prop_id', '')
    value = triggered.get('value')

    if prop_id.startswith('sleep-date.') and picker_date:
        return picker_date

    if isinstance(value, list):
        if not any(v for v in value if v):
            return current_selected_date
    elif not value:
        return current_selected_date

    triggered_id = ctx.triggered_id
    if isinstance(triggered_id, dict) and triggered_id.get('type') == 'sleep-score-day':
        return triggered_id.get('date') or current_selected_date

    return current_selected_date


@callback(
    Output('sleep-score-display', 'children', allow_duplicate=True),
    Input('sleep-score-selected-date', 'data'),
    prevent_initial_call='initial_duplicate'
)
def refresh_sleep_score_display(selected_date):
    """Refresh sleep score panel when selected date changes."""
    return render_sleep_score_display(selected_date)

# Initial chart render
@callback(
    Output('sleep-chart', 'figure', allow_duplicate=True),
    Input('sleep-date', 'date'),
    prevent_initial_call='initial_duplicate'
)
def update_chart(date_val):
    """Update chart on page load."""
    sleep_df = get_sleep_logs(30)
    return _build_sleep_trend_figure(sleep_df)

