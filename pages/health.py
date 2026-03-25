"""
Health Tracking Page
"""
import dash
from dash import html, dcc, callback, Input, Output, State, ALL
import dash_bootstrap_components as dbc
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go
from database.db import (
    log_resistance, log_resistance_workout, log_cardio, log_daily_health, get_connection,
    get_exercise_summary, get_health_summary, log_weight, get_brisbane_date,
    get_weight_trend, get_exercise_trend, get_recent_daily_health_logs
)
from analytics.scoring import award_xp, calculate_resistance_xp, calculate_cardio_xp

dash.register_page(__name__)

RESISTANCE_OPTIONS = [
    {'label': 'Push-ups', 'value': 'pushups'},
    {'label': 'Pull-ups', 'value': 'pullups'},
    {'label': 'Squats', 'value': 'squats'},
    {'label': 'Plank (seconds)', 'value': 'plank'},
    {'label': 'Sit-ups', 'value': 'situps'},
    {'label': 'Dips', 'value': 'dips'},
    {'label': 'Lunges', 'value': 'lunges'},
]

def render_health_summary_cards(health_summary: dict | None = None, exercise_summary: dict | None = None):
    """Build top summary cards from latest DB values."""
    health_summary = health_summary or get_health_summary()
    exercise_summary = exercise_summary or get_exercise_summary(days=7)
    latest_date = health_summary.get('latest_weight_date')
    latest_date_label = latest_date if latest_date else "--"

    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Weight", className="text-muted mb-2"),
                    html.H3(f"{health_summary['latest_weight']:.1f} kg" if health_summary['latest_weight'] else "--", className="mb-0"),
                    html.Small(latest_date_label, className="text-muted")
                ])
            ], className="shadow-sm summary-card h-100 health-metric-card")
        ], md=6, lg=3),

        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Avg Calories", className="text-muted mb-2"),
                    html.H3(f"{health_summary['avg_calories']:.0f}" if health_summary['avg_calories'] else "--", className="mb-0"),
                    html.Small("Last 7 days", className="text-muted")
                ])
            ], className="shadow-sm summary-card h-100 health-metric-card")
        ], md=6, lg=3),

        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Cardio", className="text-muted mb-2"),
                    html.H3(f"{exercise_summary['cardio_minutes']:.0f} min", className="mb-0"),
                    html.Small("This week", className="text-muted")
                ])
            ], className="shadow-sm summary-card h-100 health-metric-card")
        ], md=6, lg=3),

        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Total Reps", className="text-muted mb-2"),
                    html.H3(f"{sum(exercise_summary['resistance'].values()):.0f}", className="mb-0"),
                    html.Small("This week", className="text-muted")
                ])
            ], className="shadow-sm summary-card h-100 health-metric-card")
        ], md=6, lg=3),
    ], className="mb-4 g-3")

def create_weight_trend_figure(days: int = 90, df: pd.DataFrame | None = None):
    """Create weight-over-time line chart."""
    df = df if df is not None else get_weight_trend(days=days)
    fig = go.Figure()
    if df.empty:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="white",
            plot_bgcolor="white",
            margin=dict(l=24, r=24, t=44, b=24),
            height=320,
            title="Weight Trend",
            annotations=[dict(text="No weight entries yet", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)]
        )
        return fig

    df['date'] = pd.to_datetime(df['date'])
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['weight_kg'],
        mode='lines+markers',
        name='Weight',
        line=dict(color='#2563eb', width=3),
        marker=dict(size=6, color='#1d4ed8', line=dict(width=1, color='white'))
    ))
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title="Weight Trend",
        xaxis_title="Date",
        yaxis_title="Weight (kg)",
        hovermode="x unified",
        margin=dict(l=24, r=24, t=44, b=24),
        height=320,
        showlegend=False
    )
    fig.update_xaxes(
        tickformat="%b %d",
        showline=True,
        linewidth=1,
        linecolor="#d0d7e2",
        gridcolor="#eef2f7"
    )
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor="#d0d7e2",
        gridcolor="#eef2f7",
        zeroline=False
    )
    return fig

def create_exercise_trend_figure(days: int = 30, df: pd.DataFrame | None = None):
    """Create daily resistance reps bars plus cardio line chart."""
    df = df if df is not None else get_exercise_trend(days=days)
    fig = go.Figure()
    if df.empty:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="white",
            plot_bgcolor="white",
            margin=dict(l=24, r=24, t=44, b=24),
            height=360,
            title="Exercise Trend (Reps + Cardio)",
            annotations=[dict(text="No exercise entries yet", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)]
        )
        return fig

    df['date'] = pd.to_datetime(df['date'])
    one_day_ms = 24 * 60 * 60 * 1000
    cardio_by_date = (
        df.groupby('date', as_index=False)['cardio_min']
        .max()
        .sort_values('date')
    )

    resistance_rows = df[df['exercise_type'].notna() & (df['total_reps'] > 0)].copy()
    if not resistance_rows.empty:
        resistance_by_date = (
            resistance_rows.groupby('date', as_index=False)['total_reps']
            .sum()
            .sort_values('date')
        )
        fig.add_trace(go.Bar(
            x=resistance_by_date['date'],
            y=resistance_by_date['total_reps'],
            name='Resistance Reps',
            marker_color='#2563eb',
            width=one_day_ms * 0.72,
        ))

    fig.add_trace(go.Scatter(
        x=cardio_by_date['date'],
        y=cardio_by_date['cardio_min'],
        name='Cardio Minutes',
        mode='lines+markers',
        line=dict(color='#0f172a', width=2.5),
        marker=dict(size=5, color='#0f172a'),
        yaxis='y2'
    ))

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title="Exercise Trend (Reps + Cardio)",
        xaxis_title="Date",
        yaxis=dict(title="Resistance Reps"),
        yaxis2=dict(title="Cardio Minutes", overlaying="y", side="right", rangemode='tozero'),
        hovermode="x unified",
        margin=dict(l=24, r=24, t=44, b=24),
        height=360,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0)
    )
    fig.update_xaxes(
        tickformat="%b %d",
        showline=True,
        linewidth=1,
        linecolor="#d0d7e2",
        gridcolor="#eef2f7"
    )
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor="#d0d7e2",
        gridcolor="#eef2f7",
        zeroline=False
    )
    return fig

def build_recent_diet_entries(limit: int = 5):
    """Render recent diet and macro-tag entries."""
    recent = get_recent_daily_health_logs(limit)

    if not recent:
        return html.Small("No diet entries yet.", className="text-muted")

    recent_display = []
    for entry in recent:
        chips = []
        if entry['steps']:
            chips.append(dbc.Badge(f"Steps: {entry['steps']}", color="light", text_color="dark", className="border"))
        if entry['calories']:
            chips.append(dbc.Badge(f"Calories: {entry['calories']}", color="light", text_color="dark", className="border"))
        if entry['water_liters']:
            chips.append(dbc.Badge(f"Water: {entry['water_liters']}L", color="light", text_color="dark", className="border"))
        if entry['high_protein']:
            chips.append(dbc.Badge("High Protein", color="light", text_color="dark", className="border"))
        if entry['high_carbs']:
            chips.append(dbc.Badge("High Carbs", color="light", text_color="dark", className="border"))
        if entry['high_fat']:
            chips.append(dbc.Badge("High Fat", color="light", text_color="dark", className="border"))

        recent_display.append(
            dbc.Card([
                dbc.CardBody([
                    html.Small(entry['date'], className="text-muted d-block mb-1"),
                    html.Div(
                        chips if chips else [html.Small("No metrics logged", className="text-muted")],
                        className="d-flex flex-wrap gap-2"
                    )
                ])
            ], className="mb-2")
        )

    return recent_display

def build_exercise_compass_panel(
    summary_7: dict | None = None,
    trend_30: pd.DataFrame | None = None,
):
    """Render an always-populated exercise guidance panel."""
    summary_7 = summary_7 or get_exercise_summary(7)
    trend_30 = trend_30 if trend_30 is not None else get_exercise_trend(30)

    cardio_minutes = float(summary_7.get('cardio_minutes') or 0)
    resistance_total = float(sum((summary_7.get('resistance') or {}).values()) or 0)
    cardio_goal = 150
    cardio_pct = min(100, (cardio_minutes / cardio_goal) * 100) if cardio_goal else 0

    active_days = 0
    resistance_days = 0
    top_movement = "None yet"

    if not trend_30.empty:
        active_rows = trend_30[(trend_30['cardio_min'] > 0) | (trend_30['total_reps'] > 0)]
        if not active_rows.empty:
            active_days = int(active_rows['date'].nunique())

        resistance_rows = trend_30[(trend_30['exercise_type'].notna()) & (trend_30['total_reps'] > 0)]
        if not resistance_rows.empty:
            resistance_days = int(resistance_rows['date'].nunique())
            grouped = (
                resistance_rows.groupby('exercise_type', as_index=False)['total_reps']
                .sum()
                .sort_values('total_reps', ascending=False)
            )
            if not grouped.empty:
                top_movement = str(grouped.iloc[0]['exercise_type']).replace('_', ' ').title()

    resistance_goal_days = 3
    resistance_pct = min(100, (resistance_days / resistance_goal_days) * 100) if resistance_goal_days else 0

    return html.Div([
        html.Small("Weekly momentum and next actions.", className="text-muted d-block mb-3"),
        html.Small(f"Cardio Goal ({int(cardio_minutes)}/{cardio_goal} min)", className="text-muted d-block"),
        dbc.Progress(value=cardio_pct, color="dark", style={"height": "10px"}, className="mb-2"),
        html.Small(f"Resistance Days ({resistance_days}/{resistance_goal_days})", className="text-muted d-block"),
        dbc.Progress(value=resistance_pct, color="primary", style={"height": "10px"}, className="mb-3"),
        dbc.Row([
            dbc.Col(
                dbc.Card(dbc.CardBody([
                    html.Small("Top Movement", className="text-muted d-block"),
                    html.Strong(top_movement),
                ], className="py-2"), className="border"),
                md=6,
                className="mb-2"
            ),
            dbc.Col(
                dbc.Card(dbc.CardBody([
                    html.Small("Active Days (30d)", className="text-muted d-block"),
                    html.Strong(str(active_days)),
                ], className="py-2"), className="border"),
                md=6,
                className="mb-2"
            ),
        ], className="g-2"),
        html.Small(f"Total Reps (7d): {int(resistance_total)}", className="text-muted d-block mt-1 mb-2"),
        html.Hr(className="my-2"),
        html.Small("Next best actions", className="text-muted d-block mb-1"),
        html.Ul([
            html.Li("Log 20+ minutes of cardio today."),
            html.Li("Complete one resistance session with meaningful total reps."),
            html.Li("Add a short note after sessions for better insights."),
        ], className="small ps-3 mb-0")
    ], className="health-recent-feed")


def render_weekly_exercise_snapshot_cards(exercise_summary: dict | None = None):
    """Render the weekly snapshot cards using live exercise summary values."""
    exercise_summary = exercise_summary or get_exercise_summary(days=7)
    return dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.Small("Cardio Minutes", className="text-muted d-block"),
                            html.Strong(f"{exercise_summary['cardio_minutes']:.0f} min"),
                        ],
                        className="py-2",
                    ),
                    className="border",
                ),
                md=6,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.Small("Resistance Reps", className="text-muted d-block"),
                            html.Strong(f"{sum(exercise_summary['resistance'].values()):.0f}"),
                        ],
                        className="py-2",
                    ),
                    className="border",
                ),
                md=6,
            ),
        ],
        className="g-2 mb-3",
    )

def layout():
    today = get_brisbane_date()

    return dbc.Container([
        html.Div([
            html.H2("Health Tracking", className="mb-1"),
            html.P("Log training, body metrics, and daily wellness fundamentals.", className="mb-0"),
        ], className="app-page-head"),
        dcc.Store(id='health-refresh-trigger', data=0),
        html.Div(id='health-summary-row', children=dbc.Spinner(size="sm")),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Training Log", className="mb-0"),
                        html.Small("Resistance and cardio sessions", className="text-muted")
                    ], className="bg-white"),
                    dbc.CardBody([
                        dbc.Tabs([
                            dbc.Tab(label="Resistance", tab_id="resistance", children=[
                                html.Div([
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Date"),
                                            html.Div(
                                                dcc.DatePickerSingle(
                                                    id='resistance-date',
                                                    date=today,
                                                    display_format='DD-MM-YYYY',
                                                ),
                                                className="mb-3"
                                            ),
                                        ], md=4),
                                        dbc.Col([
                                            dbc.Label("Exercises"),
                                            dcc.Dropdown(
                                                id='resistance-types',
                                                options=RESISTANCE_OPTIONS,
                                                value=['pushups'],
                                                multi=True,
                                                className="mb-3"
                                            ),
                                        ], md=8),
                                    ], className="g-2"),
                                    html.Div(id='resistance-exercise-inputs', className="mb-2"),
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Workout Duration (minutes)"),
                                            dbc.Input(
                                                id='resistance-duration',
                                                type='number',
                                                value=30,
                                                min=1,
                                                className="mb-3"
                                            ),
                                        ], md=4),
                                        dbc.Col([
                                            dbc.Label("Notes"),
                                            dbc.Textarea(
                                                id='resistance-notes',
                                                placeholder="Optional notes...",
                                                rows=2,
                                                className="mb-3"
                                            ),
                                        ], md=8),
                                    ], className="g-2"),
                                    dbc.Button(
                                        "Log Resistance Session",
                                        id='resistance-submit',
                                        color="dark",
                                        className="w-100"
                                    ),
                                    html.Div(id='resistance-feedback', className="mt-3")
                                ], className="pt-3")
                            ]),
                            dbc.Tab(label="Cardio", tab_id="cardio", children=[
                                html.Div([
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Date"),
                                            html.Div(
                                                dcc.DatePickerSingle(
                                                    id='cardio-date',
                                                    date=today,
                                                    display_format='DD-MM-YYYY',
                                                ),
                                                className="mb-3"
                                            ),
                                        ], md=4),
                                        dbc.Col([
                                            dbc.Label("Type"),
                                            dcc.Dropdown(
                                                id='cardio-type',
                                                options=[
                                                    {'label': 'Run', 'value': 'run'},
                                                    {'label': 'Cycle', 'value': 'cycle'},
                                                    {'label': 'Swim', 'value': 'swim'},
                                                    {'label': 'HIIT', 'value': 'hiit'},
                                                    {'label': 'Walk', 'value': 'walk'},
                                                ],
                                                value='run',
                                                className="mb-3"
                                            ),
                                        ], md=8),
                                    ], className="g-2"),
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Duration (min)"),
                                            dbc.Input(
                                                id='cardio-duration',
                                                type='number',
                                                value=30,
                                                min=1,
                                                className="mb-3"
                                            ),
                                        ], md=4),
                                        dbc.Col([
                                            dbc.Label("Distance (km)"),
                                            dbc.Input(
                                                id='cardio-distance',
                                                type='number',
                                                value=5,
                                                min=0,
                                                step=0.1,
                                                className="mb-3"
                                            ),
                                        ], md=4),
                                        dbc.Col([
                                            dbc.Label("Intensity"),
                                            dbc.Checklist(
                                                id='cardio-intense',
                                                options=[{'label': ' Intense workout', 'value': 'intense'}],
                                                value=[],
                                                className="mb-3 pt-2"
                                            ),
                                        ], md=4),
                                    ], className="g-2"),
                                    dbc.Label("Notes"),
                                    dbc.Textarea(
                                        id='cardio-notes',
                                        placeholder="Route, pace, etc...",
                                        rows=2,
                                        className="mb-3"
                                    ),
                                    dbc.Button(
                                        "Log Cardio Session",
                                        id='cardio-submit',
                                        color="dark",
                                        className="w-100"
                                    ),
                                    html.Div(id='cardio-feedback', className="mt-3")
                                ], className="pt-3")
                            ]),
                        ], id="health-tabs", active_tab="resistance"),
                        html.Div([
                            html.Hr(className="my-4"),
                            html.H6("Weekly Exercise Snapshot", className="mb-2"),
                            html.Div(
                                id='health-weekly-snapshot',
                                children=html.Small("Loading snapshot...", className="text-muted")
                            ),
                            html.Div([
                                dbc.Badge("150+ min cardio / week", color="light", text_color="dark", className="border"),
                                dbc.Badge("3+ resistance sessions / week", color="light", text_color="dark", className="border"),
                                dbc.Badge("Progress > perfect", color="light", text_color="dark", className="border"),
                            ], className="d-flex flex-wrap gap-2 mb-3"),
                            html.H6("Exercise Compass", className="mb-1"),
                            html.Div(
                                id='recent-exercise-entries',
                                children=html.Small("Loading exercise compass...", className="text-muted")
                            )
                        ], className="health-insight-strip")
                    ])
                ], className="shadow-sm border-0 health-panel health-log-card")
            ], lg=8, className="mb-3"),
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H6("Weight Tracking", className="mb-0"),
                            html.Small("Keep your weight trend current", className="text-muted")
                        ], className="bg-white"),
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Date"),
                                    html.Div(
                                        dcc.DatePickerSingle(
                                            id='weight-date',
                                            date=today,
                                            display_format='YYYY-MM-DD',
                                        ),
                                        className="mb-3"
                                    ),
                                ], md=12),
                                dbc.Col([
                                    dbc.Label("Weight (kg)"),
                                    dbc.Input(
                                        id='weight-value',
                                        type='number',
                                        min=20,
                                        max=400,
                                        step=0.1,
                                        placeholder="e.g. 78.4",
                                        className="mb-3"
                                    ),
                                ], md=12),
                                dbc.Col([
                                    dbc.Label("Notes"),
                                    dbc.Input(
                                        id='weight-notes',
                                        type='text',
                                        placeholder="Optional",
                                        className="mb-3"
                                    ),
                                ], md=12),
                            ], className="g-2"),
                            dbc.Button(
                                "Save Weight",
                                id='weight-submit',
                                color="dark",
                                className="w-100"
                            ),
                            html.Div(id='weight-feedback', className="mt-3")
                        ])
                    ], className="shadow-sm border-0 health-panel"),
                    dbc.Card([
                        dbc.CardHeader([
                            html.H6("Diet and Wellness", className="mb-0"),
                            html.Small("Daily nutrition and recovery markers", className="text-muted")
                        ], className="bg-white"),
                        dbc.CardBody([
                            dbc.Label("Date"),
                            html.Div(
                                dcc.DatePickerSingle(
                                    id='diet-date',
                                    date=today,
                                    display_format='YYYY-MM-DD',
                                ),
                                className="mb-3"
                            ),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Steps"),
                                    dbc.Input(
                                        id='diet-steps',
                                        type='number',
                                        placeholder="Daily steps",
                                        min=0,
                                        className="mb-3"
                                    ),
                                ], md=6),
                                dbc.Col([
                                    dbc.Label("Calories"),
                                    dbc.Input(
                                        id='diet-calories',
                                        type='number',
                                        placeholder="Total calories",
                                        min=0,
                                        className="mb-3"
                                    ),
                                ], md=6),
                            ], className="g-2"),
                            dbc.Label("Water (liters)"),
                            dbc.Input(
                                id='diet-water',
                                type='number',
                                placeholder="Water intake",
                                min=0,
                                step=0.1,
                                className="mb-3"
                            ),
                            dbc.Label("Macro Profile"),
                            dbc.Checklist(
                                id='diet-macros',
                                options=[
                                    {'label': ' High protein', 'value': 'high_protein'},
                                    {'label': ' High carbs', 'value': 'high_carbs'},
                                    {'label': ' High fat', 'value': 'high_fat'},
                                ],
                                value=[],
                                className="mb-3"
                            ),
                            dbc.Label("Notes"),
                            dbc.Textarea(
                                id='diet-notes',
                                placeholder="Meals, energy levels, etc.",
                                rows=3,
                                className="mb-3"
                            ),
                            dbc.Button(
                                "Log Wellness Entry",
                                id='diet-submit',
                                color="dark",
                                className="w-100"
                            ),
                            html.Div(id='diet-feedback', className="mt-3"),
                            html.Hr(className="my-4"),
                            html.H6("Daily Targets", className="mb-2"),
                            html.Div([
                                dbc.Badge("10,000+ steps", color="light", text_color="dark", className="border"),
                                dbc.Badge("Calories tracked", color="light", text_color="dark", className="border"),
                                dbc.Badge("2.0L+ water", color="light", text_color="dark", className="border"),
                            ], className="d-flex flex-wrap gap-2 mb-3"),
                            html.H6("Recent Entries", className="mb-2"),
                            html.Div(id='recent-diet-entries', children=build_recent_diet_entries())
                        ])
                    ], className="shadow-sm border-0 health-panel")
                ], className="health-side-stack")
            ], lg=4, className="mb-3"),
        ], className="g-3 align-items-start"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Exercise Trend", className="mb-0"),
                        html.Small("Stacked reps by movement + cardio line", className="text-muted")
                    ], className="bg-white"),
                    dbc.CardBody([
                        dcc.Graph(id='exercise-trends-chart', figure=go.Figure())
                    ])
                ], className="shadow-sm border-0 health-panel")
            ], md=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Weight Trend", className="mb-0"),
                        html.Small("Daily bodyweight trend", className="text-muted")
                    ], className="bg-white"),
                    dbc.CardBody([
                        dcc.Graph(id='weight-trend-chart', figure=go.Figure())
                    ])
                ], className="shadow-sm border-0 health-panel")
            ], md=6),
        ], className="g-3 mt-1")
    ], fluid=True, className="app-page-shell")


# Resistance callbacks
@callback(
    Output('resistance-exercise-inputs', 'children'),
    Input('resistance-types', 'value'),
)
def render_resistance_inputs(selected_types):
    selected_types = selected_types or []
    if not selected_types:
        return html.Small("Select at least one movement.", className="text-muted")
    rows = []
    for ex in selected_types:
        label = str(ex).replace('_', ' ').title()
        default_reps = 60 if ex == 'plank' else 20
        rows.append(
            dbc.Row(
                [
                    dbc.Col(html.Small(label, className="text-muted d-block pt-2"), md=5),
                    dbc.Col(
                        dbc.Input(
                            id={'type': 'resistance-reps', 'exercise': ex},
                            type='number',
                            value=default_reps,
                            min=1,
                            placeholder='Total reps',
                        ),
                        md=7,
                    ),
                ],
                className="g-2 mb-2",
            )
        )
    return rows


@callback(
    Output('resistance-feedback', 'children'),
    Output('health-refresh-trigger', 'data', allow_duplicate=True),
    Input('resistance-submit', 'n_clicks'),
    State('resistance-date', 'date'),
    State('resistance-types', 'value'),
    State({'type': 'resistance-reps', 'exercise': ALL}, 'id'),
    State({'type': 'resistance-reps', 'exercise': ALL}, 'value'),
    State('resistance-duration', 'value'),
    State('resistance-notes', 'value'),
    State('health-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def log_resistance_entry(
    n_clicks,
    date_val,
    selected_types,
    reps_ids,
    reps_values,
    duration_min,
    notes,
    refresh_count,
):
    """Log a multi-exercise resistance workout and award XP."""
    if not n_clicks:
        return dash.no_update, dash.no_update

    selected_types = selected_types or []
    reps_by_ex = {}
    for rid, rval in zip(reps_ids or [], reps_values or []):
        ex = (rid or {}).get('exercise')
        if ex:
            reps_by_ex[ex] = int(rval or 0)

    exercises = []
    total_reps = 0
    xp = 0
    for ex in selected_types:
        reps = int(reps_by_ex.get(ex) or 0)
        if reps > 0:
            exercises.append({"exercise_type": ex, "reps": reps, "sets": 1})
            total_reps += reps
            xp += calculate_resistance_xp(ex, reps, 1, 3)

    if not exercises:
        return dbc.Alert("Add at least one valid exercise with total reps.", color="warning", dismissable=True), refresh_count

    log_resistance_workout(date_val, exercises, int(duration_min or 0), 3, notes or "")
    award_xp(
        date_val,
        'health',
        f'resistance_workout_{datetime.utcnow().isoformat()}',
        int(xp),
        notes=f"duration={int(duration_min or 0)}m | exercises={len(exercises)}",
    )

    names = ", ".join(str(e["exercise_type"]).replace('_', ' ') for e in exercises[:3])
    if len(exercises) > 3:
        names += ", ..."
    feedback = dbc.Alert([
        html.I(className="fas fa-check-circle me-2"),
        html.Strong(f"Workout logged ({len(exercises)} exercises, {int(duration_min or 0)} min). "),
        f"{total_reps} total reps | +{int(xp)} XP",
        html.Br(),
        html.Small(names, className="text-muted"),
    ], color="success", dismissable=True, duration=4000)

    return feedback, (refresh_count or 0) + 1

# Cardio callbacks
@callback(
    Output('cardio-feedback', 'children'),
    Output('health-refresh-trigger', 'data', allow_duplicate=True),
    Input('cardio-submit', 'n_clicks'),
    State('cardio-date', 'date'),
    State('cardio-type', 'value'),
    State('cardio-duration', 'value'),
    State('cardio-distance', 'value'),
    State('cardio-intense', 'value'),
    State('cardio-notes', 'value'),
    State('health-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def log_cardio_entry(n_clicks, date_val, cardio_type, duration, distance, intense, notes, refresh_count):
    """Log cardio and award XP."""
    if not n_clicks:
        return dash.no_update, dash.no_update
    
    is_intense = 'intense' in (intense or [])
    
    # Log to database
    log_cardio(date_val, cardio_type, duration, distance, is_intense, notes or "")
    
    # Calculate and award XP
    xp = calculate_cardio_xp(duration, is_intense, distance)
    award_xp(date_val, 'health', f'cardio_{cardio_type}', xp,
             notes=f"{duration}min {cardio_type}")
    
    # Feedback
    pace_info = ""
    if distance and distance > 0:
        pace = duration / distance
        pace_info = f" | {pace:.2f} min/km"
    
    feedback = dbc.Alert([
        html.I(className="fas fa-check-circle me-2"),
        html.Strong(f"{duration}min {cardio_type} logged! "),
        f"+{xp} XP{pace_info}"
    ], color="success", dismissable=True, duration=4000)
    
    return feedback, (refresh_count or 0) + 1

# Diet logging callback
@callback(
    Output('diet-feedback', 'children'),
    Output('recent-diet-entries', 'children'),
    Output('health-refresh-trigger', 'data', allow_duplicate=True),
    Input('diet-submit', 'n_clicks'),
    State('diet-date', 'date'),
    State('diet-steps', 'value'),
    State('diet-calories', 'value'),
    State('diet-water', 'value'),
    State('diet-macros', 'value'),
    State('diet-notes', 'value'),
    State('health-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def log_diet_entry(n_clicks, date_val, steps, calories, water, macros, notes, refresh_count):
    """Log daily health metrics."""
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update

    selected_macros = set(macros or [])
    if not any([steps, calories, water, selected_macros]):
        feedback = dbc.Alert([
            html.I(className="fas fa-exclamation-triangle me-2"),
            "Please enter at least one metric or macro profile"
        ], color="warning", dismissable=True)
        return feedback, dash.no_update, refresh_count

    # Log to database
    log_daily_health(
        date_val,
        steps,
        calories,
        water,
        notes or "",
        high_protein='high_protein' in selected_macros,
        high_carbs='high_carbs' in selected_macros,
        high_fat='high_fat' in selected_macros
    )
    
    # Award XP based on completeness
    xp = 0
    if steps and steps >= 10000:
        xp += 50  # Bonus for hitting 10k steps
    if calories:
        xp += 25  # Base XP for tracking calories
    if water and water >= 2.0:
        xp += 25  # Bonus for good hydration
    
    if xp > 0:
        conn = get_connection()
        existing = conn.execute(
            """
            SELECT id
            FROM xp_logs
            WHERE domain = ? AND activity = ?
            LIMIT 1
            """,
            ("health", f"daily_tracking_{date_val}")
        ).fetchone()
        conn.close()
        if not existing:
            award_xp(
                date_val,
                'health',
                f'daily_tracking_{date_val}',
                xp,
                notes=f"Steps: {steps or 0}, Cals: {calories or 0}"
            )
    
    feedback = dbc.Alert([
        html.I(className="fas fa-check-circle me-2"),
        html.Strong("Entry logged! "),
        f"+{xp} XP" if xp > 0 else ""
    ], color="success", dismissable=True, duration=4000)
    
    return feedback, build_recent_diet_entries(), (refresh_count or 0) + 1


@callback(
    Output('weight-feedback', 'children'),
    Output('health-refresh-trigger', 'data', allow_duplicate=True),
    Output('quest-sync-signal', 'data', allow_duplicate=True),
    Input('weight-submit', 'n_clicks'),
    State('weight-date', 'date'),
    State('weight-value', 'value'),
    State('weight-notes', 'value'),
    State('health-refresh-trigger', 'data'),
    prevent_initial_call=True
)
def log_weight_entry(n_clicks, date_val, weight_val, notes, refresh_count):
    """Log bodyweight and trigger summary refresh."""
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update

    if not weight_val or weight_val <= 0:
        feedback = dbc.Alert(
            [html.I(className="fas fa-exclamation-triangle me-2"), "Please enter a valid weight."],
            color="warning",
            dismissable=True
        )
        return feedback, refresh_count, dash.no_update

    log_weight(date_val, float(weight_val), notes or "")
    feedback = dbc.Alert(
        [html.I(className="fas fa-check-circle me-2"), html.Strong("Weight saved."), f" {float(weight_val):.1f} kg"],
        color="success",
        dismissable=True,
        duration=3500
    )

    return feedback, (refresh_count or 0) + 1, {'event': 'weight_logged', 'ts': datetime.utcnow().isoformat()}


@callback(
    Output('health-summary-row', 'children'),
    Output('health-weekly-snapshot', 'children'),
    Output('weight-trend-chart', 'figure'),
    Output('exercise-trends-chart', 'figure'),
    Output('recent-exercise-entries', 'children'),
    Input('health-refresh-trigger', 'data'),
    prevent_initial_call=False
)
def refresh_health_reporting(_refresh):
    """Refresh summary and trend charts after new logs."""
    health_summary = get_health_summary()
    exercise_summary = get_exercise_summary(days=7)
    weight_trend = get_weight_trend(days=90)
    exercise_trend = get_exercise_trend(days=30)

    return (
        render_health_summary_cards(health_summary, exercise_summary),
        render_weekly_exercise_snapshot_cards(exercise_summary),
        create_weight_trend_figure(90, weight_trend),
        create_exercise_trend_figure(30, exercise_trend),
        build_exercise_compass_panel(exercise_summary, exercise_trend),
    )
