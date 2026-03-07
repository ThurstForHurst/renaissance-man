"""
Insights & Pattern Discovery Page
"""
import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
from datetime import datetime
import plotly.graph_objs as go
import pandas as pd
from database.db import discover_correlations, get_connection

dash.register_page(__name__)

def layout():
    return dbc.Container([
        html.Div([
            html.H2("Insights & Patterns", className="mb-1"),
            html.P("Analyze cross-domain patterns and personal records.", className="mb-0"),
        ], className="app-page-head"),
        
        dbc.Row([
            dbc.Col([
                dbc.Button(
                    "🔍 Discover New Patterns",
                    id='discover-patterns-btn',
                    color="primary",
                    className="mb-3 w-100"
                )
            ], md=12)
        ]),
        
        # Pattern discoveries
        html.Div(id='pattern-discoveries'),
        
        # Correlation matrix
        dbc.Card([
            dbc.CardHeader(html.H4("Metric Correlations")),
            dbc.CardBody([
                dcc.Graph(id='correlation-heatmap')
            ])
        ], className="mb-4"),
        
        # Personal bests
        dbc.Card([
            dbc.CardHeader(html.H4("Personal Bests & Records")),
            dbc.CardBody(id='personal-bests')
        ], className="mb-4"),
        
        # Trends over time
        dbc.Card([
            dbc.CardHeader(html.H4("Multi-Domain Trends")),
            dbc.CardBody([
                dcc.Graph(id='trends-chart')
            ])
        ])
        
    ], fluid=True, className="app-page-shell")

@callback(
    Output('pattern-discoveries', 'children'),
    Input('discover-patterns-btn', 'n_clicks'),
    prevent_initial_call=True
)
def discover_patterns(n_clicks):
    """Run pattern discovery algorithm."""
    if not n_clicks:
        return dash.no_update
    
    patterns = discover_correlations()
    
    if not patterns:
        return dbc.Alert(
            "No significant patterns found yet. Keep logging data!",
            color="info"
        )
    
    # Create cards for each pattern
    pattern_cards = []
    for p in patterns:
        significance_color = {
            'strong': 'success',
            'moderate': 'warning',
            'weak': 'info'
        }.get(p['significance'], 'secondary')
        
        pattern_cards.append(
            dbc.Card([
                dbc.CardBody([
                    html.H5("📊 " + p['description']),
                    dbc.Badge(p['significance'].title(), color=significance_color),
                    html.Hr(),
                    html.P(f"Correlation: {p['correlation_value']:.3f}")
                ])
            ], className="mb-2")
        )
    
    return html.Div(pattern_cards)

@callback(
    Output('correlation-heatmap', 'figure'),
    Input('discover-patterns-btn', 'n_clicks')
)
def update_correlation_heatmap(n_clicks):
    """Create correlation heatmap."""
    conn = get_connection()
    
    # Get data for correlations
    query = """
        SELECT 
            s.date,
            s.duration_minutes / 60.0 as sleep_hours,
            s.energy as sleep_energy,
            s.sleep_quality as computed_sleep_quality,
            (SELECT SUM(duration_min) FROM exercise_cardio e WHERE e.date = s.date) as cardio_min,
            (SELECT SUM(reps * sets) FROM exercise_resistance r WHERE r.date = s.date AND r.exercise_type = 'pushups') as pushups,
            (SELECT SUM(duration_min) FROM project_sessions ps WHERE ps.date = s.date) as project_min
        FROM sleep_logs s
        WHERE s.date >= date('now', '-30 days')
        ORDER BY s.date DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if len(df) < 5:
        # Not enough data
        fig = go.Figure()
        fig.update_layout(
            title="Not enough data for correlation analysis",
            height=400
        )
        return fig
    
    # Fill NaN with 0 for exercises that weren't done
    df = df.fillna(0)
    
    # Calculate correlation matrix
    metrics = ['sleep_hours', 'sleep_energy', 'computed_sleep_quality', 'cardio_min', 'pushups', 'project_min']
    metric_labels = {
        'sleep_hours': 'Sleep Hours',
        'sleep_energy': 'Sleep Energy',
        'computed_sleep_quality': 'Computed Sleep Quality',
        'cardio_min': 'Cardio Minutes',
        'pushups': 'Push-ups',
        'project_min': 'Project Minutes'
    }
    df_metrics = df[metrics]
    corr_matrix = df_metrics.corr()
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=[metric_labels[m] for m in metrics],
        y=[metric_labels[m] for m in metrics],
        colorscale='RdBu',
        zmid=0,
        text=corr_matrix.values,
        texttemplate='%{text:.2f}',
        textfont={"size": 10},
        colorbar=dict(title="Correlation")
    ))
    
    fig.update_layout(
        title='Metric Correlations (Last 30 Days)',
        height=500,
        xaxis={'side': 'bottom'},
        yaxis={'autorange': 'reversed'}
    )
    
    return fig

@callback(
    Output('personal-bests', 'children'),
    Input('discover-patterns-btn', 'n_clicks')
)
def show_personal_bests(n_clicks):
    """Show personal bests and records."""
    conn = get_connection()
    
    records = []
    
    # Best sleep week
    best_sleep_week = conn.execute("""
        SELECT 
            date(date, 'weekday 0', '-7 days') as week_start,
            AVG(energy) as avg_energy,
            AVG(duration_minutes) / 60.0 as avg_hours
        FROM sleep_logs
        WHERE date >= date('now', '-90 days')
        GROUP BY week_start
        ORDER BY avg_energy DESC
        LIMIT 1
    """).fetchone()
    
    if best_sleep_week:
        records.append(
            dbc.ListGroupItem([
                html.H6("🌟 Best Energy Week"),
                html.P(f"Week of {best_sleep_week['week_start']}: {best_sleep_week['avg_energy']:.1f}/5 avg energy, {best_sleep_week['avg_hours']:.1f}h avg sleep")
            ])
        )
    
    # Most pushups in a day
    most_pushups = conn.execute("""
        SELECT date, SUM(reps * sets) as total_pushups
        FROM exercise_resistance
        WHERE exercise_type = 'pushups'
        GROUP BY date
        ORDER BY total_pushups DESC
        LIMIT 1
    """).fetchone()
    
    if most_pushups:
        records.append(
            dbc.ListGroupItem([
                html.H6("💪 Most Push-ups in a Day"),
                html.P(f"{most_pushups['date']}: {most_pushups['total_pushups']} push-ups")
            ])
        )
    
    # Longest cardio session
    longest_cardio = conn.execute("""
        SELECT date, type, duration_min, distance_km
        FROM exercise_cardio
        ORDER BY duration_min DESC
        LIMIT 1
    """).fetchone()
    
    if longest_cardio:
        records.append(
            dbc.ListGroupItem([
                html.H6("🏃 Longest Cardio Session"),
                html.P(f"{longest_cardio['date']}: {longest_cardio['duration_min']} min {longest_cardio['type']}" + 
                       (f", {longest_cardio['distance_km']:.1f} km" if longest_cardio['distance_km'] else ""))
            ])
        )
    
    # Most productive project week
    most_productive = conn.execute("""
        SELECT 
            date(date, 'weekday 0', '-7 days') as week_start,
            COUNT(*) as sessions,
            SUM(duration_min) as total_min
        FROM project_sessions
        WHERE date >= date('now', '-90 days')
        GROUP BY week_start
        ORDER BY sessions DESC
        LIMIT 1
    """).fetchone()
    
    if most_productive:
        records.append(
            dbc.ListGroupItem([
                html.H6("🚀 Most Productive Week"),
                html.P(f"Week of {most_productive['week_start']}: {most_productive['sessions']} sessions, {most_productive['total_min']} minutes")
            ])
        )
    
    # Current streaks
    sleep_streak = conn.execute("""
        WITH RECURSIVE streak_days(d, active) AS (
            SELECT
                date('now') AS d,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM sleep_logs s
                        WHERE s.date = date('now')
                    ) THEN 1 ELSE 0
                END AS active
            UNION ALL
            SELECT
                date(d, '-1 day') AS d,
                CASE
                    WHEN active = 1 AND EXISTS (
                        SELECT 1 FROM sleep_logs s
                        WHERE s.date = date(d, '-1 day')
                    ) THEN 1 ELSE 0
                END AS active
            FROM streak_days
            WHERE d > date('now', '-30 days') AND active = 1
        )
        SELECT COUNT(*) AS streak
        FROM streak_days
        WHERE active = 1
    """).fetchone()

    
    if sleep_streak and sleep_streak['streak'] > 0:
        records.append(
            dbc.ListGroupItem([
                html.H6("🔥 Current Sleep Log Streak"),
                html.P(f"{sleep_streak['streak']} consecutive days with a sleep log entry")
            ])
        )
    
    conn.close()
    
    if not records:
        return html.P("No records yet. Keep logging data!")
    
    return dbc.ListGroup(records)

@callback(
    Output('trends-chart', 'figure'),
    Input('discover-patterns-btn', 'n_clicks')
)
def update_trends_chart(n_clicks):
    """Show multi-domain trends over time."""
    conn = get_connection()
    
    # Get aggregated weekly data
    query = """
        WITH weeks AS (
            SELECT DISTINCT date(date, 'weekday 0', '-7 days') as week_start
            FROM sleep_logs
            WHERE date >= date('now', '-90 days')
        )
        SELECT 
            w.week_start,
            AVG(s.energy) as avg_energy,
            (SELECT SUM(duration_min) FROM exercise_cardio e 
             WHERE date(e.date, 'weekday 0', '-7 days') = w.week_start) as cardio_min,
            (SELECT COUNT(*) FROM project_sessions ps 
             WHERE date(ps.date, 'weekday 0', '-7 days') = w.week_start) as project_sessions
        FROM weeks w
        LEFT JOIN sleep_logs s ON date(s.date, 'weekday 0', '-7 days') = w.week_start
        GROUP BY w.week_start
        ORDER BY w.week_start
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if len(df) < 2:
        fig = go.Figure()
        fig.update_layout(title="Not enough data yet", height=400)
        return fig
    
    # Create multi-axis chart
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['week_start'],
        y=df['avg_energy'],
        name='Sleep Energy (1-5)',
        mode='lines+markers',
        line=dict(color='#3498db')
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start'],
        y=df['cardio_min'].fillna(0) / 30,  # Scale to similar range
        name='Cardio (min/30)',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color='#e74c3c')
    ))
    
    fig.add_trace(go.Scatter(
        x=df['week_start'],
        y=df['project_sessions'].fillna(0),
        name='Project Sessions',
        mode='lines+markers',
        yaxis='y3',
        line=dict(color='#2ecc71')
    ))
    
    fig.update_layout(
        title='Weekly Trends (Last 90 Days)',
        xaxis=dict(title='Week'),
        yaxis=dict(title='Sleep Energy', side='left'),
        yaxis2=dict(title='Cardio', overlaying='y', side='right'),
        yaxis3=dict(title='Sessions', overlaying='y', side='right', position=0.95),
        hovermode='x unified',
        height=500,
        showlegend=True
    )
    
    return fig

# Initialize displays on page load
@callback(
    Output('pattern-discoveries', 'children', allow_duplicate=True),
    Output('correlation-heatmap', 'figure', allow_duplicate=True),
    Output('personal-bests', 'children', allow_duplicate=True),
    Output('trends-chart', 'figure', allow_duplicate=True),
    Input('pattern-discoveries', 'children'),
    prevent_initial_call='initial_duplicate'
)
def init_insights(children):
    """Initialize all insights on page load."""
    # Trigger pattern discovery
    patterns = discover_correlations()
    
    pattern_display = html.P("Click 'Discover New Patterns' to analyze your data", className="text-muted")
    if patterns:
        pattern_cards = []
        for p in patterns:
            significance_color = {
                'strong': 'success',
                'moderate': 'warning',
                'weak': 'info'
            }.get(p['significance'], 'secondary')
            
            pattern_cards.append(
                dbc.Card([
                    dbc.CardBody([
                        html.H5("📊 " + p['description']),
                        dbc.Badge(p['significance'].title(), color=significance_color)
                    ])
                ], className="mb-2")
            )
        pattern_display = html.Div(pattern_cards)
    
    # Get other displays
    heatmap = update_correlation_heatmap(None)
    bests = show_personal_bests(None)
    trends = update_trends_chart(None)
    
    return pattern_display, heatmap, bests, trends
