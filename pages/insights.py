"""Insights page."""
from datetime import date, timedelta

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
from dash import Input, Output, State, callback, dcc, html
from plotly.subplots import make_subplots

from database.db import discover_correlations, get_brisbane_date, get_connection

dash.register_page(__name__)

METRIC_LABELS = {
    "sleep_hours": "Sleep Hours",
    "sleep_energy": "Sleep Energy",
    "sleep_quality": "Sleep Quality",
    "cardio_min": "Cardio Minutes",
    "resistance_reps": "Resistance Reps",
    "project_min": "Project Minutes",
}

CHART_MARGIN = dict(l=24, r=24, t=56, b=24)


def _brisbane_today() -> date:
    return date.fromisoformat(get_brisbane_date())


def _cutoff_iso(days_back: int) -> str:
    return (_brisbane_today() - timedelta(days=days_back)).isoformat()


def _query_dataframe(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    rows = conn.execute(query, params).fetchall()
    conn.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(row) for row in rows])


def _empty_figure(title: str, message: str, height: int = 380) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title=title,
        margin=CHART_MARGIN,
        height=height,
        annotations=[
            dict(
                text=message,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color="#64748b"),
            )
        ],
    )
    return fig


def _panel(title: str, subtitle: str, body, class_name: str = ""):
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, className="insights-kicker"),
                html.H4(subtitle, className="insights-panel-title"),
                body,
            ]
        ),
        className=f"insights-panel {class_name}".strip(),
    )


def _metric_card(label: str, value: str, detail: str, tone: str = "default"):
    return html.Div(
        [
            html.Span(label, className="insights-metric-label"),
            html.Strong(value, className="insights-metric-value"),
            html.Span(detail, className="insights-metric-detail"),
        ],
        className=f"insights-metric-card insights-metric-card-{tone}",
    )


def _get_data_coverage() -> dict:
    cutoff_30 = _cutoff_iso(29)
    conn = get_connection()
    coverage = {
        "sleep_days": int(
            conn.execute("SELECT COUNT(*) AS count FROM sleep_logs WHERE date >= ?", (cutoff_30,)).fetchone()["count"] or 0
        ),
        "exercise_days": int(
            conn.execute(
                """
                SELECT COUNT(DISTINCT date) AS count
                FROM (
                    SELECT date FROM exercise_cardio WHERE date >= ?
                    UNION ALL
                    SELECT date FROM exercise_resistance WHERE date >= ?
                )
                """,
                (cutoff_30, cutoff_30),
            ).fetchone()["count"]
            or 0
        ),
        "project_days": int(
            conn.execute(
                "SELECT COUNT(DISTINCT date) AS count FROM project_sessions WHERE date >= ?",
                (cutoff_30,),
            ).fetchone()["count"]
            or 0
        ),
        "weight_days": int(
            conn.execute("SELECT COUNT(*) AS count FROM weight_logs WHERE date >= ?", (cutoff_30,)).fetchone()["count"] or 0
        ),
        "nutrition_days": int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM daily_health_logs WHERE date >= ?",
                (cutoff_30,),
            ).fetchone()["count"]
            or 0
        ),
    }
    conn.close()
    active_domains = sum(1 for key in coverage if coverage[key] > 0)
    coverage["tracked_domains"] = active_domains
    coverage["coverage_pct"] = round((sum(coverage[key] for key in coverage if key.endswith("_days")) / 150.0) * 100.0, 1)
    return coverage


def _build_overview_metrics(patterns: list[dict], coverage: dict, records: list[dict]):
    strongest = max((abs(float(p.get("correlation_value") or 0.0)) for p in patterns), default=0.0)
    return html.Div(
        [
            _metric_card("Patterns detected", str(len(patterns)), "Cross-domain links surfaced", tone="primary"),
            _metric_card("Strongest signal", f"{strongest:.2f}", "Absolute correlation strength", tone="warm"),
            _metric_card("Tracked domains", f"{coverage['tracked_domains']}/5", "Active in the last 30 days", tone="default"),
            _metric_card("Records surfaced", str(len(records)), "Personal bests and streaks", tone="default"),
        ],
        className="insights-metric-grid",
    )


def _build_pattern_cards(patterns: list[dict]):
    if not patterns:
        return html.Div(
            [
                html.H5("No reliable cross-domain patterns yet.", className="mb-2"),
                html.P(
                    "The page will get sharper as sleep, exercise, project, and nutrition logs overlap more consistently.",
                    className="insights-muted mb-0",
                ),
            ],
            className="insights-empty-state",
        )

    cards = []
    for pattern in sorted(patterns, key=lambda item: abs(float(item.get("correlation_value") or 0.0)), reverse=True):
        significance = (pattern.get("significance") or "weak").strip().lower()
        corr_value = float(pattern.get("correlation_value") or 0.0)
        direction_text = "Positive link" if corr_value >= 0 else "Inverse link"
        cards.append(
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(significance.title(), className=f"insights-pattern-badge insights-pattern-badge-{significance}"),
                            html.Span(f"{direction_text} · {corr_value:.2f}", className="insights-pattern-meta"),
                        ],
                        className="insights-pattern-top",
                    ),
                    html.H5(pattern.get("description") or "Pattern detected", className="insights-pattern-title"),
                    html.P(
                        "This is strongest when those domains have overlapping entries on the same days.",
                        className="insights-muted mb-0",
                    ),
                ],
                className=f"insights-pattern-card insights-pattern-card-{significance}",
            )
        )
    return html.Div(cards, className="insights-pattern-stack")


def _build_data_quality_panel(coverage: dict):
    rows = [
        ("Sleep logs", coverage["sleep_days"], "days"),
        ("Exercise days", coverage["exercise_days"], "days"),
        ("Project days", coverage["project_days"], "days"),
        ("Weight entries", coverage["weight_days"], "entries"),
        ("Nutrition entries", coverage["nutrition_days"], "entries"),
    ]

    tips = []
    if coverage["sleep_days"] < 12:
        tips.append("Log sleep more consistently so other domains have a stable anchor metric.")
    if coverage["exercise_days"] < 8:
        tips.append("Exercise is under-sampled right now, so training-related signals will stay weak.")
    if coverage["project_days"] < 8:
        tips.append("Project sessions are sparse, which limits productivity trend comparisons.")
    if coverage["weight_days"] < 4:
        tips.append("Add a few more weight entries if you want body-composition trends to show up cleanly.")
    if not tips:
        tips.append("Coverage is solid. Keep entries consistent and the signal quality should continue compounding.")

    return html.Div(
        [
            html.Div(
                [
                    html.Span("Coverage score", className="insights-kicker"),
                    html.H3(f"{coverage['coverage_pct']:.0f}%", className="insights-coverage-score"),
                    html.P("Across the five main streams over the last 30 days.", className="insights-muted mb-3"),
                ]
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(label, className="insights-coverage-label"),
                            html.Span(f"{value}/30 {unit}", className="insights-coverage-value"),
                        ],
                        className="insights-coverage-row",
                    )
                    for label, value, unit in rows
                ],
                className="mb-3",
            ),
            html.Div(
                [html.Div(tip, className="insights-tip-item") for tip in tips],
                className="insights-tip-stack",
            ),
        ]
    )


def _get_personal_records() -> list[dict]:
    cutoff_120 = _cutoff_iso(119)
    today_iso = get_brisbane_date()
    conn = get_connection()
    records: list[dict] = []

    sleep_week_df = _query_dataframe(
        """
        SELECT date, energy, duration_minutes
        FROM sleep_logs
        WHERE date >= ?
        ORDER BY date
        """,
        (cutoff_120,),
    )
    if not sleep_week_df.empty:
        sleep_week_df["date"] = pd.to_datetime(sleep_week_df["date"])
        sleep_week_df["week_start"] = sleep_week_df["date"].dt.to_period("W-SAT").apply(lambda period: period.start_time)
        sleep_weekly = (
            sleep_week_df.groupby("week_start", as_index=False)
            .agg(avg_energy=("energy", "mean"), avg_hours=("duration_minutes", lambda series: series.mean() / 60.0))
            .sort_values(["avg_energy", "avg_hours"], ascending=[False, False])
        )
        best_sleep_week = sleep_weekly.iloc[0]
        records.append(
            {
                "label": "Best Sleep Week",
                "value": f"{float(best_sleep_week['avg_energy'] or 0):.1f}/5",
                "detail": (
                    f"Week of {best_sleep_week['week_start'].strftime('%Y-%m-%d')} "
                    f"with {float(best_sleep_week['avg_hours'] or 0):.1f}h average sleep."
                ),
            }
        )

    best_resistance_day = conn.execute(
        """
        SELECT date, SUM(reps * sets) AS total_reps
        FROM exercise_resistance
        GROUP BY date
        ORDER BY total_reps DESC
        LIMIT 1
        """
    ).fetchone()
    if best_resistance_day:
        records.append(
            {
                "label": "Biggest Resistance Day",
                "value": f"{int(best_resistance_day['total_reps'] or 0):,} reps",
                "detail": f"Logged on {best_resistance_day['date']}.",
            }
        )

    longest_cardio = conn.execute(
        """
        SELECT date, type, duration_min, distance_km
        FROM exercise_cardio
        ORDER BY duration_min DESC
        LIMIT 1
        """
    ).fetchone()
    if longest_cardio:
        distance = f" · {float(longest_cardio['distance_km']):.1f} km" if longest_cardio["distance_km"] else ""
        records.append(
            {
                "label": "Longest Cardio Session",
                "value": f"{int(longest_cardio['duration_min'] or 0)} min",
                "detail": f"{longest_cardio['date']} · {(longest_cardio['type'] or 'cardio').title()}{distance}.",
            }
        )

    project_week_df = _query_dataframe(
        """
        SELECT date, duration_min
        FROM project_sessions
        WHERE date >= ?
        ORDER BY date
        """,
        (cutoff_120,),
    )
    if not project_week_df.empty:
        project_week_df["date"] = pd.to_datetime(project_week_df["date"])
        project_week_df["week_start"] = project_week_df["date"].dt.to_period("W-SAT").apply(lambda period: period.start_time)
        project_weekly = (
            project_week_df.groupby("week_start", as_index=False)
            .agg(sessions=("date", "count"), total_min=("duration_min", "sum"))
            .sort_values(["total_min", "sessions"], ascending=[False, False])
        )
        strongest_project_week = project_weekly.iloc[0]
        records.append(
            {
                "label": "Strongest Project Week",
                "value": f"{int(strongest_project_week['total_min'] or 0)} min",
                "detail": (
                    f"Week of {strongest_project_week['week_start'].strftime('%Y-%m-%d')} "
                    f"across {int(strongest_project_week['sessions'] or 0)} sessions."
                ),
            }
        )

    sleep_dates = conn.execute(
        "SELECT date FROM sleep_logs WHERE date <= ? ORDER BY date DESC LIMIT 60",
        (today_iso,),
    ).fetchall()
    conn.close()

    date_set = {row["date"] for row in sleep_dates}
    streak = 0
    cursor = _brisbane_today()
    while cursor.isoformat() in date_set:
        streak += 1
        cursor -= timedelta(days=1)
    if streak > 0:
        records.append(
            {
                "label": "Current Sleep Streak",
                "value": f"{streak} days",
                "detail": "Consecutive days with a sleep log entry.",
            }
        )

    return records


def _build_record_cards(records: list[dict]):
    if not records:
        return html.Div(
            "No records yet. Keep logging and the page will start surfacing personal bests.",
            className="insights-empty-state",
        )

    return html.Div(
        [
            html.Div(
                [
                    html.Span(record["label"], className="insights-record-label"),
                    html.Strong(record["value"], className="insights-record-value"),
                    html.Span(record["detail"], className="insights-record-detail"),
                ],
                className="insights-record-card",
            )
            for record in records
        ],
        className="insights-record-grid",
    )


def create_correlation_heatmap() -> go.Figure:
    cutoff = _cutoff_iso(44)
    query = """
        WITH daily_dates AS (
            SELECT date FROM sleep_logs WHERE date >= ?
            UNION
            SELECT date FROM exercise_cardio WHERE date >= ?
            UNION
            SELECT date FROM exercise_resistance WHERE date >= ?
            UNION
            SELECT date FROM project_sessions WHERE date >= ?
        )
        SELECT
            d.date,
            (SELECT duration_minutes / 60.0 FROM sleep_logs s WHERE s.date = d.date) AS sleep_hours,
            (SELECT energy FROM sleep_logs s WHERE s.date = d.date) AS sleep_energy,
            (SELECT sleep_quality FROM sleep_logs s WHERE s.date = d.date) AS sleep_quality,
            COALESCE((SELECT SUM(duration_min) FROM exercise_cardio e WHERE e.date = d.date), 0) AS cardio_min,
            COALESCE((SELECT SUM(reps * sets) FROM exercise_resistance r WHERE r.date = d.date), 0) AS resistance_reps,
            COALESCE((SELECT SUM(duration_min) FROM project_sessions p WHERE p.date = d.date), 0) AS project_min
        FROM daily_dates d
        ORDER BY d.date
    """
    df = _query_dataframe(query, (cutoff, cutoff, cutoff, cutoff))
    if df.empty or len(df) < 7:
        return _empty_figure("Cross-Domain Correlation Map", "Not enough overlapping data yet.")

    activity_cols = ["cardio_min", "resistance_reps", "project_min"]
    for col in activity_cols:
        df[col] = df[col].fillna(0)

    metric_order = ["sleep_hours", "sleep_energy", "sleep_quality", "cardio_min", "resistance_reps", "project_min"]
    valid_metrics = [col for col in metric_order if df[col].notna().sum() >= 7 and df[col].nunique(dropna=True) > 1]
    if len(valid_metrics) < 3:
        return _empty_figure("Cross-Domain Correlation Map", "The current sample is too thin for a useful correlation view.")

    corr_matrix = df[valid_metrics].corr().round(2)
    labels = [METRIC_LABELS[col] for col in valid_metrics]

    fig = go.Figure(
        data=go.Heatmap(
            z=corr_matrix.values,
            x=labels,
            y=labels,
            zmin=-1,
            zmax=1,
            colorscale=[
                [0.0, "#9b2c2c"],
                [0.5, "#f8fafc"],
                [1.0, "#0b4a8b"],
            ],
            text=corr_matrix.values,
            texttemplate="%{text:.2f}",
            textfont={"size": 11},
            colorbar=dict(title="Correlation"),
            hovertemplate="%{x}<br>%{y}<br>%{z:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title="Cross-Domain Correlation Map",
        margin=CHART_MARGIN,
        height=430,
    )
    fig.update_xaxes(side="bottom")
    fig.update_yaxes(autorange="reversed")
    return fig


def create_weekly_momentum_figure() -> go.Figure:
    cutoff = _cutoff_iso(83)
    query = """
        WITH daily_dates AS (
            SELECT date FROM sleep_logs WHERE date >= ?
            UNION
            SELECT date FROM exercise_cardio WHERE date >= ?
            UNION
            SELECT date FROM exercise_resistance WHERE date >= ?
            UNION
            SELECT date FROM project_sessions WHERE date >= ?
        )
        SELECT
            d.date,
            (SELECT AVG(energy) FROM sleep_logs s WHERE s.date = d.date) AS sleep_energy,
            COALESCE((SELECT SUM(duration_min) FROM exercise_cardio e WHERE e.date = d.date), 0) AS cardio_min,
            COALESCE((SELECT SUM(reps * sets) FROM exercise_resistance r WHERE r.date = d.date), 0) AS resistance_reps,
            COALESCE((SELECT SUM(duration_min) FROM project_sessions p WHERE p.date = d.date), 0) AS project_min
        FROM daily_dates d
        ORDER BY d.date
    """
    df = _query_dataframe(query, (cutoff, cutoff, cutoff, cutoff))
    if df.empty or len(df) < 10:
        return _empty_figure("Weekly Momentum", "Not enough recent activity to map weekly momentum.", height=520)

    df["date"] = pd.to_datetime(df["date"])
    for col in ["cardio_min", "resistance_reps", "project_min"]:
        df[col] = df[col].fillna(0)

    weekly = (
        df.set_index("date")
        .sort_index()
        .resample("W-SUN")
        .agg(
            {
                "sleep_energy": "mean",
                "cardio_min": "sum",
                "resistance_reps": "sum",
                "project_min": "sum",
            }
        )
        .tail(12)
        .reset_index()
    )
    if len(weekly) < 2:
        return _empty_figure("Weekly Momentum", "Not enough weekly spread yet.", height=520)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.11,
        specs=[[{"secondary_y": True}], [{"secondary_y": True}]],
        subplot_titles=("Recovery and Cardio", "Resistance and Project Focus"),
    )

    fig.add_trace(
        go.Scatter(
            x=weekly["date"],
            y=weekly["sleep_energy"],
            mode="lines+markers",
            name="Sleep Energy",
            line=dict(color="#0b4a8b", width=3),
            marker=dict(size=6, color="#0b4a8b"),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=weekly["date"],
            y=weekly["cardio_min"],
            name="Cardio Minutes",
            marker_color="#d97706",
            opacity=0.6,
        ),
        row=1,
        col=1,
        secondary_y=True,
    )
    fig.add_trace(
        go.Bar(
            x=weekly["date"],
            y=weekly["resistance_reps"],
            name="Resistance Reps",
            marker_color="#2563eb",
            opacity=0.72,
        ),
        row=2,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=weekly["date"],
            y=weekly["project_min"],
            mode="lines+markers",
            name="Project Minutes",
            line=dict(color="#0f172a", width=2.5),
            marker=dict(size=5, color="#0f172a"),
        ),
        row=2,
        col=1,
        secondary_y=True,
    )

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        title="Weekly Momentum",
        margin=dict(l=24, r=24, t=84, b=24),
        height=560,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(
        tickformat="%b %d",
        showline=True,
        linewidth=1,
        linecolor="#d0d7e2",
        gridcolor="#eef2f7",
    )
    fig.update_yaxes(title_text="Sleep Energy", range=[0, 5], row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Cardio Minutes", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Resistance Reps", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Project Minutes", row=2, col=1, secondary_y=True)
    return fig


def layout():
    return dbc.Container(
        [
            html.Div(
                [
                    html.H2("Insights & Patterns", className="mb-1"),
                    html.P(
                        "A cleaner cross-domain read on what is compounding, what is weakly supported, and where more data would make the page smarter.",
                        className="mb-0 insights-muted",
                    ),
                ],
                className="app-page-head",
            ),
            dcc.Store(id="insights-refresh", data=0),
            dbc.Card(
                dbc.CardBody(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Div("Insight Engine", className="insights-kicker"),
                                        html.H3("Cross-domain signal board", className="mb-2"),
                                        html.P(
                                            "The page now stays live with your current data instead of waiting for a manual discovery pass.",
                                            className="insights-muted mb-0",
                                        ),
                                    ],
                                    lg=8,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Button(
                                            "Refresh Insights",
                                            id="insights-refresh-btn",
                                            color="dark",
                                            className="w-100 mb-2",
                                        ),
                                        html.Small(id="insights-last-refreshed", className="insights-refresh-meta"),
                                    ],
                                    lg=4,
                                    className="ms-auto",
                                ),
                            ],
                            className="g-3 align-items-center",
                        ),
                        html.Div(id="insights-overview", className="mt-4"),
                    ]
                ),
                className="insights-hero-card mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Loading(
                            _panel(
                                "Pattern radar",
                                "What the data is hinting at",
                                html.Div(id="insights-patterns"),
                            )
                        ),
                        lg=8,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dcc.Loading(
                            _panel(
                                "Signal quality",
                                "Coverage and what would sharpen it",
                                html.Div(id="insights-data-quality"),
                            )
                        ),
                        lg=4,
                        className="mb-3",
                    ),
                ],
                className="g-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Loading(
                            _panel(
                                "Correlation map",
                                "How the main metrics move together",
                                dcc.Graph(id="insights-correlation-heatmap", config={"displayModeBar": False}),
                            )
                        ),
                        lg=7,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dcc.Loading(
                            _panel(
                                "Records ledger",
                                "Standout weeks, sessions, and streaks",
                                html.Div(id="insights-records"),
                            )
                        ),
                        lg=5,
                        className="mb-3",
                    ),
                ],
                className="g-3",
            ),
            dcc.Loading(
                _panel(
                    "Momentum",
                    "Weekly direction across recovery, training, and focused work",
                    dcc.Graph(id="insights-trends-chart", config={"displayModeBar": False}),
                ),
                className="mb-3",
            ),
        ],
        fluid=True,
        className="insights-page app-page-shell",
    )


@callback(
    Output("insights-refresh", "data"),
    Input("insights-refresh-btn", "n_clicks"),
    State("insights-refresh", "data"),
    prevent_initial_call=True,
)
def trigger_insights_refresh(n_clicks, refresh_count):
    if not n_clicks:
        return dash.no_update
    return (refresh_count or 0) + 1


@callback(
    Output("insights-overview", "children"),
    Output("insights-patterns", "children"),
    Output("insights-data-quality", "children"),
    Output("insights-correlation-heatmap", "figure"),
    Output("insights-records", "children"),
    Output("insights-trends-chart", "figure"),
    Output("insights-last-refreshed", "children"),
    Input("insights-refresh", "data"),
)
def render_insights(_refresh):
    patterns = discover_correlations()
    coverage = _get_data_coverage()
    records = _get_personal_records()
    refreshed_label = f"Data through {_brisbane_today().strftime('%b %d, %Y')}"

    return (
        _build_overview_metrics(patterns, coverage, records),
        _build_pattern_cards(patterns),
        _build_data_quality_panel(coverage),
        create_correlation_heatmap(),
        _build_record_cards(records),
        create_weekly_momentum_figure(),
        refreshed_label,
    )
