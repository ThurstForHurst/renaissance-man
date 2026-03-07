"""Summary / Home Page."""
import dash
from dash import html, callback, Input, Output
import dash_bootstrap_components as dbc
from analytics.scoring import get_all_progress
from database.db import (
    get_exercise_summary, get_finance_summary, get_active_projects, get_sleep_logs,
    get_overall_level, get_latest_weight, get_net_worth, get_brisbane_date,
    generate_daily_quests, get_daily_quests
)
from utils.affirmations import get_identity_statement

dash.register_page(__name__, path='/')


def _top_priority_projects(projects: list, limit: int = 3):
    """Pick top Tier 1 projects for summary focus."""
    if not projects:
        return []

    tier_one = [p for p in projects if int(p.get('tier') or p.get('priority') or 99) == 1]
    return sorted(
        tier_one,
        key=lambda p: (
            int(p.get('tier') or p.get('priority') or 99),
            -(p.get('importance') or 0),
            p.get('days_since_touched') or 9999
        )
    )[:limit]


def create_level_progress_card(domain: str, progress: dict):
    """Create a card showing level progress for a domain."""
    if not progress:
        return html.Div()

    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.Small(domain.title(), className="summary-kicker"),
                html.H6(f"Level {progress['current_level']}", className="mb-2 summary-value"),
            ]),
            dbc.Progress(
                value=progress['percentage'],
                color="primary",
                className="mb-2",
                style={"height": "14px"}
            ),
            html.Small(
                f"{progress['progress']:,}/{progress['needed']:,} XP to Level {progress['next_level']}",
                className="text-muted"
            )
        ])
    ], className="summary-surface-card summary-interactive-card h-100")


def create_quest_status_card(quest: dict):
    """Render quest status card for summary page (read-only)."""
    difficulty_colors = {'easy': 'success', 'medium': 'warning', 'hard': 'danger'}
    color = difficulty_colors.get((quest.get('difficulty') or '').lower(), 'secondary')
    is_done = bool(quest.get('completed'))

    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.H6(quest['quest_text'], className="mb-2"),
                    dbc.Badge(f"+{quest['xp_reward']} XP", color=color, className="me-2"),
                    dbc.Badge((quest.get('difficulty') or 'n/a').title(), color="light", text_color=color),
                ], width=10),
                dbc.Col([
                    html.Span("Complete", className="text-success small fw-semibold") if is_done else html.Span("Pending", className="text-muted small")
                ], width=2, className="text-end align-self-center")
            ])
        ], className="py-2", style={'opacity': 0.65 if is_done else 1})
    ], className="summary-surface-card summary-interactive-card mb-2")


def render_quest_status_list():
    """Build the summary quest list from current DB state."""
    today = get_brisbane_date()
    generate_daily_quests(today)
    quests = get_daily_quests(today)
    return [create_quest_status_card(quest) for quest in quests]


def render_overall_level_section():
    """Render overall level card so it can be refreshed reactively."""
    overall = get_overall_level()
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.Small("Identity Engine", className="summary-kicker"),
                html.H3(f"Overall Level {overall['level']}", className="mb-3"),
            ]),
            dbc.Progress(
                value=overall['percentage'],
                color="primary",
                className="mb-2",
                style={"height": "28px"}
            ),
            html.P(
                f"{overall['xp_in_level']:,}/{overall['xp_per_level']:,} XP to Level {overall['next_level']}",
                className="text-muted mb-3"
            ),
            dbc.Row([
                dbc.Col([
                    html.Small("Next Milestone", className="summary-kicker d-block"),
                    html.Span(
                        f"Level {overall['milestones']['next_level']} (${overall['milestones']['next_reward']})",
                        className="fw-semibold d-block"
                    ),
                    html.Small(
                        f"{overall['milestones']['xp_to_next']:,} XP remaining",
                        className="text-muted"
                    ),
                ], md=4, className="mb-2"),
                dbc.Col([
                    html.Small("Last Milestone", className="summary-kicker d-block"),
                    html.Span(
                        (
                            f"Level {overall['milestones']['last_level']} (${overall['milestones']['last_reward']})"
                            if overall['milestones']['last_level'] is not None
                            else "None yet"
                        ),
                        className="fw-semibold d-block"
                    ),
                ], md=4, className="mb-2"),
                dbc.Col([
                    html.Small("Rewards Banked", className="summary-kicker d-block"),
                    html.Span(
                        f"${overall['milestones']['total_rewards_value']:,}",
                        className="fw-semibold d-block"
                    ),
                    html.Small(
                        f"{overall['milestones']['count_100']} x $100 | {overall['milestones']['count_500']} x $500",
                        className="text-muted"
                    ),
                ], md=4, className="mb-2"),
            ])
        ])
    ], className="summary-hero-card mb-4")


def render_overview_section():
    """Render summary overview section (financial, bodyweight, project focus)."""
    net_worth = get_net_worth()
    latest_weight = get_latest_weight()
    projects = get_active_projects()
    top_projects = _top_priority_projects(projects, limit=3)
    primary_project = top_projects[0] if top_projects else None

    chips = []
    for project in top_projects:
        chips.append(
            dbc.Badge(
                f"T{project['tier']} - {project['name']}",
                color="light",
                text_color="dark",
                className="summary-project-chip border"
            )
        )

    return dbc.Card([
        dbc.CardHeader(html.H5("Overview", className="mb-0")),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Small("Net Worth", className="summary-kicker d-block"),
                    html.H4(
                        f"${net_worth['net_worth']:,.2f}",
                        className=f"mb-1 {'text-success' if net_worth['net_worth'] >= 0 else 'text-danger'}"
                    ),
                    html.Small("Current total assets minus liabilities", className="text-muted"),
                ], lg=4, className="mb-3"),
                dbc.Col([
                    html.Small("Current Weight", className="summary-kicker d-block"),
                    html.H4(
                        f"{latest_weight:.1f} kg" if latest_weight else "--",
                        className="mb-1"
                    ),
                    html.Small("Logged from Health page", className="text-muted"),
                ], lg=4, className="mb-3"),
                dbc.Col([
                    html.Small("Primary Focus", className="summary-kicker d-block"),
                    html.H4(primary_project['name'] if primary_project else "No active Tier 1 projects", className="mb-1"),
                    html.Small(
                        f"Tier {primary_project['tier']} | {primary_project.get('session_count') or 0} sessions"
                        if primary_project else "Create or promote a Tier 1 project in Projects",
                        className="text-muted"
                    ),
                    html.Div(
                        chips if chips else html.Small("No active Tier 1 projects", className="text-muted"),
                        className="d-flex flex-wrap gap-2 mt-2"
                    ),
                ], lg=4, className="mb-3"),
            ], className="mb-2"),
        ])
    ], className="summary-surface-card mb-4")


def render_domain_progress_row():
    """Render domain progress cards so they can refresh reactively."""
    progress = get_all_progress()
    return dbc.Row([
        dbc.Col(create_level_progress_card('sleep', progress['sleep']), md=6, lg=3, className="mb-3"),
        dbc.Col(create_level_progress_card('health', progress['health']), md=6, lg=3, className="mb-3"),
        dbc.Col(create_level_progress_card('projects', progress['projects']), md=6, lg=3, className="mb-3"),
        dbc.Col(create_level_progress_card('finance', progress['finance']), md=6, lg=3, className="mb-3"),
    ], className="mb-1")


def render_weekly_glance():
    """Render weekly cross-domain quick metrics."""
    exercise = get_exercise_summary(7)
    finance = get_finance_summary()
    sleep_df = get_sleep_logs(7)
    projects = get_active_projects()
    sleep_logs_count = len(sleep_df)

    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Small("Sleep", className="summary-kicker d-block"),
                    html.H4(f"{sleep_logs_count}/7", className="mb-1"),
                    html.Small(
                        "Sleep logs recorded in the last 7 days",
                        className="text-muted"
                    ),
                ])
            ], className="summary-surface-card summary-interactive-card h-100")
        ], md=6, lg=3, className="mb-3"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Small("Exercise", className="summary-kicker d-block"),
                    html.H4(f"{exercise['cardio_minutes']:.0f} min", className="mb-1"),
                    html.Small(
                        f"{sum(exercise['resistance'].values()):.0f} resistance reps",
                        className="text-muted"
                    ),
                ])
            ], className="summary-surface-card summary-interactive-card h-100")
        ], md=6, lg=3, className="mb-3"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Small("Projects", className="summary-kicker d-block"),
                    html.H4(f"{len(projects)} active", className="mb-1"),
                    html.Small(f"{sum((p.get('session_count') or 0) for p in projects)} total sessions", className="text-muted"),
                ])
            ], className="summary-surface-card summary-interactive-card h-100")
        ], md=6, lg=3, className="mb-3"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Small("Finance", className="summary-kicker d-block"),
                    html.H4(f"{finance['savings_rate']:.0f}%", className="mb-1"),
                    html.Small(f"${finance['avg_savings']:.0f} average weekly savings", className="text-muted"),
                ])
            ], className="summary-surface-card summary-interactive-card h-100")
        ], md=6, lg=3, className="mb-3"),
    ])


def layout():
    identity_stmt = get_identity_statement()
    return dbc.Container([
        html.Div([
            html.H2("Summary", className="mb-1"),
            html.P("Your cross-domain performance dashboard.", className="text-muted mb-0"),
        ], className="summary-page-head app-page-head mb-4"),
        html.Div(id='summary-overall-level', children=render_overall_level_section()),
        dbc.Alert([
            html.I(className="fas fa-quote-left me-2"),
            html.Span(identity_stmt, className="small"),
            html.I(className="fas fa-quote-right ms-2"),
        ], color="light", className="mb-3 py-2 summary-identity-alert"),
        html.Div(id='summary-overview', children=render_overview_section()),
        html.H4("Domain Progress", className="mb-3"),
        html.Div(id='summary-domain-progress', children=render_domain_progress_row()),
        html.H4("Daily Quests", className="mb-3"),
        html.Div(id='summary-quest-status', children=render_quest_status_list(), className="mb-4"),
        html.H4("This Week", className="mb-3"),
        html.Div(render_weekly_glance(), className="mb-2"),
        dbc.Row([
            dbc.Col(dbc.Button("Log Sleep", href="/sleep", color="dark", size="sm", className="w-100"), md=3, className="mb-2"),
            dbc.Col(dbc.Button("Log Health", href="/health", color="dark", size="sm", className="w-100"), md=3, className="mb-2"),
            dbc.Col(dbc.Button("Log Project Session", href="/projects", color="dark", size="sm", className="w-100"), md=3, className="mb-2"),
            dbc.Col(dbc.Button("View Insights", href="/insights", color="dark", size="sm", className="w-100"), md=3, className="mb-2"),
        ], className="mt-2")
    ], fluid=True, className="summary-page app-page-shell")


@callback(
    Output('summary-overall-level', 'children'),
    Output('summary-overview', 'children'),
    Output('summary-domain-progress', 'children'),
    Output('summary-quest-status', 'children'),
    Input('quest-sync-signal', 'data'),
    Input('sidebar-refresh-interval', 'n_intervals')
)
def refresh_summary_quests(sync_signal, n_intervals):
    """Keep summary cards in sync with live sidebar and tracking events."""
    return (
        render_overall_level_section(),
        render_overview_section(),
        render_domain_progress_row(),
        render_quest_status_list(),
    )
