"""
Main Application Entry Point
"""
import os
import dash
from dash import Dash, html, dcc, page_container
import dash_bootstrap_components as dbc

from database.db import init_db
from components.sidebar import render_sidebar, render_collapsed_sidebar

init_db()

# Initialize app
app = Dash(
    __name__,
    use_pages=True,
    pages_folder='pages',
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    suppress_callback_exceptions=True
)

# Server for deployment
server = app.server

# App layout with navbar and sidebar
app.layout = html.Div([
    # Navbar (sticky at top)
    dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand(
                "Renaissance Man", 
                href="/",
                style={'fontSize': '1.8rem', 'fontWeight': 'bold'}
            ),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink("Summary", href="/", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Sleep", href="/sleep", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Health", href="/health", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Projects", href="/projects", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Finance", href="/finance", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Journal", href="/journal", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Insights", href="/insights", style={'fontSize': '1.05rem'}, className="app-nav-link")),
                dbc.NavItem(dbc.NavLink("Execution Cycle", href="/quarterly", style={'fontSize': '1.05rem'}, className="app-nav-link app-nav-link-exec")),
            ], className="ms-auto", style={'paddingRight': '2vw'}),
        ], fluid=True),
        color="#001f3f",
        dark=True,
        className="app-top-navbar",
        style={
            'minHeight': '100px',
            'paddingTop': '1.5rem',
            'paddingBottom': '1.5rem',
            'position': 'fixed',
            'top': '0',
            'width': '100%',
            'zIndex': '1100'
        }
    ),
    
    # Main content area with sidebar
    html.Div([
        # Sidebar container (will be replaced by callback)
        html.Div(id='sidebar-container', children=render_collapsed_sidebar()),
        
        # Page content (with left margin for sidebar)
        html.Div(
            id='main-content',
            children=page_container,
            style={'marginLeft': '44px', 'width': 'calc(100% - 44px)', 'paddingTop': '24px', 'paddingBottom': '3rem', 'transition': 'all 0.3s ease'},
            className='app-main-content'
        ),
        
        # Store for sidebar state
        dcc.Store(id='sidebar-state', data=False),
        dcc.Store(id='quest-sync-signal', data=None),
        dcc.Store(id='task-sync-signal', data=None),
        dcc.Store(id='sidebar-refresh-signature', data=""),
        
        # Interval for auto-refresh
        dcc.Interval(id='sidebar-refresh-interval', interval=60000, n_intervals=0)
    ], style={'paddingTop': '100px'})
])

if __name__ == '__main__':
    debug_mode = os.getenv('DASH_DEBUG', '0') == '1'
    # Run app
    app.run(
        debug=debug_mode,
        host='127.0.0.1',
        port=8050,
        use_reloader=False,
        dev_tools_hot_reload=False,
        dev_tools_ui=False,
        dev_tools_props_check=False,
        dev_tools_serve_dev_bundles=False
    )
