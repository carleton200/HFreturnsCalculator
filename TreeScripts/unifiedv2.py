# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import webbrowser
import time
import os
from datetime import datetime

"""
Created on Fri Nov  7 16:48:12 2025

@author: mmullaney
"""

#!/usr/bin/env python3
"""
Unified Dash app (Version 7)
- Dynamic "Target Date" selection.
- Graph data generated on-the-fly for selected date.
"""

import json
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import os
import numpy as np
import pandas as pd
from math import ceil

import dash
from dash import Dash, dcc, html, Input, Output, State, no_update, callback_context
import dash_cytoscape as cyto
from dash.dash_table import DataTable

# Enable extra layouts
cyto.load_extra_layouts()

# Import data ingestion & processing logic
import TreeScripts.create_all_paths as create_all_paths
import importlib
importlib.reload(create_all_paths)  # Reload to pick up new functions

# =============================================================================
# 1) Data Loading (Full History)
# =============================================================================
# Global variables that will be set by create_dash_app or loaded directly
FULL_DATA = None
DATE_OPTIONS = []
DEFAULT_DATE = None


def load_and_prepare_data(full_data=None):
    """
    Load and prepare data for the Dash app.
    
    Args:
        full_data: Optional pre-loaded DataFrame. If None, loads from API.
        
    Returns:
        tuple: (FULL_DATA, DATE_OPTIONS, DEFAULT_DATE)
    """
    if full_data is None:
        print("WARNING: No data loaded in for dash app. Closing...")
        return
    else:
        print(f"Using pre-loaded data: {len(full_data)} rows")
    
    # Normalize date column
    if "As of date" in full_data.columns:
        full_data["As of date"] = pd.to_datetime(full_data["As of date"], errors="coerce")
    else:
        full_data["As of date"] = pd.NaT
    
    # Calculate end of previous month (cutoff date)
    today = pd.Timestamp.now()
    # First day of current month
    first_of_month = today.replace(day=1)
    # Last day of previous month
    end_of_prev_month = first_of_month - pd.Timedelta(days=1)
    
    # Truncate data to only include dates <= end of previous month
    full_data = full_data[full_data["As of date"] <= end_of_prev_month].copy()
    
    # Get sorted unique dates for dropdown (now filtered to <= end of previous month)
    available_dates = sorted(full_data["As of date"].dropna().unique(), reverse=True)
    date_options = [
        {"label": pd.Timestamp(d).strftime("%Y-%m-%d"), "value": str(pd.Timestamp(d))}
        for d in available_dates
    ]
    
    # Default to the most recent available date (which will be <= end of previous month)
    default_date = date_options[0]["value"] if date_options else None
    
    print(f"Loaded {len(full_data)} rows. Available dates: {len(available_dates)}")
    
    return full_data, date_options, default_date


# Load data if running standalone
if __name__ == "__main__":
    FULL_DATA, DATE_OPTIONS, DEFAULT_DATE = load_and_prepare_data()

# =============================================================================
# Helpers
# =============================================================================
def format_money(val):
    if pd.isna(val): return ""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)
    
    abs_val = abs(val)
    sign = "-$" if val < 0 else "$"
    
    if abs_val >= 1e9: return f"{sign}{abs_val/1e9:.1f}B"
    if abs_val >= 1e6: return f"{sign}{abs_val/1e6:.1f}M"
    if abs_val >= 1e3: return f"{sign}{abs_val/1e3:.0f}K"
    return f"{sign}{abs_val:.0f}"

def format_money_full(val):
    if pd.isna(val): return ""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)
    
    sign = "-$" if val < 0 else "$"
    val = abs(val)
    return f"{sign}{val:,.2f}"

def format_pct(val):
    if pd.isna(val): return ""
    try:
        val = float(val)
        return f"{val*100:.0f}%"
    except (ValueError, TypeError):
        return ""

def build_children_map(edges_df: pd.DataFrame) -> Dict[str, List[str]]:
    ch: Dict[str, List[str]] = {}
    for s, t in edges_df[["source", "target"]].itertuples(index=False):
        ch.setdefault(s, []).append(t)
        ch.setdefault(t, [])
    return ch

def build_parents_map(edges_df: pd.DataFrame) -> Dict[str, List[str]]:
    pa: Dict[str, List[str]] = {}
    for s, t in edges_df[["source", "target"]].itertuples(index=False):
        pa.setdefault(t, []).append(s)
        pa.setdefault(s, [])
    return pa

def ancestors_of(leaf: str, parents: Dict[str, List[str]], max_depth: int | None = None) -> Set[str]:
    keep: Set[str] = {leaf}
    frontier = [(leaf, 0)]
    while frontier:
        node, d = frontier.pop(0)
        if max_depth is not None and d >= max_depth:
            continue
        for p in parents.get(node, []):
            if p not in keep:
                keep.add(p)
                frontier.append((p, d + 1))
    return keep

def descendants_of(root: str, children: Dict[str, List[str]], max_depth: int | None = None) -> Set[str]:
    keep: Set[str] = {root}
    frontier = [(root, 0)]
    while frontier:
        node, d = frontier.pop(0)
        if max_depth is not None and d >= max_depth:
            continue
        for c in children.get(node, []):
            if c not in keep:
                keep.add(c)
                frontier.append((c, d + 1))
    return keep

def parse_store_data(data):
    if not data:
        return pd.DataFrame(), pd.DataFrame(), {}, {}, {}, {}, {}, {}
    
    nodes_df = pd.DataFrame(data['nodes'])
    edges_df = pd.DataFrame(data['edges'])
    
    # Rebuild maps
    parents_map = build_parents_map(edges_df)
    children_map = build_children_map(edges_df)
    
    # Rebuild lookups
    # Ensure correct types
    if "level" in nodes_df.columns:
        # handle potential mixed types
        nodes_df["level"] = pd.to_numeric(nodes_df["level"], errors='coerce')
        
    level_by_id = nodes_df.set_index("id")["level"].to_dict()
    balance_by_id = nodes_df.set_index("id")["balance"].to_dict()
    in_deg = edges_df.groupby("target").size().to_dict()
    out_deg = edges_df.groupby("source").size().to_dict()
    
    return nodes_df, edges_df, parents_map, children_map, level_by_id, balance_by_id, in_deg, out_deg

# =============================================================================
# Layout / Styling
# =============================================================================
MODE_OPTIONS = [
    {"label": "Top-down (descendants)", "value": "topdown"},
    {"label": "Bottom-up (ancestors)", "value": "bottomup"},
    {"label": "Tidy (tables)", "value": "tidy"},
]

DENSITY_CAP = {"low": 15, "med": 30, "high": 50}

def base_stylesheet(curve_style: str = "straight", extra_edge: Dict | None = None) -> List[Dict]:
    edge_style = {
        "curve-style": curve_style,
        "target-arrow-shape": "triangle",
        "arrow-scale": 1,
        "width": 1,
        "line-color": "#bdbdbd",
        "target-arrow-color": "#bdbdbd",
        "label": "data(label)",
        "font-size": "10px",
        "text-background-color": "#ffffff",
        "text-background-opacity": 0.8,
        "text-background-shape": "roundrectangle",
        "text-background-padding": "2px",
    }
    if extra_edge:
        edge_style.update(extra_edge)
    return [
        {"selector": "node", "style": {
            "label": "data(display_label)", "font-size": "12px", "min-zoomed-font-size": 8,
            "text-wrap": "wrap", "text-max-width": "220px",
            "text-valign": "center", "text-halign": "center",
            "color": "#ffffff",
            "background-color": "data(bg)", "border-width": 1, "border-color": "#6baed6",
            "shape": "round-rectangle", "padding": "8px",
            "width": "label", "height": "label",
        }},
        {"selector": "edge", "style": edge_style},
        {"selector": ".lvl-1", "style": {"background-color": "#003f5c", "border-color": "#002a3e"}},
        {"selector": ".lvl-2", "style": {"background-color": "#58508d", "border-color": "#3b365e"}},
        {"selector": ".lvl-3", "style": {"background-color": "#bc5090", "border-color": "#7e3560"}},
        {"selector": ".lvl-4", "style": {"background-color": "#ff6361", "border-color": "#cc4f4d"}},
        {"selector": ".lvl-5", "style": {"background-color": "#ffa600", "border-color": "#cc8500"}},
        {"selector": "edge.same-level", "style": {"line-color": "#d62728","target-arrow-color": "#d62728",
                                                  "width": 3, "opacity": 1}},
    ]

def build_elements(sub_nodes: pd.DataFrame, sub_edges: pd.DataFrame, positions: Dict[str, Dict[str, float]] | None = None) -> List[Dict]:
    elems = []
    for r in sub_nodes.itertuples(index=False):
        nid = str(getattr(r, "id"))
        label = str(getattr(r, "label"))
        lvl = int(getattr(r, "level")) if not pd.isna(getattr(r, "level", np.nan)) else 0
        bg = getattr(r, "color", None) or "#9ecae1"
        bal = getattr(r, "balance", None)
        bal_str = format_money(bal)
        display_label = f"{label}\n{bal_str}" if bal_str else label
        
        d = {"id": nid, "label": label, "display_label": display_label, "level": lvl, "bg": bg, "balance": bal_str}
        node = {"data": d, "classes": f"lvl-{lvl}" if lvl else ""}
        if positions and nid in positions:
            node["position"] = positions[nid]
        elems.append(node)
        
    # Must build LEVEL_BY_ID for edge coloring locally or pass it
    # For now, we infer it from sub_nodes which is safe for the subgraph
    local_level_map = sub_nodes.set_index("id")["level"].to_dict()

    for r in sub_edges.itertuples(index=False):
        s, t = str(r.source), str(r.target)
        ls, lt = local_level_map.get(s), local_level_map.get(t)
        same_level = (pd.notna(ls) and pd.notna(lt) and int(ls) == int(lt))
        
        pct = getattr(r, "percentage", None)
        pct_str = format_pct(pct)
        val = getattr(r, "position_value", None)
        val_str = format_money(val)
        edge_label = val_str if val_str else ""
        
        elems.append({
            "data": {"source": s, "target": t, "label": edge_label, "percentage": pct_str, "value": val_str},
            "classes": ("same-level" if same_level else "")
        })
    return elems

def pick_layout_and_styles(visible_nodes: List[str], roots: List[str] | None = None, layout_mode: str = "auto", small_threshold: int = 50, mode: str = "topdown"):
    small = len(visible_nodes) <= small_threshold
    if layout_mode == "auto":
        layout_name = "dagre" if small else "preset"
    else:
        layout_name = layout_mode
    
    if layout_name in ("dagre", "fcose"):
        layout = {"name": layout_name, "fit": True, "padding": 40, "animate": False}
        if layout_name == "dagre" and roots:
            layout["roots"] = roots
    else:
        layout = {"name": "preset", "fit": False, "animate": False}
    
    if mode == "bottomup":
        edge_style = {"curve-style": "taxi", "taxi-direction": "vertical", "taxi-turn-min-distance": 12}
        stylesheet = base_stylesheet(curve_style="taxi", extra_edge=edge_style)
    else:
        stylesheet = base_stylesheet(curve_style="straight")
    return small, layout, stylesheet

# =============================================================================
# App Initialization
# =============================================================================
def create_app_layout(initial_node=None, initial_date=None):
    """
    Create the Dash app layout with optional initial values.
    
    Args:
        initial_node: Optional node ID to pre-select in focus dropdown
        initial_date: Optional date string to pre-select in date dropdown
        
    Returns:
        Dash layout component
    """
    # Determine initial date value
    date_value = DEFAULT_DATE
    if initial_date:
        # Convert YYYY-MM-DD to timestamp string for dropdown
        try:
            dt = pd.Timestamp(initial_date)
            date_value = str(dt)
            # Ensure it's in DATE_OPTIONS
            if not any(opt["value"] == date_value for opt in DATE_OPTIONS):
                print(f"Warning: Initial date {initial_date} not in available dates. Using default.")
                date_value = DEFAULT_DATE
        except:
            print(f"Warning: Could not parse initial date {initial_date}. Using default.")
            date_value = DEFAULT_DATE
    
    return html.Div(
    [
        # Header / Controls
        html.Div(
            [
                html.Div([
                    html.Label("As of Date"),
                    dcc.Dropdown(
                        id="date-dropdown",
                        options=DATE_OPTIONS,
                        value=date_value,
                        clearable=False,
                        style={"fontWeight": "bold"}
                    ),
                ], style={"minWidth": 180, "marginRight": "10px"}),

                html.Div([
                    html.Label("Mode"),
                    dcc.RadioItems(id="mode-radio", options=MODE_OPTIONS, value="topdown", inline=True),
                ], style={"minWidth": 320, "marginRight": "10px"}),

                html.Div([
                    html.Label("Node (top-down) / Non-root (bottom-up) / Focus (tidy)"),
                    dcc.Dropdown(
                        id="focus-dropdown", 
                        placeholder="Type to select…", 
                        clearable=True,
                        value=initial_node if initial_node else None
                    ),
                ], style={"flex": 2, "minWidth": 320, "marginRight": "10px"}),
                
                html.Div([
                    html.Label("Density (Tidy)"),
                    dcc.Dropdown(
                        id="tidy-density",
                        options=[
                            {"label": "Low", "value": "low"},
                            {"label": "Med", "value": "med"},
                            {"label": "High", "value": "high"},
                        ],
                        value="med", clearable=False,
                    ),
                ], style={"minWidth": 120, "marginRight": "10px"}),
                
                html.Div([
                    html.Label("Depth"),
                    dcc.Slider(id="depth-slider", min=0, max=10, step=1, value=5, marks={i: str(i) for i in range(11)}),
                ], style={"flex": 1, "minWidth": 200, "marginRight": "10px"}),

                html.Div([
                    html.Label("Small threshold"),
                    dcc.Input(id="small-thresh", type="number", min=5, max=300, step=5, value=50, style={"width": "100px"}),
                ], style={"minWidth": 120, "marginRight": "10px"}),

                html.Div([
                    html.Label("Options"),
                    dcc.Checklist(
                        id="highlight-toggle",
                        options=[{"label": "Highlight Path", "value": "on"}],
                        value=[], # Default off
                        inline=True,
                        inputStyle={"marginRight": "5px"}
                    ),
                ], style={"minWidth": 120, "marginRight": "10px"}),

                html.Div([
                    html.Button("Back", id="back-btn", n_clicks=0),
                    html.Button("Up one level", id="up-btn", n_clicks=0, style={"marginLeft": "6px"}),
                    html.Button("Reset", id="reset-btn", n_clicks=0, style={"marginLeft": "6px"}),
                    html.Button("Fit", id="fit-btn", n_clicks=0, style={"marginLeft": "6px"}),
                ], style={"display": "flex", "alignItems": "end"}),
            ], 
            style={"display": "flex", "gap": "8px", "marginBottom": "8px", "flexWrap": "wrap", "backgroundColor": "#f9fafb", "padding": "10px", "borderRadius": "8px", "border": "1px solid #e5e7eb"},
        ),

        # Stores
        dcc.Loading(
            id="graph-loading",
            type="default",
            fullscreen=True,
            children=[dcc.Store(id="graph-store", data={})]
        ),
        dcc.Store(id="history-store", data=[]),
        
        # Main Views
        html.Div(
            id="graph-wrap",
            children=[
                cyto.Cytoscape(
                    id="graph",
                    elements=[],
                    layout={"name": "breadthfirst", "directed": True},
                    stylesheet=base_stylesheet(),
                    style={"width": "100%", "height": "85vh", "border": "1px solid #eee"},
                    wheelSensitivity=0.2,
                    zoomingEnabled=True, userZoomingEnabled=True, userPanningEnabled=True,
                    minZoom=0.1, maxZoom=4,
                ),
            ],
        ),
        
        html.Div(
            id="tidy-wrap",
            style={"display": "none", "width": "100%"},
            children=[
                html.Div([
                    html.Div([
                        html.H4("Parents", style={"display": "inline-block", "marginRight": "10px"}),
                        html.Span(id="parents-count", children="", style={"color": "#6b7280"}),
                    ]),
                    DataTable(
                        id="parents-table",
                        columns=[
                            {"name": "Label", "id": "label", "presentation": "markdown"},
                            {"name": "Level", "id": "level"},
                            {"name": "Value", "id": "value"},
                            {"name": "In", "id": "in_deg"},
                            {"name": "Out", "id": "out_deg"},
                        ],
                        data=[],
                        sort_action="native", style_table={"maxHeight": "28vh", "overflowY": "auto"},
                        style_cell={"whiteSpace": "normal", "height": "auto", "padding": "6px"},
                    ),
                    html.Button("Show more", id="parents-more-btn", n_clicks=0, style={"display": "none", "marginTop": "5px"}),
                ], style={"marginBottom": "10px"}),

                html.Div(id="tidy-focus-card", style={
                    "border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px 12px",
                    "margin": "10px 0", "background": "rgba(249,250,251,0.9)"
                }),

                html.Div([
                    html.Div([
                        html.H4("Children", style={"display": "inline-block", "marginRight": "10px"}),
                        html.Span(id="children-count", children="", style={"color": "#6b7280"}),
                    ]),
                    DataTable(
                        id="children-table",
                        columns=[
                            {"name": "Label", "id": "label", "presentation": "markdown"},
                            {"name": "Level", "id": "level"},
                            {"name": "Value", "id": "value"},
                            {"name": "In", "id": "in_deg"},
                            {"name": "Out", "id": "out_deg"},
                        ],
                        data=[],
                        sort_action="native", style_table={"maxHeight": "34vh", "overflowY": "auto"},
                        style_cell={"whiteSpace": "normal", "height": "auto", "padding": "6px"},
                    ),
                    html.Button("Show more", id="children-more-btn", n_clicks=0, style={"display": "none", "marginTop": "5px"}),
                ]),
            ],
        ),

        # Hover Card
        html.Div(
            id="hover-card",
            style={
                "position": "absolute", "bottom": "16px", "left": "16px",
                "maxWidth": "360px", "background": "rgba(255,255,255,0.95)",
                "border": "1px solid #e5e7eb", "borderRadius": "10px",
                "boxShadow": "0 2px 8px rgba(0,0,0,.08)", "padding": "10px 12px",
                "fontSize": "12px", "zIndex": 10,
            },
        ),
        
        # Hidden/Unused but kept for callback compatibility if needed
        dcc.Dropdown(id="parent-chooser", style={"display": "none"}), 
        html.Button(id="choose-parent-btn", style={"display": "none"}),
        
        # Store initial node value for callback
        dcc.Store(id="initial-node-store", data=initial_node),
        
        # Shutdown overlay and monitoring
        html.Div(
            id="shutdown-overlay",
            children=[
                html.Div([
                    html.H2("Shutting Down", style={"color": "#fff", "marginBottom": "20px", "fontSize": "28px"}),
                    html.P("The main application has shut down or an activity timeout has occured.", 
                           style={"color": "#fff", "fontSize": "16px", "margin": "0"}),
                ], style={"textAlign": "center"})
            ],
            style={"display": "none"}
        ),
        
        # Interval to check for shutdown status
        dcc.Interval(
            id="shutdown-check-interval",
            interval=1000,  # Check every second
            n_intervals=0
        ),
        
        # Store to track shutdown state
        dcc.Store(id="shutdown-flag-store", data=False),
    ],
    style={"padding": "10px", "position": "relative"}
    )

app = Dash(__name__)
app.title = "Hierarchy Explorer (Version 7)"



# =============================================================================
# Callbacks
# =============================================================================

# 1. Update Graph Data based on Date Selection
@app.callback(
    Output("graph-store", "data"),
    Input("date-dropdown", "value"),
)
def update_graph_data(date_str):
    if not date_str:
        return {}
    
    target_date = pd.Timestamp(date_str)
    print(f"Generating graph for {target_date}...")
    nodes, edges = create_all_paths.generate_graph_for_date(FULL_DATA, target_date)
    
    graph_data = {
        "nodes": nodes.to_dict(orient="records"),
        "edges": edges.to_dict(orient="records")
    }
    
    return graph_data

# 2. Refresh Dropdown Options based on Mode & Current Data
@app.callback(
    Output("focus-dropdown", "options"),
    Output("focus-dropdown", "value", allow_duplicate=True),
    Input("mode-radio", "value"),
    Input("graph-store", "data"),
    State("focus-dropdown", "value"),
    State("initial-node-store", "data"),
    prevent_initial_call='initial_duplicate',
)
def refresh_dropdown(mode, store_data, current_value, initial_node):
    if not store_data:
        return [], None
    
    # Check what triggered this callback
    ctx = dash.callback_context
    triggered = [t["prop_id"] for t in ctx.triggered] if ctx.triggered else []
    mode_changed = "mode-radio.value" in triggered
    data_changed = "graph-store.data" in triggered
    
    nodes_df = pd.DataFrame(store_data['nodes'])
    edges_df = pd.DataFrame(store_data['edges'])
    parents_map = build_parents_map(edges_df)
    
    if mode == "bottomup":
        # roots = nodes with NO parents
        roots = {n for n, ps in parents_map.items() if not ps}
        sub = nodes_df[~nodes_df["id"].isin(roots)].sort_values("label")
    else:
        sub = nodes_df.sort_values("label")

    options = [{"label": r["label"], "value": r["id"]} for _, r in sub.iterrows()]
    valid_values = {opt["value"] for opt in options}
    
    # If mode changed, always clear the dropdown
    if mode_changed:
        new_value = None
    # If data changed (initial load or date change)
    elif data_changed:
        # If we have a current value that's valid, keep it (user's choice)
        if current_value and current_value in valid_values:
            new_value = current_value
        # Otherwise, use initial_node if available and valid
        elif initial_node and initial_node in valid_values:
            new_value = initial_node
            print(f"Setting initial node: {initial_node}")
        else:
            new_value = None
    else:
        # Preserve current value if it's valid
        new_value = current_value
        if isinstance(new_value, list):
             valid_list = [v for v in new_value if v in valid_values]
             new_value = valid_list[0] if valid_list else None
        elif new_value and new_value not in valid_values:
             # Value is invalid for this mode - try initial_node if available
             if initial_node and initial_node in valid_values:
                 new_value = initial_node
             else:
                 new_value = None
        # If new_value is None and we have initial_node, use it
        elif new_value is None and initial_node and initial_node in valid_values:
            new_value = initial_node
    
    return options, new_value

# 2b. Clear dropdown on reset
@app.callback(
    Output("focus-dropdown", "value", allow_duplicate=True),
    Input("reset-btn", "n_clicks"),
    prevent_initial_call=True,
)
def clear_dropdown_on_reset(reset_clicks):
    if reset_clicks:
        return None
    return no_update

# 2c. Update Depth Slider based on Max Level
@app.callback(
    Output("depth-slider", "max"),
    Output("depth-slider", "marks"),
    Output("depth-slider", "value"),
    Input("graph-store", "data"),
    State("depth-slider", "value"),
)
def update_depth_slider(store_data, current_value):
    if not store_data:
        return 10, {i: str(i) for i in range(11)}, current_value
        
    nodes = store_data.get("nodes", [])
    if not nodes:
        return 10, {i: str(i) for i in range(11)}, current_value
        
    # Calculate max level in the data
    max_lvl = 0
    for n in nodes:
        l = n.get("level")
        if l is not None:
            try:
                val = int(l)
                if val > max_lvl:
                    max_lvl = val
            except (ValueError, TypeError):
                pass
                
    # Ensure at least some range
    if max_lvl < 1:
        max_lvl = 5
        
    marks = {i: str(i) for i in range(max_lvl + 1)}
    
    new_value = current_value
    if new_value > max_lvl:
        new_value = max_lvl
        
    return max_lvl, marks, new_value

# 3. Toggle View (Graph vs Tidy)
@app.callback(
    Output("graph-wrap", "style"),
    Output("tidy-wrap", "style"),
    Input("mode-radio", "value"),
)
def toggle_view_main(mode):
    if mode == "tidy":
        return {"display": "none"}, {"display": "block", "width": "100%"}
    return {"display": "block"}, {"display": "none"}

# 4. Clear History on Mode Change
@app.callback(
    Output("history-store", "data", allow_duplicate=True),
    Input("mode-radio", "value"),
    prevent_initial_call=True,
)
def clear_history_on_mode_change(mode):
    return []

# 5. Main Unify Callback (Graph Rendering)
@app.callback(
    Output("graph", "elements"),
    Output("graph", "layout", allow_duplicate=True),
    Output("graph", "zoom"),
    Output("history-store", "data"),
    Output("graph", "stylesheet"),
    Input("graph", "tapNode"),
    Input("focus-dropdown", "value"),
    Input("mode-radio", "value"),
    Input("depth-slider", "value"),
    Input("small-thresh", "value"),
    Input("back-btn", "n_clicks"),
    Input("up-btn", "n_clicks"),
    Input("reset-btn", "n_clicks"),
    Input("fit-btn", "n_clicks"),
    Input("graph-store", "data"), # Trigger on data update
    State("history-store", "data"),
    prevent_initial_call='initial_duplicate',
)
def unify(tap_node, focus_value, mode, depth, small_thresh,
          back_clicks, up_clicks, reset_clicks, fit_clicks, store_data, history):
    
    if not store_data:
        return [], {"name": "breadthfirst"}, no_update, [], []

    # Parse data
    nodes_df, edges_df, parents_map, children_map, _, _, _, _ = parse_store_data(store_data)
    
    triggered = [t["prop_id"] for t in dash.callback_context.triggered]
    
    # Reset
    if "reset-btn.n_clicks" in triggered and reset_clicks:
        elems = build_elements(nodes_df, edges_df, None)
        layout = {"name": "breadthfirst", "directed": True, "nodeDimensionsIncludeLabels": True, "spacingFactor": 1.3}
        stylesheet = base_stylesheet(curve_style=("taxi" if mode == "bottomup" else "straight"))
        return elems, layout, no_update, [], stylesheet

    node_clicked = tap_node["data"]["id"] if (tap_node and "data" in tap_node) else None
    cur_focus = None

    # History / Up / Select
    if "back-btn.n_clicks" in triggered and back_clicks and history:
        history = history[:-1]
        if history:
            mode = history[-1]["mode"]
            cur_focus = history[-1]["node"]
    elif "up-btn.n_clicks" in triggered and up_clicks:
        last = history[-1] if history else None
        last_node = last["node"] if last else (node_clicked or focus_value)
        candidates = last_node if isinstance(last_node, list) else [last_node]
        all_parents = set()
        for cand in candidates:
            if cand:
                ps = parents_map.get(cand, [])
                all_parents.update(ps)
        if all_parents:
            cur_focus = sorted(list(all_parents))
            mode = "topdown"
            history = history + [{"mode": mode, "node": cur_focus}]
    else:
        focus_changed = "focus-dropdown.value" in triggered
        mode_changed = "mode-radio.value" in triggered
        
        # Determine focus based on trigger source to prevent stale clicks/selections
        if "graph.tapNode" in triggered:
            cur_focus = node_clicked
        elif focus_changed:
            cur_focus = focus_value
        elif mode_changed:
            cur_focus = None
        else:
            # Fallback (sliders, etc.) - stick to history if available
            if history:
                cur_focus = history[-1]["node"]
            else:
                cur_focus = focus_value or node_clicked
        
        # Validation
        if cur_focus and mode != "tidy":
            # First check if the node exists in the graph
            if cur_focus not in nodes_df["id"].values:
                cur_focus = None
            elif mode == "bottomup":
                # In bottom-up mode, can't focus on root nodes
                roots = {n for n, ps in parents_map.items() if not ps}
                if cur_focus in roots:
                    cur_focus = None
        
        # Update history
        if cur_focus:
            new_entry = {"mode": mode, "node": cur_focus}
            should_update = mode_changed or (not history or history[-1] != new_entry)
            if should_update:
                history = history + [new_entry]
        elif mode_changed or focus_changed:
            history = []

    # No focus -> full graph
    if not cur_focus:
        elems = build_elements(nodes_df, edges_df, None)
        layout = {"name": "breadthfirst", "directed": True, "nodeDimensionsIncludeLabels": True, "spacingFactor": 1.3}
        stylesheet = base_stylesheet(curve_style=("taxi" if mode == "bottomup" else "straight"))
        return elems, layout, no_update, history, stylesheet

    # Subgraph generation
    if mode == "topdown":
        positions, keep, sub_edges = create_all_paths.compute_subtree_positions(
            nodes_df, edges_df, root_ids=cur_focus, max_depth=depth,
            layer_gap=160, col_gap=240,
            vertical_by_level=False,
            stagger_same_level=False,
            stagger_fraction=0.5,
            min_sep_px=160,
        )
        sub_nodes = nodes_df[nodes_df["id"].isin(keep)].copy()
        roots = cur_focus if isinstance(cur_focus, list) else [cur_focus]
        
    elif mode == "bottomup":
        # Bottom-up: get ancestors of the focus node
        cf = cur_focus[0] if isinstance(cur_focus, list) else cur_focus
        if not cf:
            # No focus - show full graph
            elems = build_elements(nodes_df, edges_df, None)
            layout = {"name": "breadthfirst", "directed": True, "nodeDimensionsIncludeLabels": True, "spacingFactor": 1.3}
            stylesheet = base_stylesheet(curve_style="taxi")
            return elems, layout, no_update, history, stylesheet
        
        keep = ancestors_of(cf, parents_map, max_depth=depth)
        sub_nodes = nodes_df[nodes_df["id"].isin(keep)].copy()
        sub_edges = edges_df[edges_df["source"].isin(keep) & edges_df["target"].isin(keep)].copy()
        
        # Compute bottom-up positions
        positions, bottom_up_levels = create_all_paths._rank_bottom_up(sub_nodes, keep, parents_map)
        
        # Invert Y coordinates so leaves (focus node) appear at the bottom, roots (ancestors) at the top
        if positions:
            # Calculate max_y from positions directly to be robust
            all_ys = [p["y"] for p in positions.values()]
            if all_ys:
                max_y = max(all_ys)
                for node_id in positions:
                    positions[node_id]["y"] = max_y - positions[node_id]["y"]
        
        # Roots for bottom-up are the top-level ancestors (nodes with no parents in the subgraph)
        roots = sorted([n for n in keep if not any(p in keep for p in parents_map.get(n, []))])
        
    else:
        # Tidy mode or other - shouldn't reach here for graph rendering
        elems = build_elements(nodes_df, edges_df, None)
        layout = {"name": "breadthfirst", "directed": True, "nodeDimensionsIncludeLabels": True, "spacingFactor": 1.3}
        stylesheet = base_stylesheet(curve_style="straight")
        return elems, layout, no_update, history, stylesheet

    # Build elements with positions
    elems = build_elements(sub_nodes, sub_edges, positions)

    # Layout and stylesheet selection
    # Both top-down and bottom-up use preset layout with computed positions
    layout = {"name": "preset", "fit": True, "padding": 40, "animate": False}
    
    # Stylesheet based on mode
    if mode == "bottomup":
        edge_style = {"curve-style": "taxi", "taxi-direction": "vertical", "taxi-turn-min-distance": 12}
        stylesheet = base_stylesheet(curve_style="taxi", extra_edge=edge_style)
    else:
        stylesheet = base_stylesheet(curve_style="straight")

    zoom = no_update
    if mode == "topdown" and "graph.tapNode" in triggered and tap_node:
        layout = {**layout, "fit": True, "padding": 100}

    if "fit-btn.n_clicks" in triggered and fit_clicks:
        layout = {**layout, "fit": True}

    return elems, layout, zoom, history, stylesheet

# 6. Tidy Tables Update
@app.callback(
    Output("parents-table", "data"),
    Output("children-table", "data"),
    Output("parents-count", "children"),
    Output("children-count", "children"),
    Output("parents-more-btn", "style"),
    Output("children-more-btn", "style"),
    Output("tidy-focus-card", "children"),
    Input("mode-radio", "value"),
    Input("focus-dropdown", "value"),
    Input("tidy-density", "value"),
    Input("parents-more-btn", "n_clicks"),
    Input("children-more-btn", "n_clicks"),
    Input("graph-store", "data"),
)
def update_tidy_tables(mode, focus_id, density, p_clicks, c_clicks, store_data):
    if not store_data or mode != "tidy" or not focus_id:
        return [], [], "", "", {"display": "none"}, {"display": "none"}, html.Div("Select a node to see its neighborhood.")

    nodes_df, edges_df, parents_map, children_map, level_by_id, balance_by_id, in_deg, out_deg = parse_store_data(store_data)

    def get_tidy_rows(ids):
        rows = []
        for nid in ids:
            lvl = level_by_id.get(nid, None)
            bal = balance_by_id.get(nid, None)
            val_str = format_money(bal) if pd.notna(bal) else ""
            
            label = nodes_df.loc[nodes_df['id']==nid, 'label'].iloc[0] if (nodes_df['id']==nid).any() else nid
            
            rows.append({
                "id": nid,
                "label": label,
                "level": (int(lvl) if pd.notna(lvl) else None),
                "in_deg": int(in_deg.get(nid, 0)),
                "out_deg": int(out_deg.get(nid, 0)),
                "value": val_str,
            })
        return rows

    parents = parents_map.get(focus_id, [])
    kids = children_map.get(focus_id, [])

    parents_rows_all = sorted(get_tidy_rows(parents), key=lambda r: (-r["in_deg"]-r["out_deg"], r["label"]))
    children_rows_all = sorted(get_tidy_rows(kids),    key=lambda r: (-r["in_deg"]-r["out_deg"], r["label"]))

    step = 50
    base_cap = DENSITY_CAP.get(density or "med", 30)
    eff_p_cap = base_cap + (p_clicks or 0) * step
    eff_c_cap = base_cap + (c_clicks or 0) * step

    p_total = len(parents_rows_all)
    c_total = len(children_rows_all)

    parents_rows = parents_rows_all[:eff_p_cap]
    children_rows = children_rows_all[:eff_c_cap]

    p_more = max(0, p_total - eff_p_cap)
    c_more = max(0, c_total - eff_c_cap)

    p_style = {"display": "inline-block"} if p_more > 0 else {"display": "none"}
    c_style = {"display": "inline-block"} if c_more > 0 else {"display": "none"}

    p_count = f"({p_total} total)" if p_total else "(0)"
    c_count = f"({c_total} total)" if c_total else "(0)"

    focus_label = nodes_df.loc[nodes_df["id"] == focus_id, "label"]
    focus_label = focus_label.iloc[0] if not focus_label.empty else focus_id
    
    sibs = sorted({s for p in parents for s in children_map.get(p, []) if s != focus_id})

    bal = balance_by_id.get(focus_id)
    val_str = format_money(bal) if pd.notna(bal) else "—"

    card = html.Div([
        html.Div(focus_label, style={"fontWeight": 700, "fontSize": "16px", "marginBottom": "6px"}),
        html.Div([
            html.Span(f"Level: {level_by_id.get(focus_id, '—')}", style={"marginRight": "12px"}),
            html.Span(f"Value: {val_str}", style={"marginRight": "12px"}),
            html.Span(f"Parents: {len(parents)}", style={"marginRight": "12px"}),
            html.Span(f"Children: {len(kids)}",   style={"marginRight": "12px"}),
            html.Span(f"Siblings: {len(sibs)}"),
        ], style={"color": "#374151", "marginBottom": "6px"}),
        html.Div("Tip: click a row to drill into that node.", style={"color": "#6b7280"}),
    ])

    return parents_rows, children_rows, p_count, c_count, p_style, c_style, card

# 7. Drill Down from Tidy Table
@app.callback(
    Output("focus-dropdown", "value", allow_duplicate=True),
    Input("parents-table", "active_cell"),
    Input("children-table", "active_cell"),
    State("parents-table", "data"),
    State("children-table", "data"),
    prevent_initial_call=True,
)
def drill_from_tables(p_cell, c_cell, p_data, c_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "parents-table":
        cell = p_cell
        data = p_data
    elif trigger_id == "children-table":
        cell = c_cell
        data = c_data
    else:
        return dash.no_update

    if not cell or not data:
        return dash.no_update
    
    if cell["row"] < len(data):
        row = data[cell["row"]]
        return row.get("id")
        
    return dash.no_update

# 8. Hover Card Visibility and Content
@app.callback(
    Output("hover-card", "style"),
    Output("hover-card", "children"),
    Input("graph", "mouseoverNodeData"),
    Input("graph", "mouseoverEdgeData"),
    Input("mode-radio", "value"),
    Input("graph-store", "data"),
    prevent_initial_call=False,
)
def show_hover_info(node_hover, edge_hover, mode, store_data):
    base_style = {
        "position": "absolute", "bottom": "16px", "left": "16px",
        "maxWidth": "360px", "background": "rgba(255,255,255,0.95)",
        "border": "1px solid #e5e7eb", "borderRadius": "10px",
        "boxShadow": "0 2px 8px rgba(0,0,0,.08)", "padding": "10px 12px",
        "fontSize": "12px", "zIndex": 10,
    }
    
    if mode == "tidy" or not store_data:
        base_style["display"] = "none"
        return base_style, []
    
    base_style["display"] = "block"

    ctx = dash.callback_context
    if not ctx.triggered:
        return base_style, [html.B("Hover a node or edge to see details")]
    
    prop_id = ctx.triggered[0]["prop_id"]
    hover_data = None
    if "mouseoverNodeData" in prop_id:
        hover_data = node_hover
    elif "mouseoverEdgeData" in prop_id:
        hover_data = edge_hover
    
    if not hover_data:
        return base_style, [html.B("Hover a node to see details"), html.Br(), html.Span("Tip: click a node to focus its subtree.")]
    
    # Check if edge
    if "source" in hover_data and "target" in hover_data:
        src = hover_data.get("source")
        tgt = hover_data.get("target")
        
        # Look up raw value for better formatting
        val_str = hover_data.get("value", "-")
        if store_data and 'edges' in store_data:
             edges_df = pd.DataFrame(store_data['edges'])
             match = edges_df[(edges_df["source"] == src) & (edges_df["target"] == tgt)]
             if not match.empty:
                 raw_val = match.iloc[0].get("position_value")
                 val_str = format_money_full(raw_val)
        
        # Check target info from store
        nodes_df = pd.DataFrame(store_data['nodes'])
        target_row = nodes_df[nodes_df['id'] == tgt]
        target_bal = target_row.iloc[0]['balance'] if not target_row.empty else None
        target_bal_str = format_money_full(target_bal) if pd.notna(target_bal) else "-"
        
        is_hi_target = str(tgt).endswith("(HI)")
        
        return base_style, [
            html.Div([
                html.Div("Edge Details", style={"fontWeight": 600, "fontSize": "13px", "marginBottom": "4px"}),
                html.Div([html.Span("Source: ", style={"fontWeight": 600}), html.Span(src)]),
                html.Div([html.Span("Target: ", style={"fontWeight": 600}), html.Span(tgt)]),
                html.Div([html.Span("Edge Value: ", style={"fontWeight": 600}), html.Span(val_str, style={"fontWeight": 600, "color": "#2563eb"})], style={"marginTop": "4px", "padding": "4px", "backgroundColor": "#f0f9ff", "borderRadius": "4px"}),
                html.Div([html.Span("Target Total Value: ", style={"fontWeight": 600}), html.Span(target_bal_str)], style={"marginTop": "2px", "fontSize": "11px", "color": "#6b7280"}) if is_hi_target else None,
            ])
        ]

    # Node
    node_id = hover_data.get("id")
    nodes_df = pd.DataFrame(store_data['nodes'])
    edges_df = pd.DataFrame(store_data['edges'])
    
    row = nodes_df.loc[nodes_df["id"] == node_id]
    if row.empty:
        return base_style, [html.B(node_id or "(unknown)")]
    
    label = str(row.iloc[0]["label"])
    level = int(row.iloc[0]["level"]) if pd.notna(row.iloc[0]["level"]) else None
    bal_val = row.iloc[0].get("balance")
    bal_str = format_money_full(bal_val) if pd.notna(bal_val) else "—"
    
    in_deg = edges_df[edges_df["target"] == node_id].shape[0]
    out_deg = edges_df[edges_df["source"] == node_id].shape[0]
    
    return base_style, [
        html.Div([
            html.Div(label, style={"fontWeight": 600, "fontSize": "13px", "marginBottom": "4px"}),
            html.Div([
                html.Div([html.Span("ID: ", style={"fontWeight": 600}), html.Code(node_id)]),
                html.Div([html.Span("Level: ", style={"fontWeight": 600}), html.Span(str(level) if level is not None else "—")]),
                html.Div([html.Span("Balance: ", style={"fontWeight": 600}), html.Span(bal_str)]),
                html.Div([html.Span("In-degree: ", style={"fontWeight": 600}), html.Span(str(in_deg))]),
                html.Div([html.Span("Out-degree: ", style={"fontWeight": 600}), html.Span(str(out_deg))]),
            ]),
        ])
    ]

# 9. Path Highlighting
@app.callback(
    Output("graph", "stylesheet", allow_duplicate=True),
    Input("graph", "mouseoverNodeData"),
    Input("graph-store", "data"),
    Input("mode-radio", "value"),
    Input("highlight-toggle", "value"),
    State("graph", "elements"),
    prevent_initial_call=True,
)
def highlight_paths(hover_data, store_data, mode, toggle_value, elements):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update
    
    trigger_prop = ctx.triggered[0]["prop_id"]
    
    # Determine basic edge style based on mode
    if mode == "bottomup":
        edge_style_extra = {"curve-style": "taxi", "taxi-direction": "vertical", "taxi-turn-min-distance": 12}
        curve = "taxi"
    else:
        edge_style_extra = None
        curve = "straight"
        
    base = base_stylesheet(curve_style=curve, extra_edge=edge_style_extra)
    
    # If highlighting is disabled (toggle empty/None), always return base
    if not toggle_value or "on" not in toggle_value:
        return base

    # Reset triggers: if graph data changed (new date/drill) or mode changed
    if "graph-store" in trigger_prop or "mode-radio" in trigger_prop:
        return base
        
    if "mouseoverNodeData" in trigger_prop and not hover_data:
        return dash.no_update

    # Mouseover logic
    hovered_id = hover_data["id"]
    
    # Build graph structure
    parents = defaultdict(list)
    children = defaultdict(list)
    
    if elements:
        for el in elements:
            data = el.get("data", {})
            if "source" in data and "target" in data:
                s, t = data["source"], data["target"]
                parents[t].append(s)
                children[s].append(t)
                
    # Find all related nodes
    ancs = ancestors_of(hovered_id, parents)
    descs = descendants_of(hovered_id, children)
    highlight_nodes = ancs | descs
    
    # Construct new stylesheet
    new_styles = list(base)
    new_styles.append({"selector": "node", "style": {"opacity": 0.1}})
    new_styles.append({"selector": "edge", "style": {"opacity": 0.1}})
    
    for nid in highlight_nodes:
        new_styles.append({
            "selector": f"node[id='{nid}']", 
            "style": {
                "opacity": 1,
                "border-width": 3,
                "border-color": "#333", 
                "z-index": 9999
            }
        })
        
    for u in highlight_nodes:
        for v in children.get(u, []):
            if v in highlight_nodes:
                new_styles.append({
                    "selector": f"edge[source='{u}'][target='{v}']",
                    "style": {
                        "opacity": 1,
                        "width": 3,
                        "line-color": "#555",
                        "target-arrow-color": "#555",
                        "z-index": 9998
                    }
                })
                
    return new_styles

# =============================================================================
# Run
# =============================================================================
def create_dash_app(full_data=None, initial_node=None, initial_date=None, port=8052, 
                    inactivity_timeout=30, active_flag_dict=None):
    """
    Create and run the Dash app with optional pre-loaded data and initial selections.
    
    Args:
        full_data: Optional pre-loaded DataFrame with position data
        initial_node: Optional node ID to pre-select in focus dropdown
        initial_date: Optional date string (YYYY-MM-DD) to pre-select
        port: Port number for the Dash server
        inactivity_timeout: Minutes of inactivity before auto-shutdown (default: 30)
        active_flag_dict: Shared multiprocessing dict with 'active' key to track parent app state
    """
    global FULL_DATA, DATE_OPTIONS, DEFAULT_DATE
    
    # Load and prepare data
    FULL_DATA, DATE_OPTIONS, DEFAULT_DATE = load_and_prepare_data(full_data)
    
    # Create and set app layout
    app.layout = create_app_layout(initial_node=initial_node, initial_date=initial_date)
    
    # Track last activity time for timeout
    last_activity = [datetime.now()]  # Use list to allow modification in nested functions
    
    # Shared shutdown flag (list to allow modification from nested functions)
    shutdown_flag = [False]
    shutdown_reason = [""]  # Reason for shutdown
    
    # Track activity on all requests
    @app.server.before_request
    def track_activity():
        last_activity[0] = datetime.now()
    
    # Callback to check shutdown flag and show/hide overlay
    @app.callback(
        Output("shutdown-overlay", "style"),
        Output("shutdown-flag-store", "data"),
        Input("shutdown-check-interval", "n_intervals"),
        State("shutdown-flag-store", "data"),
    )
    def check_shutdown_status(n_intervals, current_flag):
        """Check if shutdown flag is set and update overlay visibility"""
        if shutdown_flag[0] and not current_flag:
            # Shutdown was triggered, show overlay
            overlay_style = {
                "display": "flex",
                "position": "fixed",
                "top": 0,
                "left": 0,
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0, 0, 0, 0.85)",
                "zIndex": 9999,
                "justifyContent": "center",
                "alignItems": "center",
                "flexDirection": "column"
            }
            return overlay_style, True
        elif shutdown_flag[0]:
            # Already showing, keep it visible
            overlay_style = {
                "display": "flex",
                "position": "fixed",
                "top": 0,
                "left": 0,
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0, 0, 0, 0.85)",
                "zIndex": 9999,
                "justifyContent": "center",
                "alignItems": "center",
                "flexDirection": "column"
            }
            return overlay_style, True
        else:
            # Not shutting down, hide overlay
            return {"display": "none"}, False
    
    # Function to check for shutdown conditions
    def check_shutdown():
        """Background thread to check for inactivity timeout or parent app shutdown"""
        while True:
            time.sleep(10)  # Check every 10 seconds for faster response
            # Check inactivity timeout
            if inactivity_timeout > 0:
                inactive_minutes = (datetime.now() - last_activity[0]).total_seconds() / 60
                if inactive_minutes >= inactivity_timeout:
                    print(f"\nShutting down Dash app due to {inactivity_timeout} minutes of inactivity...")
                    shutdown_flag[0] = True
                    shutdown_reason[0] = f"inactivity ({inactivity_timeout} minutes)"
                    # Wait 3 seconds for message to display, then exit
                    time.sleep(3)
                    os._exit(0)
            
            # Check if parent app is still active via shared dict
            if active_flag_dict is not None:
                try:
                    # Check if the flag is False or if the dict is no longer accessible (manager closed)
                    if not active_flag_dict.get('active', False):
                        print(f"\nShutting down Dash app: parent app has closed (active flag is False)...")
                        shutdown_flag[0] = True
                        shutdown_reason[0] = "parent application closed"
                        # Wait 3 seconds for message to display, then exit
                        time.sleep(3)
                        os._exit(0)
                except (OSError, ValueError, AttributeError, KeyError) as e:
                    # Manager has been closed or dict is no longer accessible
                    print(f"\nShutting down Dash app: parent app manager closed ({type(e).__name__})...")
                    shutdown_flag[0] = True
                    shutdown_reason[0] = "parent application closed"
                    # Wait 3 seconds for message to display, then exit
                    time.sleep(3)
                    os._exit(0)
    
    # Start shutdown monitoring thread
    shutdown_thread = threading.Thread(target=check_shutdown, daemon=True)
    shutdown_thread.start()
    
    
    
    url = f"http://127.0.0.1:{port}"
    
    def open_browser():
        """Open browser after a short delay to allow server to start"""
        time.sleep(1.5)
        webbrowser.open(url)
    
    # Start browser opening in a separate thread
    threading.Thread(target=open_browser, daemon=True).start()
    
    # Run the app
    print(f"Starting Dash app on {url}")
    if inactivity_timeout > 0:
        print(f"Auto-shutdown after {inactivity_timeout} minutes of inactivity")
    if active_flag_dict is not None:
        print(f"Monitoring parent app active flag")
    app.run(debug=False, host="127.0.0.1", port=port, use_reloader=False)