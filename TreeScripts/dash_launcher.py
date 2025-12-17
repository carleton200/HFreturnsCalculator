# -*- coding: utf-8 -*-
"""
Dash App Launcher Module

This module provides functionality to launch the Tree Hierarchy Viewer Dash app
from the main Returns Calculator application, with database integration and
optional pre-selection of nodes and dates.
"""

import os
import sys
import pandas as pd
from typing import Optional
import io
import pickle
import TreeScripts.unifiedv2 as unifiedv2


def launch_dash_app(data_pickle: bytes, initial_node: Optional[str] = None, 
                    initial_date: Optional[str] = None, port: int = 8052,
                    inactivity_timeout: int = 30, active_flag_dict: Optional[dict] = None):
    """
    Main entry point for launching Dash app with pre-loaded data.
    
    Args:
        data_pickle: Pickled DataFrame with position data
        initial_node: Optional node name to pre-select in the focus dropdown
        initial_date: Optional date string (YYYY-MM-DD format) to pre-select
        port: Port number for the Dash server (default: 8052)
        inactivity_timeout: Minutes of inactivity before auto-shutdown (default: 30)
        active_flag_dict: Shared multiprocessing dict with 'active' key to track parent app state
    """
    print("=" * 60)
    print("Launching Tree Hierarchy Viewer")
    print("=" * 60)
    
    # Unpickle the data
    try:
        full_data = pickle.loads(data_pickle)
        print(f"Data loaded successfully: {len(full_data)} rows")
    except Exception as e:
        print(f"ERROR: Failed to unpickle data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if full_data.empty:
        print("ERROR: No data loaded. Cannot launch Dash app.")
        return
    
    # Import the Dash app creator
    # We need to modify unifiedv2.py to export a create_dash_app function
    try:
        # Add current directory to path if needed
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        
        
        # Create and run the Dash app
        print(f"Starting Dash server on port {port}...")
        print(f"Initial node: {initial_node if initial_node else 'None (full view)'}")
        print(f"Initial date: {initial_date if initial_date else 'Latest available'}")
        
        # Call the create_dash_app function (we'll create this in unifiedv2.py)
        unifiedv2.create_dash_app(
            full_data=full_data,
            initial_node=initial_node,
            initial_date=initial_date,
            port=port,
            inactivity_timeout=inactivity_timeout,
            active_flag_dict=active_flag_dict
        )
        
    except Exception as e:
        print(f"Error launching Dash app: {e}")
        import traceback
        traceback.print_exc()

