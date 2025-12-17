# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 13:23:09 2025

@author: mmullaney
"""

import pandas as pd
import unicodedata
from collections import deque, defaultdict

# =============================================================================
# Helpers
# =============================================================================
def norm_series(s: pd.Series) -> pd.Series:
    """
    Normalize labels (trim/collapse whitespace, Unicode NFKC).
    Matches your original behavior (keeps strings; no casefold by default).
    """
    s = s.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    s = s.map(lambda x: unicodedata.normalize("NFKC", x))
    return s


def build_edges(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    From a DataFrame with 'Source name' and 'Target name', build normalized, de-duplicated edges.
    Removes rows with NA endpoints and self-loops.
    Returns (edges_df, hi_total_values_dict) where:
    - edges_df has columns: ['src', 'dst', 'position_value', 'percentage']
    - hi_total_values_dict maps normalized (HI) node names to their total values
    
    Special handling for (HI) nodes:
    - For edges TO (HI) nodes, percentage = edge_value / total_HI_node_value
    - Total HI node value is calculated from where the (HI) node appears as Source name
    - For non-(HI) nodes, percentage = edge_value / sum_of_incoming_edges (standard logic)
    """
    # Ensure optional columns exist if not provided
    if "position_value" not in df.columns:
        df["position_value"] = 0.0
    
    # ---- Identify (HI) nodes using ORIGINAL names (before normalization) ----
    # This avoids erroneously combining nodes through normalization
    # Find all unique (HI) target nodes (original names ending with "(HI)")
    hi_target_names_orig = set()
    for target_name in df["Target name"].dropna().unique():
        if str(target_name).endswith("(HI)"):
            hi_target_names_orig.add(target_name)
    
    # Find all unique (HI) source nodes (original names ending with "(HI)")
    hi_source_names_orig = set()
    for source_name in df["Source name"].dropna().unique():
        if str(source_name).endswith("(HI)"):
            hi_source_names_orig.add(source_name)
    
    # Calculate total value for (HI) nodes using ORIGINAL names (before normalization)
    # For (HI) nodes: node value = sum of position_value grouped by Target name (where Target = (HI) node)
    # This gives the total value of the HI holding
    hi_total_values_orig = {}
    if hi_target_names_orig:
        hi_target_df = df[df["Target name"].isin(hi_target_names_orig)].copy()
        if not hi_target_df.empty:
            hi_totals_orig = hi_target_df.groupby("Target name")["position_value"].sum()
            hi_total_values_orig = hi_totals_orig.to_dict()
    
    # Now normalize names for edge aggregation
    # Original mapping: src = Source name, dst = Target name
    # For bottom-up calculation, we want value to flow from child to parent
    # So we interpret: src = child (Source), dst = parent (Target)
    
    # Safety check: Warn if normalization reduces unique count (merges distinct nodes)
    unique_sources_orig = df["Source name"].nunique()
    unique_targets_orig = df["Target name"].nunique()
    df = df.assign(
        src=norm_series(df["Source name"]),
        dst=norm_series(df["Target name"])
    )
    unique_sources_norm = df["src"].nunique()
    unique_targets_norm = df["dst"].nunique()
    
    if unique_sources_norm < unique_sources_orig:
        print(f"⚠️  WARNING: Normalization merged {unique_sources_orig - unique_sources_norm} distinct source names "
              f"({unique_sources_orig} → {unique_sources_norm})")
    if unique_targets_norm < unique_targets_orig:
        print(f"⚠️  WARNING: Normalization merged {unique_targets_orig - unique_targets_norm} distinct target names "
              f"({unique_targets_orig} → {unique_targets_norm})")
    
    # Create mapping from original names to normalized names for (HI) nodes
    # Map original (HI) target names to their normalized versions
    hi_target_orig_to_norm = {}
    if hi_target_names_orig:
        for orig_name in hi_target_names_orig:
            norm_name = norm_series(pd.Series([orig_name]))[0]
            hi_target_orig_to_norm[orig_name] = norm_name
    
    # Map original (HI) source names to their normalized versions
    hi_source_orig_to_norm = {}
    if hi_source_names_orig:
        for orig_name in hi_source_names_orig:
            norm_name = norm_series(pd.Series([orig_name]))[0]
            hi_source_orig_to_norm[orig_name] = norm_name
    
    # Create reverse mapping: normalized (HI) target names -> original names
    hi_nodes_norm = set(hi_target_orig_to_norm.values())
    
    # Map original (HI) target totals to normalized names
    # For (HI) nodes, we use Target name totals (sum of position_value where Target = (HI) node)
    hi_total_values = {}
    for orig_name, total_val in hi_total_values_orig.items():
        if orig_name in hi_target_orig_to_norm:
            norm_name = hi_target_orig_to_norm[orig_name]
            # If multiple original names normalize to same normalized name, sum them
            if norm_name in hi_total_values:
                hi_total_values[norm_name] += total_val
            else:
                hi_total_values[norm_name] = total_val
    
    # Drop self-loops and NAs
    df = df.dropna(subset=["src", "dst"])
    df = df[df["src"] != df["dst"]]

    # Aggregate value between same src/dst
    edges = (
        df.groupby(["src", "dst"], as_index=False)
        .agg({"position_value": "sum"})
    )
    
    # ---- Recalculate Percentages based on Inferred Total Balance ----
    # For (HI) nodes: Total value = sum of position_value grouped by Target name (where Target = (HI) node)
    # For non-(HI) nodes: Total value = sum of all incoming position values
    
    # Calculate total incoming value for each destination (standard calculation)
    total_in_dict = edges.groupby("dst")["position_value"].sum().to_dict()
    
    # Calculate percentages
    percentages = []
    for idx, row in edges.iterrows():
        dst = row["dst"]
        edge_val = row["position_value"]
        
        if dst in hi_nodes_norm and dst in hi_total_values:
            # (HI) node: do not calculate percentage, set to None
            percentages.append(None)
        else:
            # Non-(HI) node: use sum of incoming edges
            total_val = total_in_dict.get(dst, 0.0)
            if total_val > 0:
                percentages.append(edge_val / total_val)
            else:
                percentages.append(0.0)
    
    edges["percentage"] = percentages
    # Fill NaN for non-HI edges only, keep None/NaN for HI edges
    non_hi_mask = ~edges["dst"].isin(hi_nodes_norm)
    edges.loc[non_hi_mask, "percentage"] = edges.loc[non_hi_mask, "percentage"].fillna(0.0)
    
    return edges, hi_total_values


def topo_sort(edges: pd.DataFrame):
    """
    Kahn's algorithm over the edge list.
    """
    nodes = set(edges["src"]) | set(edges["dst"])
    in_deg = {n: 0 for n in nodes}
    children = defaultdict(list)

    for u, v in edges[["src", "dst"]].itertuples(index=False, name=None):
        in_deg[v] += 1
        children[u].append(v)

    q = deque([n for n, d in in_deg.items() if d == 0])
    visited, topo = set(), []
    processed = 0

    while q:
        u = q.popleft()
        if u in visited:
            continue
        visited.add(u)
        topo.append(u)
        for v in children.get(u, []):
            in_deg[v] -= 1
            processed += 1
            if in_deg[v] == 0:
                q.append(v)

    return {
        "is_dag": len(visited) == len(nodes),
        "topological_order": topo,
        "visited_count": len(visited),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "processed_edges": processed,
        "residual_in_deg": {n: d for n, d in in_deg.items() if d > 0},
        "children": children,
        "nodes": nodes,
    }


def graph_stats(edges: pd.DataFrame):
    """
    Convenience stats: roots (in-degree 0), leaves (out-degree 0), orphans (no in/out).
    Also returns in/out-degree dicts.
    """
    nodes = set(edges["src"]) | set(edges["dst"])
    in_counts = edges.groupby("dst").size()
    out_counts = edges.groupby("src").size()

    roots = sorted(n for n in nodes if n not in in_counts.index)     # in-degree 0
    leaves = sorted(n for n in nodes if n not in out_counts.index)   # out-degree 0
    orphans = sorted(
        n for n in nodes if n not in in_counts.index and n not in out_counts.index
    )

    return {
        "roots": roots,
        "leaves": leaves,
        "orphans": orphans,
        "in_degree": in_counts.to_dict(),
        "out_degree": out_counts.to_dict(),
    }


def build_original_label_map(df: pd.DataFrame) -> dict:
    """
    Map each normalized node -> one representative original spelling/casing.
    """
    tmp = df[["Source name", "Target name"]].melt(value_name="name").dropna()
    tmp["norm"] = norm_series(tmp["name"])
    rep = tmp.drop_duplicates("norm")[["norm", "name"]]  # first occurrence wins
    return dict(zip(rep["norm"], rep["name"]))


def extract_paths(children: dict[str, list[str]], roots: list[str], max_depth: int = 5):
    """
    Traverse the DAG up to `max_depth` and return list of paths:
    [Root, Level2, Level3, ... up to max_depth].
    """
    paths = []

    def dfs(path):
        node = path[-1]
        # stop at max depth or leaf
        if len(path) >= max_depth or node not in children or not children[node]:
            paths.append(path + [None] * (max_depth - len(path)))  # pad to length
            return
        for nxt in children[node]:
            dfs(path + [nxt])

    for r in roots:
        dfs([r])

    cols = [f"Level {i}" for i in range(1, max_depth + 1)]
    return pd.DataFrame(paths, columns=cols)


# =============================================================================
# Core Generator Function
# =============================================================================
def generate_graph_for_date(combined: pd.DataFrame, target_date: pd.Timestamp):
    """
    Generates nodes and edges DataFrames for a specific date from the combined history.
    """
    # 1. Filter by date
    if "As of date" in combined.columns:
        # Assuming cumulative/history is needed? Or just snapshot?
        # Based on previous logic, we want snapshot for target date.
        df_date = combined[combined["As of date"] == target_date].copy()
    else:
        df_date = combined.copy()

    if df_date.empty:
        # Return empty structures if no data
        return pd.DataFrame(), pd.DataFrame()

    # 2. Run pipeline
    edges, hi_total_values_from_edges = build_edges(df_date)
    res = topo_sort(edges)
    stats = graph_stats(edges)
    orig_label = build_original_label_map(df_date)

    # 3. Compute paths
    paths_df = extract_paths(res["children"], stats["roots"], max_depth=5)
    paths_df = paths_df.map(lambda x: orig_label.get(x, x) if pd.notna(x) else None)
    paths_df = paths_df.sort_values(
        by=list(paths_df.columns), 
        ascending=[True] * len(paths_df.columns),
        na_position='last'
    ).reset_index(drop=True)

    # 4. Build Edges (Cytoscape ready)
    cols = [c for c in paths_df.columns if c.lower().startswith("level")]
    paths_df_levels = paths_df[cols]
    
    edge_rows = []
    for i in range(len(cols) - 1):
        src_col, dst_col = cols[i], cols[i + 1]
        pair = (
            paths_df_levels[[src_col, dst_col]]
            .dropna()
            .drop_duplicates()
            .rename(columns={src_col: "source", dst_col: "target"})
        )
        edge_rows.append(pair)
    
    if edge_rows:
        edges_out = pd.concat(edge_rows, ignore_index=True).drop_duplicates()
    else:
        edges_out = pd.DataFrame(columns=["source", "target"])

    # Merge attributes
    edges_out = edges_out.merge(
        edges, 
        left_on=["source", "target"], 
        right_on=["src", "dst"], 
        how="left"
    ).drop(columns=["src", "dst"])

    edges_out["position_value"] = edges_out["position_value"].fillna(0)
    hi_target_mask = edges_out["target"].astype(str).str.endswith("(HI)")
    edges_out.loc[~hi_target_mask, "percentage"] = edges_out.loc[~hi_target_mask, "percentage"].fillna(1.0)

    # 5. Build Nodes
    nodes_long = (
        paths_df_levels.melt(var_name="level_col", value_name="label")
        .dropna()
        .drop_duplicates()
    )
    if not nodes_long.empty:
        nodes_long["level"] = nodes_long["level_col"].str.extract(r"(\d+)").astype(int)
        nodes_max_level = (
            nodes_long.groupby("label")["level"].max().reset_index()
            .rename(columns={"label": "id", "level": "max_level"})
        )
        nodes = nodes_max_level.rename(columns={"max_level": "level"}).copy()
        nodes["max_level"] = nodes["level"]
    else:
        nodes = pd.DataFrame(columns=["id", "label", "level", "max_level", "balance"])

    # 6. Calculate Balances
    orig_to_norm = {orig: norm_series(pd.Series([orig]))[0] for orig in nodes["id"].unique()}
    
    outgoing_edge_sums = edges.groupby("src")["position_value"].sum().to_dict()
    incoming_edge_sums = edges.groupby("dst")["position_value"].sum().to_dict()
    leaves_norm = set(stats["leaves"])
    
    balance_map = {}
    all_nodes_norm = set(res["nodes"])
    
    for n in all_nodes_norm:
        if n in leaves_norm:
            balance_map[n] = incoming_edge_sums.get(n, 0.0)
        else:
            balance_map[n] = outgoing_edge_sums.get(n, 0.0)
        
        if incoming_edge_sums.get(n, 0.0) > balance_map[n]:
            balance_map[n] = incoming_edge_sums.get(n, 0.0)

    # HI Parent Adjustment
    hi_nodes_norm_set = set()
    for node_id in nodes["id"]:
        if str(node_id).endswith("(HI)"):
            node_norm = orig_to_norm.get(node_id, norm_series(pd.Series([node_id]))[0])
            hi_nodes_norm_set.add(node_norm)
    
    parents_of_hi = set()
    for idx, row in edges.iterrows():
        if row["dst"] in hi_nodes_norm_set:
            parents_of_hi.add(row["src"])
            
    for parent_norm in parents_of_hi:
        p_bal = balance_map.get(parent_norm, 0.0)
        p_inc = incoming_edge_sums.get(parent_norm, 0.0)
        if p_bal > p_inc:
            balance_map[parent_norm] = p_inc

    nodes["balance"] = nodes["id"].map(lambda x: balance_map.get(orig_to_norm.get(x, x), 0.0))
    if not nodes.empty:
        nodes = nodes.sort_values(by=['level','id'], ascending=[True,True])
        # Add label column (same as id for display purposes)
        nodes["label"] = nodes["id"]

    # 7. Edges to HI Adjustment
    hi_nodes_norm_balance = hi_nodes_norm_set # same set
    parent_to_hi_children = {}
    for parent_norm, children_list in res["children"].items():
        hi_children = [child for child in children_list if child in hi_nodes_norm_balance]
        if hi_children:
            parent_to_hi_children[parent_norm] = hi_children

    for idx, row in edges_out.iterrows():
        target = str(row["target"])
        source = str(row["source"])
        if target.endswith("(HI)"):
            source_norm = orig_to_norm.get(source, norm_series(pd.Series([source]))[0])
            target_norm = orig_to_norm.get(target, norm_series(pd.Series([target]))[0])
            original_edge_value = row["position_value"]
            hi_node_total_value = hi_total_values_from_edges.get(target_norm, 0.0)
            hi_children_of_parent = parent_to_hi_children.get(source_norm, [])
            parent_balance = balance_map.get(source_norm, 0.0)

            if len(hi_children_of_parent) == 1 and original_edge_value < hi_node_total_value:
                edges_out.at[idx, "position_value"] = original_edge_value
            elif hi_children_of_parent:
                sum_hi_values = sum(hi_total_values_from_edges.get(child, 0.0) for child in hi_children_of_parent)
                if sum_hi_values > parent_balance and parent_balance > 0:
                    hi_node_pct = hi_node_total_value / sum_hi_values if sum_hi_values > 0 else 0.0
                    edges_out.at[idx, "position_value"] = parent_balance * hi_node_pct
                else:
                    edges_out.at[idx, "position_value"] = hi_node_total_value
            else:
                edges_out.at[idx, "position_value"] = hi_node_total_value
            
            edges_out.at[idx, "percentage"] = None

    return nodes, edges_out


def compute_subtree_positions(nodes_df, edges_df, root_ids, max_depth=5, 
                              layer_gap=160, col_gap=240, vertical_by_level=False,
                              stagger_same_level=False, stagger_fraction=0.5, min_sep_px=160):
    """
    Compute positions for nodes in a subtree starting from root_ids.
    Returns (positions_dict, keep_set, sub_edges_df)
    """
    from collections import deque, defaultdict
    
    # Build children map
    children_map = defaultdict(list)
    for _, row in edges_df.iterrows():
        children_map[str(row["source"])].append(str(row["target"]))
    
    # Normalize root_ids to list
    if root_ids is None:
        return {}, set(), pd.DataFrame()
    root_list = root_ids if isinstance(root_ids, list) else [root_ids]
    root_list = [str(r) for r in root_list]
    
    # BFS to collect all nodes in subtree
    keep = set()
    queue = deque([(r, 0) for r in root_list])  # (node_id, depth)
    
    while queue:
        node_id, depth = queue.popleft()
        if node_id in keep or depth > max_depth:
            continue
        keep.add(node_id)
        
        for child in children_map.get(node_id, []):
            if child not in keep:
                queue.append((child, depth + 1))
    
    # Filter edges to only those in the subtree
    sub_edges = edges_df[
        edges_df["source"].isin(keep) & edges_df["target"].isin(keep)
    ].copy()
    
    # Compute positions using level-based layout
    positions = {}
    level_to_nodes = defaultdict(list)
    
    # Assign levels (distance from root)
    node_levels = {}
    queue = deque([(r, 0) for r in root_list])
    while queue:
        node_id, level = queue.popleft()
        if node_id in node_levels:
            continue
        node_levels[node_id] = level
        level_to_nodes[level].append(node_id)
        
        for child in children_map.get(node_id, []):
            if child in keep and child not in node_levels:
                queue.append((child, level + 1))
    
    # Position nodes by level
    for level, nodes_at_level in sorted(level_to_nodes.items()):
        y = level * layer_gap
        x_start = -(len(nodes_at_level) - 1) * col_gap / 2
        
        for i, node_id in enumerate(nodes_at_level):
            x = x_start + i * col_gap
            positions[node_id] = {"x": x, "y": y}
    
    return positions, keep, sub_edges


def _rank_bottom_up(sub_nodes, keep, parents_map):
    """
    Compute positions for bottom-up layout.
    Returns (positions_dict, level_dict)
    """
    from collections import deque, defaultdict
    
    # Strategy 1: Use absolute 'level' column if available for strict layering
    if "level" in sub_nodes.columns:
        relevant = sub_nodes[sub_nodes["id"].isin(keep)].copy()
        relevant["level"] = pd.to_numeric(relevant["level"], errors='coerce')
        
        # Only use if we have valid levels for most nodes
        if relevant["level"].notna().sum() > 0:
            max_abs = relevant["level"].max()
            level_map = {}
            for _, row in relevant.iterrows():
                if pd.notna(row["level"]):
                    # Focus (Deepest Level) -> Rank 0
                    # Root (Level 1) -> Rank (Max-1)
                    # This matches BFS output structure so existing inversion logic works
                    level_map[str(row["id"])] = int(max_abs - row["level"])
            
            # Compute positions
            level_to_nodes = defaultdict(list)
            for node_id, lvl in level_map.items():
                level_to_nodes[lvl].append(node_id)
            
            # Build children map for barycenter sorting (minimizing edge crossings)
            children_map = defaultdict(list)
            for child in keep:
                for p in parents_map.get(child, []):
                    if p in keep:
                        children_map[p].append(child)

            positions = {}
            layer_gap = 160
            col_gap = 240
            
            for lvl, nodes_at_level in sorted(level_to_nodes.items()):
                # Sort nodes: 
                # If lvl == 0 (leaves), sort by ID.
                # If lvl > 0, sort by average X of children in previous levels (which are already placed)
                if lvl == 0:
                    nodes_at_level.sort()
                else:
                    def sort_key(nid):
                        # Find children of this node that have already been placed
                        placed_kids = [k for k in children_map.get(nid, []) if k in positions]
                        if not placed_kids:
                            return (0, nid) # Fallback to ID
                        avg_x = sum(positions[k]["x"] for k in placed_kids) / len(placed_kids)
                        return (avg_x, nid)
                    
                    nodes_at_level.sort(key=sort_key)

                y = lvl * layer_gap
                x_start = -(len(nodes_at_level) - 1) * col_gap / 2
                
                for i, node_id in enumerate(nodes_at_level):
                    x = x_start + i * col_gap
                    positions[node_id] = {"x": x, "y": y}
            
            return positions, level_map

    # Strategy 2: BFS from leaves (Fallback)
    # Find leaves (nodes with no children in the subgraph)
    children_in_subgraph = defaultdict(set)
    for node_id in keep:
        for parent in parents_map.get(node_id, []):
            if parent in keep:
                children_in_subgraph[parent].add(node_id)
    
    leaves = [n for n in keep if n not in children_in_subgraph or not children_in_subgraph[n]]
    
    # BFS from leaves upward
    level_map = {}
    queue = deque([(leaf, 0) for leaf in leaves])
    
    while queue:
        node_id, level = queue.popleft()
        if node_id in level_map:
            level_map[node_id] = max(level_map[node_id], level)
            continue
        level_map[node_id] = level
        
        for parent in parents_map.get(node_id, []):
            if parent in keep:
                queue.append((parent, level + 1))
    
    # Compute positions
    level_to_nodes = defaultdict(list)
    for node_id, level in level_map.items():
        level_to_nodes[level].append(node_id)
    
    # Build children map for barycenter sorting (minimizing edge crossings)
    children_map = defaultdict(list)
    for child in keep:
        for p in parents_map.get(child, []):
            if p in keep:
                children_map[p].append(child)

    positions = {}
    layer_gap = 160
    col_gap = 240
    
    for level, nodes_at_level in sorted(level_to_nodes.items()):
        # Sort nodes: 
        # If level == 0 (leaves), sort by ID.
        # If level > 0, sort by average X of children in previous levels (which are already placed)
        if level == 0:
            nodes_at_level.sort()
        else:
            def sort_key(nid):
                # Find children of this node that have already been placed
                placed_kids = [k for k in children_map.get(nid, []) if k in positions]
                if not placed_kids:
                    return (0, nid) # Fallback to ID
                avg_x = sum(positions[k]["x"] for k in placed_kids) / len(placed_kids)
                return (avg_x, nid)
            
            nodes_at_level.sort(key=sort_key)

        y = level * layer_gap
        x_start = -(len(nodes_at_level) - 1) * col_gap / 2
        
        for i, node_id in enumerate(nodes_at_level):
            x = x_start + i * col_gap
            positions[node_id] = {"x": x, "y": y}
    
    return positions, level_map