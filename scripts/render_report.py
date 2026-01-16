import math
import statistics
from PyQt5.QtWidgets import QFileDialog
from scripts.commonValues import maxPDFheaderUnits, maxRowPadding, maxRowsPerPage, minRowPadding, minimumFontSize, shrinkPDFthreshold, standardFontSize
from scripts.instantiate_basics import ASSETS_DIR
from reportlab.lib.units import inch
from datetime import datetime
from pathlib import Path
import matplotlib
import markdown
import sys
import os

from scripts.basicFunctions import separateRowCode
from scripts.pdf_generator import PDFReportGenerator, CONTENT_WIDTH
from reportlab.platypus import PageBreak, Spacer, Table, TableStyle, KeepTogether
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd


def get_base_dir():
    # Script mode
    if hasattr(sys.modules["__main__"], "__file__"):
        return Path(sys.modules["__main__"].__file__).resolve().parent
    # Notebook / Spyder / IPython mode
    return Path(".").resolve()


def format_number(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return f"{value:,.2f}"  


def get_holdings_row_class(row_type): #TODO: update with proper dataTypes
    """Map type to CSS class for holdings table rows."""
    type_lower = str(row_type).lower().strip()
    
    if type_lower == 'total':
        return 'holdings-total'
    elif type_lower == 'total assetclass' or type_lower == 'total subassetclass':
        return 'holdings-subclass'
    elif type_lower == 'total node':
        return 'holdings-pool'
    elif type_lower == 'benchmark':
        return 'holdings-benchmark'
    else:
        return 'holdings-investment'  # fallback


def create_page_groups(dataTypes, portfolio_holdings_rows, colorDepths, rows_per_page=maxRowsPerPage):
    """
    Split rows into page groups with a fixed number of rows per page.
    first_page_rows: Number of rows for the very first page.
    rows_per_page: Number of rows for all subsequent pages.
    """
    pages = []
    current_page = []

    colorPages = []
    currentColorPage = []

    headerSkipOpts = ('Total assetClass','Total Node', 'Total subAssetClass','Total sleeve')
        
    for i, row in enumerate(portfolio_holdings_rows):
        # Check if adding this row would exceed the current limit
        dType = dataTypes[i]
        if (len(current_page) >= rows_per_page or (rows_per_page - len(current_page) <= 5 and dType in headerSkipOpts) 
            or (rows_per_page - len(current_page) < 12   and   not any(dType.lower() == 'total target name' for dType in dataTypes[i:min(len(dataTypes) - 1,(i + rows_per_page - len(current_page)))]))):
            #skip if end of page or a header is at the base of the page OR no investments are shown before the ending (caps at 12 slots of headers/benchmarks)
            pages.append(current_page)
            current_page = [row]
            colorPages.append(currentColorPage)
            currentColorPage = [colorDepths[i],]
            # After the first page is filled, switch to the standard limit for all subsequent pages
        else:
            current_page.append(row)
            currentColorPage.append(colorDepths[i])
    
    # Add the last page if it has any rows
    if current_page:
        pages.append(current_page)
        colorPages.append(currentColorPage)
    
    return pages, colorPages


def create_benchmark_chart(benchmarks_df, column, title, output_path, y_lim=None, colors=None):
    """
    Create a vertical bar chart for benchmark returns.
    
    benchmarks_df: DataFrame with Benchmark column and return columns
    column: Column name to plot (e.g., 'MTD', '1Y')
    title: Chart title
    output_path: Path to save the chart image
    y_lim: Tuple (ymin, ymax) for shared y-axis limits
    colors: Optional list of colors for each bar
    """
    # Filter out rows with NaN values for this column
    data = benchmarks_df[['Benchmark', column]].copy()
    data = data.dropna(subset=[column])
    
    if len(data) == 0:
        return None
    
    # Default colors matching the image description
    # MSCI ACWI: dark blue, Barclays: light gray, 60/40: dark red, MSCI EM: yellow, CPPI: teal, PE: gray
    if colors is None:
        color_map = {
            'MSCI ACWI': '#1f4e78',
            'Barclays US Aggregate': '#d0d0d0',
            '60% MSCI ACWI / 40% BB Aggregate': '#8b0000',
            'MSCI EM': '#ffd700',
            'Commercial Property Prices Index': '#008080',
            'PE Benchmark': '#808080'
        }
        colors = [color_map.get(bench, '#808080') for bench in data['Benchmark']]
    
    fig, ax = plt.subplots(figsize=(6, 5))
    
    # Convert values to percentages (multiply by 100)
    data_percent = data[column]
    
    # Create vertical bar chart
    x_pos = range(len(data))
    bars = ax.bar(x_pos, data_percent, color=colors)
    
    # Format axes
    ax.set_title(title, fontsize=10, fontweight='bold', pad=20)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(data['Benchmark'], rotation=45, ha='right', fontsize=8)
    
    # Remove y-axis display
    ax.set_yticks([])
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Calculate max absolute value for positioning labels (in percentage)
    max_abs_val = data_percent.abs().max()
    if y_lim:
        # y_lim is already in percentage format (multiplied by 100)
        max_abs_val = max(abs(y_lim[0]), abs(y_lim[1]))
    
    # Add value labels on bars (with 1 decimal place)
    for i, (bar, val) in enumerate(zip(bars, data_percent)):
        if val >= 0:
            ax.text(i, val + max_abs_val * 0.02, f'{val:.1f}%', 
                   ha='center', fontsize=8, va='bottom')
        else:
            ax.text(i, val - max_abs_val * 0.02, f'({abs(val):.1f}%)', 
                   ha='center', fontsize=8, va='top', color='red')
    
    # Add zero line
    ax.axhline(y=0, color='black', linewidth=0.5)
    
    # Set y-axis limits (use shared limits if provided, already in percentage)
    if y_lim:
        ax.set_ylim(y_lim)
    else:
        # Calculate dynamic limits based on actual data
        min_val = data_percent.min()
        max_val = data_percent.max()
        
        # Add dynamic padding based on actual min/max values
        padded_min = min_val - abs(min_val) * 0.05
        y_min = min(padded_min, -3.0)
        
        padded_max = max_val + abs(max_val) * 0.05
        y_max = max(padded_max, 3.0)
        
        ax.set_ylim(y_min, y_max)
    
    # Adjust layout
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return output_path


def render_report(out_path, holdingDict, colorDepths, snapshotWb = None, benchmarks_df = None,JPMdf = None, narrative_text = None, 
                  holdings_exclude_keys = None, holdings_header_order = None, onlyHoldings = False, footerData={}):
    print(f"Report generation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    holdings_header_order = ['',*holdings_header_order]
    report_date = footerData.get('reportDate')
    sourceName = footerData.get('portfolioSource','HF Capital')

    """Scale portfolio holdings table sizes"""
    headerUnits = footerData.get('headerUnits',0.0)
    sFs = standardFontSize
    if headerUnits > shrinkPDFthreshold:
        mFs = minimumFontSize
        shrinkT = shrinkPDFthreshold
        scaleRatio = (headerUnits - shrinkT)/(maxPDFheaderUnits - shrinkT)
        #max of minimum size OR (standard - font adjustment range * unit overreach / unit overeach range)
        tableFontSize = max(mFs,sFs - (sFs-mFs) * scaleRatio)
        rowPadding = max(minRowPadding, maxRowPadding - (maxRowPadding - minRowPadding) * scaleRatio)
        rowShrinkage = math.floor(maxRowsPerPage * (maxRowPadding - rowPadding) * (1/4)) #amount of row space freed under assumption the padding is 1/4 of the row
    else:
        tableFontSize = standardFontSize
        rowPadding = maxRowPadding
        rowShrinkage = 0

    if snapshotWb and not onlyHoldings:
        try:
            dfs = snapshotWb
            # Assets and Flows-------
            af = dfs["assets_and_flows"].copy()
            af = af.iloc[:, :4]
            af.columns = ["label", "month", "year_1", "inception"]
            assets_and_flows_rows = af.to_dict(orient="records")

            # Read that sheet. Usually header row is the first row with
            # ['', 'Month', 'QTD', 'YTD', '1 Year', '3 Year', 'Inception']
            pr = dfs['portfolio returns']
            portfolio_returns_rows = pr.to_dict(orient="records")

            # 3b) Overall Family Breakdown (right panel)
            ofb = dfs["overall_family_breakdown"].copy()

            # Assume first 5 cols: Asset, LM $MM, Δ, CM $MM, %
            ofb = ofb.iloc[:, :5]
            ofb.columns = ["asset", "lm_mm", "delta_mm", "cm_mm", "pct"]
            overall_family_breakdown_rows = ofb.to_dict(orient="records")

            # 3c) HF Foundations
            # Find key that matches 'foundations' case-insensitively
            foundations_key = next((k for k in dfs.keys() if k.lower() == "foundations"), None)
            if foundations_key:
                hff = dfs[foundations_key].copy()
                # Expected columns: Asset Class, $MM, % Allocation
                # Take first 3 columns
                hff = hff.iloc[:, :3]
                hff.columns = ["asset_class", "mm", "pct"]
                hf_foundations_rows = hff.to_dict(orient="records")
            else:
                print("Warning: 'foundations' sheet not found in loaded keys:", list(dfs.keys()))
                hf_foundations_rows = []

            # 3d) Sports (under Overall Family Breakdown)
            # Look for "overall_family_breakdown2"
            sports_key = next((k for k in dfs.keys() if "breakdown2" in k.lower()), None)
            if sports_key:
                sprT = dfs[sports_key].copy()
                # Expected columns: Sports, Share %, Team Value, Debt, Equity, Family Share
                # Take first 6 columns
                sprT = sprT.iloc[:, :6]
                sprT.columns = ["sports", "share_pct", "team_value", "debt", "equity", "family_share"]
                sports_rows = sprT.to_dict(orient="records")
            else:
                print("Warning: 'overall_family_breakdown2' sheet not found in loaded keys:", list(dfs.keys()))
                sports_rows = []

            # 3e) Returns vs Benchmark
            rvb_key = next((k for k in dfs.keys() if "returns_vs_benchmark" in k.lower()), None)
            if rvb_key:
                rvb = dfs[rvb_key].copy()
                # Expecting ~20 columns
                # 0: Asset Class, 1: $MM
                # 2-4: Alloc vs Target (%, Tgt %, Delta $)
                # 5-7: Month (RTN, BM, Delta)
                # 8-10: YTD
                # 11-13: 1 Year
                # 14-16: 3 Year
                # 17-19: Inception
                rvb = rvb.iloc[:, :20]
                rvb.columns = [
                    "asset_class", "mm",
                    "alloc_pct", "tgt_pct", "alloc_delta",
                    "m_rtn", "m_bm", "m_delta",
                    "ytd_rtn", "ytd_bm", "ytd_delta",
                    "y1_rtn", "y1_bm", "y1_delta",
                    "y3_rtn", "y3_bm", "y3_delta",
                    "inc_rtn", "inc_bm", "inc_delta"
                ]
                returns_vs_benchmark_rows = rvb.to_dict(orient="records")
            else:
                print("Warning: 'returns_vs_benchmark' sheet not found in loaded keys:", list(dfs.keys()))
                returns_vs_benchmark_rows = []
        except Exception as e:
            print(f'WARNING: Error occured processing snapshot data for report export: {e.args}')
    else:
        hf_foundations_rows = []
        assets_and_flows_rows = []
        portfolio_returns_rows = []
        overall_family_breakdown_rows = []
        returns_vs_benchmark_rows = []
        sports_rows = []
    try:
        portfolio_holdings_rows = []
        dataTypes = []
        # Default exclude keys (metadata that shouldn't be columns)
        baseExclusions = ['dataType', 'type', 'row_class', 'show_border', 'rowKey']
        if holdings_exclude_keys is None:
            holdings_exclude_keys = baseExclusions
        else:
            holdings_exclude_keys.extend(baseExclusions)
        
        # Process holdingDict without conversions - use original keys
        for rowKey, rowDict in holdingDict.items():
            row = rowDict.copy()
            rowName, _ = separateRowCode(rowKey)
            investment = rowName
            row[''] = investment
            # Store type separately (it's used for styling but shouldn't be a column)
            row["type"] = rowDict.get('dataType', '')
            dataTypes.append(rowDict.get('dataType', ''))
            # Add CSS class based on type
            row["row_class"] = get_holdings_row_class(row["type"])
            # Determine if border should appear below (for Total and Category rows)
            row["show_border"] = row["type"].lower() in ['total', 'assetClass', 'subAssetClass']
            portfolio_holdings_rows.append(row)
        
        # Split rows into page groups
        portfolio_holdings_pages, colorDepthPages = create_page_groups(dataTypes, portfolio_holdings_rows, colorDepths, rows_per_page=(maxRowsPerPage + rowShrinkage))
        
        print(f"Loaded {len(portfolio_holdings_rows)} portfolio holdings rows")
        print(f"Split into {len(portfolio_holdings_pages)} pages")
    except Exception as e:
        print(f"Warning: Error loading portfolio holdings: {e}")
        import traceback
        traceback.print_exc()
        portfolio_holdings_pages = []
        colorDepthPages = []

    # 3g) Benchmarks (World Markets page)
    benchmarks_rows = []
    benchmark_chart_mtd_path = None
    benchmark_chart_1y_path = None
    
    if benchmarks_df and not onlyHoldings:
        try:
            # Expected columns: Benchmark, Info, MTD, QTD, YTD, 1Y, 3Y, 5Y, 10Y
            benchmarks_rows = benchmarks_df.to_dict(orient="records")
            
            # Create output directory for charts
            base_dir = get_base_dir()
            output_dir = base_dir / "output"
            output_dir.mkdir(exist_ok=True)
            
            # Calculate shared y-axis limits across both MTD and 1Y columns
            # Multiply by 100 to convert from decimal to percentage
            mtd_data = benchmarks_df['MTD'].dropna() * 100
            y1_data = benchmarks_df['1Y'].dropna() * 100
            
            if len(mtd_data) > 0 or len(y1_data) > 0:
                all_values = pd.concat([mtd_data, y1_data])
                min_val = all_values.min()
                max_val = all_values.max()
                
                # Add dynamic padding based on actual min/max values
                # Use smaller padding (5%) and only where needed
                padded_min = min_val - abs(min_val) * 0.05
                y_min = min(padded_min, -3.0)
                
                padded_max = max_val + abs(max_val) * 0.05
                y_max = max(padded_max, 3.0)
                
                shared_y_lim = (y_min, y_max)
            else:
                shared_y_lim = None
            
            # Create bar charts with shared y-axis
            benchmark_chart_mtd_path = create_benchmark_chart(
                benchmarks_df, 'MTD', 'Benchmarks, Month-To-Date Returns',
                output_dir / "benchmark_chart_mtd.png",
                y_lim=shared_y_lim
            )
            benchmark_chart_1y_path = create_benchmark_chart(
                benchmarks_df, '1Y', 'Benchmarks, 1 Year Returns',
                output_dir / "benchmark_chart_1y.png",
                y_lim=shared_y_lim
            )
            
            print(f"Loaded {len(benchmarks_rows)} benchmark rows")
            print(f"Created benchmark charts")
        except Exception as e:
            print(f"Warning: Error loading benchmarks: {e}")
            import traceback
            traceback.print_exc()
            benchmarks_rows = []
    else:
        benchmarks_rows = []
    # 3h) JPM LOC Data
    jpm_usage_rows = []
    jpm_collateral_rows = []
    jpm_letters_of_credit = None

    if JPMdf and not onlyHoldings:
        try:
            # JPMdf is already a DataFrame or dict of DataFrames
            if isinstance(JPMdf, dict):
                # If it's a dict, get the sheets
                usage_df = JPMdf.get('usage', list(JPMdf.values())[0] if JPMdf else pd.DataFrame())
                collateral_df = JPMdf.get('collateral', list(JPMdf.values())[1] if len(JPMdf) > 1 else pd.DataFrame())
            else:
                # Single DataFrame - assume it's usage
                usage_df = JPMdf
                collateral_df = pd.DataFrame()
            # It seems the sheet might have headers on row 0. Based on screenshot:
            # Type, Size, Cost, Amount Drawn
            # 'Letters of Credit' is likely a separate row below 'Total' or a separate variable.
            # Let's clean the DF.
            
            # Process usage data
            if not usage_df.empty:
                usage_df.columns = [str(c).strip() for c in usage_df.columns]
                
                # Extract 'Letters of Credit' if present
                loc_row = usage_df[usage_df.iloc[:, 0].astype(str).str.contains("Letters of Credit", case=False, na=False)]
                if not loc_row.empty:
                    jpm_letters_of_credit = loc_row.iloc[0, 1]
                
                # Usage table rows (exclude Letters of Credit if it was in the same table)
                usage_table = usage_df[~usage_df.iloc[:, 0].astype(str).str.contains("Letters of Credit", case=False, na=False)].copy()
                if len(usage_table.columns) >= 4:
                    usage_table = usage_table.iloc[:, :4]
                    usage_table.columns = ["type", "size", "cost", "amount_drawn"]
                    jpm_usage_rows = usage_table.to_dict(orient="records")
                else:
                    print("Warning: JPM Usage table has fewer than 4 columns")

            # Process collateral data
            if not collateral_df.empty:
                if len(collateral_df.columns) >= 4:
                    collateral_df = collateral_df.iloc[:, :4]
                    collateral_df.columns = ["account", "market_value", "lending_value", "pct"]
                    jpm_collateral_rows = collateral_df.to_dict(orient="records")
                else:
                    print("Warning: JPM Collateral table has fewer than 4 columns")
                
            print(f"Loaded {len(jpm_usage_rows)} JPM usage rows")
            print(f"Loaded {len(jpm_collateral_rows)} JPM collateral rows")
            
        except Exception as e:
            print(f"Warning: Error loading JPM LOC data: {e}")
            # Don't print full traceback for permission error if expected
            if "Permission denied" not in str(e):
                import traceback
                traceback.print_exc()

    if not onlyHoldings:
        # 4) Load Narrative
        narrative_path = ASSETS_DIR + "/reportFiles/narrative.md"
        narrative_path = Path(narrative_path)
        if os.path.exists(narrative_path):
            narrative_text = narrative_path.read_text(encoding="utf-8")
            narrative_html = markdown.markdown(narrative_text)
        else:
            print(f"Warning: Narrative text not found.")
            narrative_html = "<p>No narrative found.</p>"

        # 4b) Load JPM Commentary
        jpm_commentary_path = ASSETS_DIR + "/reportFiles/jpm_commentary.md"
        jpm_commentary_path = Path(jpm_commentary_path)
        if os.path.exists(jpm_commentary_path):
            jpm_text = jpm_commentary_path.read_text(encoding="utf-8")
            jpm_commentary_html = markdown.markdown(jpm_text)
        else:
            print(f"Warning: '{jpm_commentary_path}' not found.")
            jpm_commentary_html = "<p>No JPM commentary found.</p>"

    
    out_path = Path(out_path)
    
    # Format as "Month day, year" (e.g., "September 30, 2025")
    formatted_date = report_date.strftime("%B %Y")
    
    # 7) Generate PDF directly using ReportLab
    try:
        pdf_gen = PDFReportGenerator(str(out_path), ASSETS_DIR, footerData=footerData)
        
        if not onlyHoldings:
            # Cover page
            pdf_gen.add_cover_page(
                "Overall Post HFC - report automation test",
                "Monthly Portfolio Overview",
                formatted_date
            )
            
            # Narrative page
            pdf_gen.add_narrative_page(
                "High-Level Portfolio Snapshot Narrative",
                formatted_date,
                narrative_html
            )
            
            # Main snapshot page
            pdf_gen.add_header_row("High-Level Portfolio Snapshot", formatted_date)
        
            # Top row: 3-column layout
            # Since ReportLab has issues with nested KeepTogether in Tables,
            # we'll use a custom approach: add tables directly but adjust their widths
            # to fit the 3-column layout when rendered
            
            col1_width = 2.8 * inch
            col2_width = 3.8 * inch
            col3_width = 3.36 * inch
            
            # Add left column: Assets and Flows + Foundations
            left_content = pdf_gen.add_assets_and_flows_table(assets_and_flows_rows, col1_width)
            for item in left_content:
                pdf_gen.story.append(item)
            pdf_gen.story.append(Spacer(1, 0.2*inch))
            foundations_content = pdf_gen.add_foundations_table(hf_foundations_rows, col1_width)
            for item in foundations_content:
                pdf_gen.story.append(item)
            
            # Add middle column: Portfolio Returns
            pdf_gen.story.append(Spacer(1, 0.2*inch))
            middle_content = pdf_gen.add_portfolio_returns_table(portfolio_returns_rows, col2_width)
            for item in middle_content:
                pdf_gen.story.append(item)
            
            # Add right column: Overall Family Breakdown + Sports
            pdf_gen.story.append(Spacer(1, 0.2*inch))
            right_content = pdf_gen.add_overall_family_breakdown_table(overall_family_breakdown_rows, col3_width)
            for item in right_content:
                pdf_gen.story.append(item)
            sports_content = pdf_gen.add_sports_table(sports_rows, col3_width)
            for item in sports_content:
                pdf_gen.story.append(item)
            
            # Returns vs Benchmark table
            pdf_gen.story.append(Spacer(1, 0.2*inch))
            rvb_content = pdf_gen.add_returns_vs_benchmark_table(returns_vs_benchmark_rows)
            pdf_gen.story.extend(rvb_content)
            
            # Benchmarks page (if data available)
            if benchmarks_rows:
                pdf_gen.story.append(PageBreak())
                pdf_gen.add_header_row("What Happened In The World Markets?", formatted_date)
                pdf_gen.add_benchmark_charts(benchmark_chart_mtd_path, benchmark_chart_1y_path)
                pdf_gen.add_benchmark_table(benchmarks_rows)
            
            # JPM LOC page (if data available)
            if jpm_usage_rows or jpm_collateral_rows:
                pdf_gen.story.append(PageBreak())
                pdf_gen.add_jpm_tables(
                    jpm_usage_rows,
                    jpm_collateral_rows,
                    jpm_letters_of_credit,
                    jpm_commentary_html,
                    formatted_date
                )
        
        # Portfolio Holdings pages - start on new page
        if portfolio_holdings_pages:
            if not onlyHoldings:
                pdf_gen.story.append(PageBreak())  # Always start on new page
            for page_num, page_rows in enumerate(portfolio_holdings_pages):
                if page_num > 0:
                    pdf_gen.story.append(PageBreak())
                pageColorDepths = colorDepthPages[page_num]
                pdf_gen.add_header_row(f"{sourceName} Portfolio Holdings", formatted_date)
                pdf_gen.add_portfolio_holdings_table(page_rows, pageColorDepths, page_num, 
                                                     exclude_keys=holdings_exclude_keys,
                                                     header_order=holdings_header_order, 
                                                     fontSize = tableFontSize, rowPadding = rowPadding)
        
        # Build PDF
        pdf_gen.build()
        print(f"Wrote PDF: {out_path}")
        try:
            if os.path.exists(out_path):
                if sys.platform == "win32":
                    os.startfile(out_path)
                elif sys.platform == "darwin":
                    os.system(f"open \"{out_path}\"")
                else:
                    os.system(f"xdg-open \"{out_path}\"")
        except Exception as open_ex:
            print(f"Warning: Could not open generated PDF automatically: {open_ex}")
        
    except Exception as e:
        print(f"Error generating PDF: {e}")
        import traceback
        traceback.print_exc()

    print(f"Report generation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")