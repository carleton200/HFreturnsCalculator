"""
PDF Generator using ReportLab - Direct PDF creation matching HTML template formatting
Uses Frame system for proper layout and two-pass build for page numbering
"""
from ctypes import alignment
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak,
    Image, KeepTogether, KeepInFrame, Frame, PageTemplate, BaseDocTemplate
)
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor, blue
import pandas as pd
from pathlib import Path
import os
from typing import List, Dict, Optional, Tuple

from scripts.commonValues import maxRowPadding, standardFontSize, textCols
try:
    from scripts.basicFunctions import headerUnits
    from scripts.commonValues import smallHeaders, fraction_headers, percent_headers, yearOptions
except:
    print('ERROR: Failed to access script modules')

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

pdfmetrics.registerFont(TTFont('Tahoma', 'Tahoma.ttf'))
pdfmetrics.registerFont(TTFont('Tahoma-Bold', 'TahomaBd.ttf'))




# Color definitions matching HTML template
COLOR_PRIMARY = HexColor(0x004225)  #HF Capital official color 'British Racing Green'
COLOR_GRAY_HEADER = HexColor(0x4E4E4E)
COLOR_TOTAL = HexColor(0xbebfc2)  # rgb(190, 191, 194) - light gray for totals
COLOR_SUBCLASS = HexColor(0xf3f3f4)  # rgb(243, 243, 244) - very light gray
COLOR_POOL = HexColor(0xd7d7d9)  # rgb(215, 215, 217) - medium gray
COLOR_ILLIQUID = HexColor(0xf6d8b8)  # rgb(246, 216, 184) - peach
COLOR_LIQUID = HexColor(0xbbc8e2)  # rgb(187, 200, 226) - light blue
COLOR_CASH = HexColor(0xc9e5d3)  # rgb(201, 229, 211) - light green
COLOR_BENCHMARK = HexColor(0x3952a4)  # rgb(57, 82, 164) - blue
COLOR_RED = HexColor(0xff0000)  # Red for negative values

# Page dimensions (landscape 11x8.5)
PAGE_WIDTH = 11 * inch
PAGE_HEIGHT = 8.5 * inch
MARGIN_LEFT = 0.4 * inch
MARGIN_LEFT_CENTERED = 4.5 * inch
MARGIN_RIGHT = 0.4 * inch
MARGIN_TOP = 0.4 * inch
MARGIN_BOTTOM = 0.6 * inch
CONTENT_WIDTH = PAGE_WIDTH - MARGIN_LEFT - MARGIN_RIGHT
CONTENT_HEIGHT = PAGE_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM

# Font sizes
FONT_SIZE_NORMAL = 9
FONT_SIZE_SMALL = FONT_SIZE_NORMAL - 1
FONT_SIZE_MEDIUM = FONT_SIZE_NORMAL + 1
FONT_SIZE_LARGE = FONT_SIZE_NORMAL + 3
FONT_SIZE_TITLE = FONT_SIZE_NORMAL + 5
FONT_SIZE_COVER_TITLE = FONT_SIZE_NORMAL * 4
FONT_SIZE_COVER_SUBTITLE = int(FONT_SIZE_NORMAL * 2)

fontName = 'Tahoma'
italicFontName = 'Helvetica-Oblique'


class NumberedCanvas(canvas.Canvas):
    """Custom canvas for page numbering with two-pass build."""
    def __init__(self, filename, footerData=None, *args, **kwargs):
        canvas.Canvas.__init__(self, filename, *args, **kwargs)
        self._saved_page_states = []
        self._page_count = 0
        self.footerData = footerData if footerData is not None else {}

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()
        self._page_count += 1

    def save(self):
        """Save all pages with page numbers."""
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        """Draw footer with page numbers."""
        self.saveState()
        self.setFont(f'{fontName}', FONT_SIZE_SMALL)
        self.setFillColor(colors.black)
        page_num = self._pageNumber
        
        portfolioSource = self.footerData.get('portfolioSource') or 'Portfolio'
        classification = self.footerData.get('classification','HFC')
        reportDate = self.footerData.get('reportDate')

        self.setFillColor(COLOR_PRIMARY)
        # Left footer
        self.drawString(MARGIN_LEFT, MARGIN_BOTTOM - 0.2*inch, 
                       f"Overall Post {classification} - Confidential")
        
        #Center Footer
        if reportDate:
            printDate = datetime.now().strftime('%Y-%m-%d')
            reportDate = reportDate.strftime('%B %Y')
            self.drawString(MARGIN_LEFT_CENTERED, MARGIN_BOTTOM - 0.2*inch, 
                        f"{reportDate}  Printed {printDate}")

        # Right footer (page numbers)
        self.drawRightString(PAGE_WIDTH - MARGIN_RIGHT, MARGIN_BOTTOM - 0.2*inch,
                            f"{page_num} of {page_count}")
        self.restoreState()


class PDFReportGenerator:
    """Generates PDF reports directly using ReportLab, matching HTML template formatting."""
    
    def __init__(self, output_path: str, assets_dir: str, footerData = None):
        self.output_path = Path(output_path)
        self.assets_dir = Path(assets_dir)
        self.story = []
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        self._total_pages = 0
        self.footerData = footerData
        
    def _setup_custom_styles(self):
        """Setup custom paragraph styles matching HTML template."""
        # Normal text style
        self.styles.add(ParagraphStyle(
            name='CustomNormal',
            parent=self.styles['Normal'],
            fontName=f'{fontName}',
            fontSize=FONT_SIZE_NORMAL,
            leading=FONT_SIZE_NORMAL * 1.2,
            textColor=colors.black,
            alignment=TA_LEFT
        ))
        
        # Header style
        self.styles.add(ParagraphStyle(
            name='HeaderTitle',
            parent=self.styles['Normal'],
            fontName=f'{fontName}',
            fontSize=FONT_SIZE_TITLE,
            textColor=COLOR_PRIMARY,
            alignment=TA_LEFT,
            spaceAfter=5,
        ))
        
        # Date style
        self.styles.add(ParagraphStyle(
            name='HeaderDate',
            parent=self.styles['Normal'],
            fontName=f'{fontName}',
            fontSize=FONT_SIZE_TITLE,
            textColor=COLOR_PRIMARY,
            alignment=TA_RIGHT
        ))
        
        # Narrative content style
        self.styles.add(ParagraphStyle(
            name='NarrativeContent',
            parent=self.styles['Normal'],
            fontName=f'{fontName}',
            fontSize=FONT_SIZE_LARGE,
            leading=FONT_SIZE_LARGE * 1.8,
            textColor=colors.black,
            alignment=TA_LEFT
        ))
        
        # Cover title style
        self.styles.add(ParagraphStyle(
            name='CoverTitle',
            parent=self.styles['Heading1'],
            fontName=f'{fontName}-Bold',
            fontSize=FONT_SIZE_COVER_TITLE,
            textColor=COLOR_PRIMARY,
            alignment=TA_LEFT,
            spaceAfter=25
        ))
        
        # Cover subtitle style
        self.styles.add(ParagraphStyle(
            name='CoverSubtitle',
            parent=self.styles['Normal'],
            fontName=f'{fontName}',
            fontSize=FONT_SIZE_COVER_SUBTITLE,
            textColor=COLOR_PRIMARY,
            alignment=TA_LEFT,
            spaceAfter=5
        ))
        
        # Right-aligned style for percentages in tables
        self.styles.add(ParagraphStyle(
            name='CustomNormalRight',
            parent=self.styles['CustomNormal'],
            alignment=TA_RIGHT
        ))
        self.styles.add(ParagraphStyle(
            name='CustomBoldRight',
            parent=self.styles['CustomNormal'],
            alignment=TA_RIGHT,
            fontName= f'{fontName}-bold'
        ))
        self.styles.add(ParagraphStyle(
            name='CustomBenchmark%',
            parent=self.styles['CustomNormal'],
            alignment=TA_RIGHT,
            fontName= f'{italicFontName}'
        ))
        self.styles.add(ParagraphStyle(
            name='holdingsText-bold',
            parent=self.styles['CustomNormal'],
            alignment=TA_LEFT,
            fontName=  f'{fontName}-bold'
        ))
    
    def _format_number(self, value, is_percent=False, is_currency=False, decimals=2):
        """Format numbers matching HTML template formatting."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "-"
        
        if isinstance(value, str):
            return value
        
        if is_percent:
            if value < 0:
                return f"({abs(value):.{decimals}f}%)"
            else:
                return f"{value:.{decimals}f}%"
        
        if is_currency:
            if value < 0:
                return f"({abs(value):,.0f})"
            else:
                return f"{value:,.0f}"
        
        return f"{value:,.{decimals}f}"
    
    def _create_table_caption(self, text: str) -> Table:
        """Create a table caption matching HTML template style."""
        caption_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), COLOR_GRAY_HEADER),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), f'{fontName}-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), FONT_SIZE_NORMAL),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ])
        
        caption_table = Table([[text]], colWidths=[CONTENT_WIDTH], style=caption_style)
        return caption_table
    
    def _create_panel_table(self, headers: List[str], data: List[List], 
                           col_widths: List[float] = None,
                           caption: str = None,
                           row_colors: List[Optional[HexColor]] = None,
                           alignments: List[str] = None) -> Table:
        """Create a table matching HTML panel style with proper row borders."""
        # Build table data
        table_data = [headers]
        table_data.extend(data)
        
        # Default column widths (equal distribution)
        if col_widths is None:
            col_widths = [CONTENT_WIDTH / len(headers)] * len(headers)
        
        # Default alignments (first column left, rest right)
        if alignments is None:
            alignments = ['LEFT'] + ['RIGHT'] * (len(headers) - 1)
        
        # Create table style
        table_style = TableStyle([
            # Outer border
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            
            # Header row
            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('FONTNAME', (0, 0), (-1, 0), f'{fontName}-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), FONT_SIZE_NORMAL),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 2),
            ('TOPPADDING', (0, 0), (-1, 0), 2),
            ('LEFTPADDING', (0, 0), (-1, 0), 3),
            ('RIGHTPADDING', (0, 0), (-1, 0), 3),
            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),  # Header bottom border
            
            # First column vertical border
            ('LINEAFTER', (0, 0), (0, -1), 1, colors.black),
            
            # Data rows - add horizontal lines between ALL rows
            ('FONTNAME', (0, 1), (-1, -1), f'{fontName}'),
            ('FONTSIZE', (0, 1), (-1, -1), FONT_SIZE_NORMAL),
            ('LEFTPADDING', (0, 1), (-1, -1), 3),
            ('RIGHTPADDING', (0, 1), (-1, -1), 3),
            ('TOPPADDING', (0, 1), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 2),
        ])
        
        # Add horizontal lines between all data rows
        for i in range(1, len(table_data)):
            table_style.add('LINEBELOW', (0, i), (-1, i), 0.5, colors.black)
        
        # Add alignments
        for i, align in enumerate(alignments):
            if align == 'LEFT':
                table_style.add('ALIGN', (i, 1), (i, -1), 'LEFT')
            elif align == 'CENTER':
                table_style.add('ALIGN', (i, 1), (i, -1), 'CENTER')
            else:
                table_style.add('ALIGN', (i, 1), (i, -1), 'RIGHT')
        
        # Add row colors (only for rows that should have colors)
        if row_colors:
            for i, color in enumerate(row_colors, start=1):
                if color:
                    table_style.add('BACKGROUND', (0, i), (-1, i), color)
                # Regular rows (None color) stay white - no need to set
        
        # Add top border for last row (total row) if it's a total
        if len(data) > 0:
            table_style.add('LINEABOVE', (0, -1), (-1, -1), 1, colors.black)
            # Check if last row should be bold
            if row_colors and len(row_colors) > 0 and row_colors[-1] == COLOR_TOTAL:
                table_style.add('FONTNAME', (0, -1), (-1, -1), f'{fontName}-Bold')
        
        table = Table(table_data, colWidths=col_widths, style=table_style)
        return table
    
    def add_cover_page(self, title: str, subtitle: str, date: str):
        """Add cover page matching HTML template."""
        # Logo
        logo_path = self.assets_dir / "logo.png"
        if logo_path.exists():
            logo = Image(str(logo_path), width=1.5*inch, height=1.5*inch)
            logo.hAlign = 'RIGHT'
            self.story.append(logo)
        
        # Spacer
        self.story.append(Spacer(1, 2*inch))
        
        # Title
        title_para = Paragraph(title, self.styles['CoverTitle'])
        self.story.append(title_para)
        
        # Subtitle
        subtitle_para = Paragraph(subtitle, self.styles['CoverSubtitle'])
        self.story.append(subtitle_para)
        
        # Date
        date_para = Paragraph(date, self.styles['CoverSubtitle'])
        self.story.append(date_para)
        
        # Page break
        self.story.append(PageBreak())
    
    def add_header_row(self, title: str, date: str):
        """Add header row with title and date."""
        header_data = [
            [Paragraph(title, self.styles['HeaderTitle']), 
             Paragraph(date, self.styles['HeaderDate'])]
        ]
        header_table = Table(header_data, colWidths=[CONTENT_WIDTH * 0.7, CONTENT_WIDTH * 0.3])
        header_style = TableStyle([
            ('VALIGN', (0, 0), (-1, 0), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 0),
        ])
        header_table.setStyle(header_style)
        self.story.append(header_table)
        self.story.append(Spacer(1, 0.2*inch))
    
    def add_narrative_page(self, title: str, date: str, narrative_html: str):
        """Add narrative page with markdown content."""
        self.add_header_row(title, date)
        
        # Convert HTML to paragraphs
        narrative_para = Paragraph(narrative_html.replace('<p>', '').replace('</p>', '<br/>'), 
                                   self.styles['NarrativeContent'])
        self.story.append(narrative_para)
        self.story.append(PageBreak())
    
    
    def add_assets_and_flows_table(self, data: List[Dict], width: float):
        """Add Assets and Flows table."""
        headers = ['$ Millions', 'Month', '1 Year', 'Inception']
        table_data = []
        
        for row in data:
            table_data.append([
                row.get('label', ''),
                self._format_number(row.get('month'), is_currency=True),
                self._format_number(row.get('year_1'), is_currency=True),
                self._format_number(row.get('inception'), is_currency=True) if row.get('inception') else '-'
            ])
        
        col_widths = [width * 0.35, width * 0.22, width * 0.22, width * 0.21]
        alignments = ['LEFT', 'CENTER', 'RIGHT', 'RIGHT']
        
        caption = self._create_table_caption('Assets and Flows')
        table = self._create_panel_table(headers, table_data, col_widths, alignments=alignments)
        return [caption, table]
    
    def add_foundations_table(self, data: List[Dict], width: float):
        """Add HF Foundations table."""
        headers = ['Asset Class', '$MM', '% Allocation']
        table_data = []
        row_colors = []
        
        for row in data:
            is_total = str(row.get('asset_class', '')).lower() == 'total'
            table_data.append([
                row.get('asset_class', ''),
                self._format_number(row.get('mm'), is_currency=True),
                self._format_number(row.get('pct'), is_percent=True)
            ])
            row_colors.append(COLOR_TOTAL if is_total else None)
        
        col_widths = [width * 0.4, width * 0.3, width * 0.3]
        alignments = ['LEFT', 'CENTER', 'RIGHT']
        
        caption = self._create_table_caption('HF Foundations')
        table = self._create_panel_table(headers, table_data, col_widths, 
                                        alignments=alignments, row_colors=row_colors)
        return [Spacer(1, 0.2*inch), caption, table]
    
    def add_portfolio_returns_table(self, data: List[Dict], width: float):
        """Add Portfolio Returns table."""
        headers = ['', '$MM', 'Month', 'YTD', '1 Year', '3 Year', 'Inception']
        table_data = []
        row_colors = []
        
        for row in data:
            label = row.get('label', '')
            label_lower = label.lower()
            
            # Determine row color - only special rows get colors
            if 'illiquid' in label_lower:
                bg_color = COLOR_ILLIQUID
            elif 'liquid' in label_lower:
                bg_color = COLOR_LIQUID
            elif 'cash' in label_lower:
                bg_color = COLOR_CASH
            elif 'hf capital' in label_lower or 'hf captial' in label_lower:
                bg_color = None  # Will be bold, no background
            else:
                bg_color = None  # White background
            
            row_colors.append(bg_color)
            
            is_benchmark = (row.get('month') is None) or 'benchmark' in label_lower or 'acwi' in label_lower
            
            table_data.append([
                label,
                self._format_number(row.get('month'), is_currency=True) if not is_benchmark and row.get('month') is not None else '',
                self._format_number(row.get('qtd'), is_percent=True) if row.get('qtd') is not None else '',
                self._format_number(row.get('ytd'), is_percent=True) if row.get('ytd') is not None else '',
                self._format_number(row.get('one_year'), is_percent=True) if row.get('one_year') is not None else '',
                self._format_number(row.get('three_year'), is_percent=True) if row.get('three_year') is not None else '',
                self._format_number(row.get('inception'), is_percent=True) if row.get('inception') is not None else ''
            ])
        
        col_widths = [width * 0.25, width * 0.12, width * 0.11, width * 0.11, 
                     width * 0.11, width * 0.11, width * 0.19]
        alignments = ['LEFT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT']
        
        caption = self._create_table_caption('Portfolio Returns')
        table = self._create_panel_table(headers, table_data, col_widths, 
                                        alignments=alignments, row_colors=row_colors)
        return [caption, table]
    
    def add_overall_family_breakdown_table(self, data: List[Dict], width: float):
        """Add Overall Family Breakdown table."""
        headers = ['Asset', 'LM $MM', 'Δ', 'CM $MM', '%']
        table_data = []
        
        for row in data:
            table_data.append([
                row.get('asset', ''),
                self._format_number(row.get('lm_mm'), is_currency=True),
                self._format_number(row.get('delta_mm'), is_currency=True) if row.get('delta_mm') is not None and not pd.isna(row.get('delta_mm')) else '-',
                self._format_number(row.get('cm_mm'), is_currency=True),
                self._format_number(row.get('pct'), is_percent=True)
            ])
        
        col_widths = [width * 0.31, width * 0.27, width * 0.16, width * 0.27, width * 0.16]
        alignments = ['LEFT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT']
        
        caption = self._create_table_caption('Overall Family Breakdown')
        table = self._create_panel_table(headers, table_data, col_widths, alignments=alignments)
        return [caption, table]
    
    def add_sports_table(self, data: List[Dict], width: float):
        """Add Sports table."""
        headers = ['Sports', 'Share %', 'Team Value', 'Debt', 'Equity', 'Family Share']
        table_data = []
        row_colors = []
        
        for row in data:
            is_total = str(row.get('sports', '')).lower() == 'total'
            row_colors.append(COLOR_TOTAL if is_total else None)
            
            table_data.append([
                row.get('sports', ''),
                self._format_number(row.get('share_pct'), is_percent=True) if row.get('sports', '').lower() != 'total' and row.get('share_pct') is not None else '',
                self._format_number(row.get('team_value'), is_currency=True) if row.get('team_value') is not None else '',
                self._format_number(row.get('debt'), is_currency=True) if row.get('debt') is not None else '-',
                self._format_number(row.get('equity'), is_currency=True) if row.get('equity') is not None else '',
                self._format_number(row.get('family_share'), is_currency=True) if row.get('family_share') is not None else ''
            ])
        
        col_widths = [width * 0.25, width * 0.12, width * 0.15, width * 0.15, width * 0.15, width * 0.18]
        alignments = ['LEFT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT']
        
        table = self._create_panel_table(headers, table_data, col_widths, 
                                        alignments=alignments, row_colors=row_colors)
        return [Spacer(1, 0.2*inch), table]
    
    def add_returns_vs_benchmark_table(self, data: List[Dict]):
        """Add Returns vs Benchmark table with complex header structure."""
        # Build headers - two row structure
        header_row1 = [
            '', '',  # Asset Class, $MM
            'Asset Allocation vs. Target', '', '',
            'Month', '', '',
            'YTD', '', '',
            '1 Year', '', '',
            '3 Year', '', '',
            'Inception', '', ''
        ]
        
        header_row2 = [
            'Asset Class', '$MM',
            '%', 'Tgt %', 'Δ$',
            'RTN %', 'BM %', 'Δ',
            'RTN %', 'BM %', 'Δ',
            'RTN %', 'BM %', 'Δ',
            'RTN %', 'BM %', 'Δ',
            'RTN %', 'BM %', 'Δ'
        ]
        
        # Build data rows
        table_data = []
        row_colors = []
        
        for row in data:
            asset_class = str(row.get('asset_class', '')).lower()
            
            # Determine background color - only special rows get colors
            if asset_class == 'total':
                bg_color = COLOR_TOTAL
            elif asset_class in ['direct private equity', 'private equity', 'direct real assets', 'real assets', 'illiquid']:
                bg_color = COLOR_ILLIQUID
            elif asset_class in ['public equity', 'long/short', 'absolute return', 'liquid', 'fixed income']:
                bg_color = COLOR_LIQUID
            elif asset_class == 'cash':
                bg_color = COLOR_CASH
            else:
                bg_color = None  # White background
            
            row_colors.append(bg_color)
            
            table_data.append([
                row.get('asset_class', ''),
                self._format_number(row.get('mm'), is_currency=True),
                self._format_number(row.get('alloc_pct'), is_percent=True),
                self._format_number(row.get('tgt_pct'), is_percent=True),
                self._format_number(row.get('alloc_delta'), is_currency=True) if row.get('alloc_delta') is not None and not pd.isna(row.get('alloc_delta')) else '-',
                self._format_percent_with_bm(row.get('m_rtn')),
                self._format_percent_with_bm(row.get('m_bm')),
                self._format_percent_with_bm(row.get('m_delta')),
                self._format_percent_with_bm(row.get('ytd_rtn')),
                self._format_percent_with_bm(row.get('ytd_bm')),
                self._format_percent_with_bm(row.get('ytd_delta')),
                self._format_percent_with_bm(row.get('y1_rtn')),
                self._format_percent_with_bm(row.get('y1_bm')),
                self._format_percent_with_bm(row.get('y1_delta')),
                self._format_percent_with_bm(row.get('y3_rtn')),
                self._format_percent_with_bm(row.get('y3_bm')),
                self._format_percent_with_bm(row.get('y3_delta')),
                self._format_percent_with_bm(row.get('inc_rtn')),
                self._format_percent_with_bm(row.get('inc_bm')),
                self._format_percent_with_bm(row.get('inc_delta'))
            ])
        
        # Combine headers
        full_table_data = [header_row1, header_row2] + table_data
        
        # Column widths (20 columns total)
        col_widths = [
            CONTENT_WIDTH * 0.12,  # Asset Class
            CONTENT_WIDTH * 0.08,  # $MM
            CONTENT_WIDTH * 0.04,  # % (alloc)
            CONTENT_WIDTH * 0.04,  # Tgt %
            CONTENT_WIDTH * 0.04,  # Δ$ (alloc)
            CONTENT_WIDTH * 0.04,  # RTN % (month)
            CONTENT_WIDTH * 0.04,  # BM % (month)
            CONTENT_WIDTH * 0.04,  # Δ (month)
            CONTENT_WIDTH * 0.04,  # RTN % (ytd)
            CONTENT_WIDTH * 0.04,  # BM % (ytd)
            CONTENT_WIDTH * 0.04,  # Δ (ytd)
            CONTENT_WIDTH * 0.04,  # RTN % (1y)
            CONTENT_WIDTH * 0.04,  # BM % (1y)
            CONTENT_WIDTH * 0.04,  # Δ (1y)
            CONTENT_WIDTH * 0.04,  # RTN % (3y)
            CONTENT_WIDTH * 0.04,  # BM % (3y)
            CONTENT_WIDTH * 0.04,  # Δ (3y)
            CONTENT_WIDTH * 0.04,  # RTN % (inc)
            CONTENT_WIDTH * 0.04,  # BM % (inc)
            CONTENT_WIDTH * 0.04   # Δ (inc)
        ]
        
        alignments = ['LEFT', 'RIGHT'] + ['CENTER'] * 18
        
        caption = self._create_table_caption('Asset Class Returns Vs. Benchmarks')
        
        # Create table with custom style for merged headers
        table = Table(full_table_data, colWidths=col_widths)
        table_style = TableStyle([
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('LINEBELOW', (0, 1), (-1, 1), 1, colors.black),
            ('BACKGROUND', (0, 0), (-1, 1), colors.white),
            ('FONTNAME', (0, 0), (-1, 1), f'{fontName}-Bold'),
            ('FONTSIZE', (0, 0), (-1, 1), FONT_SIZE_NORMAL),
            ('ALIGN', (0, 0), (1, 1), 'LEFT'),
            ('ALIGN', (2, 0), (-1, 1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 1), 'BOTTOM'),
            ('SPAN', (2, 0), (4, 0)),  # Asset Allocation vs. Target
            ('SPAN', (5, 0), (7, 0)),   # Month
            ('SPAN', (8, 0), (10, 0)),  # YTD
            ('SPAN', (11, 0), (13, 0)), # 1 Year
            ('SPAN', (14, 0), (16, 0)), # 3 Year
            ('SPAN', (17, 0), (19, 0)), # Inception
            ('LINEAFTER', (1, 0), (1, -1), 1, colors.black),  # After $MM
            ('LINEAFTER', (4, 0), (4, -1), 1, colors.black),  # After Δ$ (alloc)
            ('LINEAFTER', (7, 0), (7, -1), 1, colors.black),  # After Δ (month)
            ('LINEAFTER', (10, 0), (10, -1), 1, colors.black), # After Δ (ytd)
            ('LINEAFTER', (13, 0), (13, -1), 1, colors.black), # After Δ (1y)
            ('LINEAFTER', (16, 0), (16, -1), 1, colors.black), # After Δ (3y)
        ])
        
        # Add horizontal lines between all data rows
        for i in range(2, len(full_table_data)):
            table_style.add('LINEBELOW', (0, i), (-1, i), 0.5, colors.black)
        
        # Add data row styles
        for i, color in enumerate(row_colors, start=2):
            if color:
                table_style.add('BACKGROUND', (0, i), (-1, i), color)
        
        table_style.add('FONTNAME', (0, 2), (-1, -1), f'{fontName}')
        table_style.add('FONTSIZE', (0, 2), (-1, -1), FONT_SIZE_NORMAL)
        table_style.add('ALIGN', (0, 2), (0, -1), 'LEFT')
        table_style.add('ALIGN', (1, 2), (-1, -1), 'CENTER')
        table_style.add('LEFTPADDING', (0, 0), (-1, -1), 3)
        table_style.add('RIGHTPADDING', (0, 0), (-1, -1), 3)
        table_style.add('TOPPADDING', (0, 0), (-1, -1), 2)
        table_style.add('BOTTOMPADDING', (0, 0), (-1, -1), 2)
        
        table.setStyle(table_style)
        
        # Add footnote
        footnote = Paragraph(
            '<font size="9">Cash includes mark to market impacts; current yield at 4.1%</font>',
            self.styles['CustomNormal']
        )
        return [caption, table, Spacer(1, 0.05*inch), footnote]
    
    def _format_percent_with_bm(self, value):
        """Format percentage values with 'No BM' handling. Returns Paragraph for proper HTML rendering."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return Paragraph('<font color="red">No BM</font>', self.styles['CustomNormal'])
        if isinstance(value, str):
            return Paragraph(f'<font color="red">{value}</font>', self.styles['CustomNormal'])
        if value < 0:
            return Paragraph(f'<font color="red">({abs(value) * 100:.1f}%)</font>', self.styles['CustomNormal'])
        return Paragraph(f"{value * 100:.1f}%", self.styles['CustomNormal'])
    
    def add_portfolio_holdings_table(self, data: List[Dict], colorDepths, page_num: int = 0, 
                                     exclude_keys: List[str] = None, header_order: List[str] = None,
                                      rowPadding = maxRowPadding, fontSize = standardFontSize):
        """Add Portfolio Holdings table for a page.
        
        Args:
            data: List of row dictionaries from holdingDict
            page_num: Page number for pagination
            exclude_keys: List of keys to exclude from columns (e.g., 'dataType', 'type', 'row_class', 'show_border')
            header_order: List specifying the order of column headers. If None, uses default headers.
        """
        if exclude_keys is None:
            exclude_keys = ['dataType', 'type', 'row_class', 'show_border']
        convertKeys = {'' : 'Asset Class/Investment', 'IRR ITD' : 'IRR', 'NAV' : 'Value'}
        for yr in yearOptions:
            convertKeys[f'{yr}YR'] = f'{yr}-Year'
        
        default_headers = ['', '%', 'NAV', 'MTD', 'YTD', '1YR', '3YR', 'ITD']
        
        # Determine all possible column keys from the data
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())
        
        # Remove excluded keys
        column_keys = [key for key in all_keys if key not in exclude_keys]
        
        # Use header_order if provided, otherwise use default headers
        if header_order:
            # Use the provided header_order directly - these are the actual keys to use
            # Filter to only include keys that exist in the data
            headers = [key for key in header_order if key in column_keys]
            # Use header_order as display names (they are the actual keys)
            table_header_row = headers
        else:
            # Use default headers and map them to actual keys
            headers = []
            table_header_row = []
            
            # Map default display headers to actual keys in the data
            key_variations = {
                'Asset Class / Investment': ['investment', 'Investment', 'INVESTMENT'],
                '%': ['%', 'pct', 'pct_alloc', 'Pct', 'PCT', 'allocation'],
                'NAV': ['NAV', 'nav', 'Nav', 'value', 'Value', 'VALUE'],
                'MTD': ['MTD', 'mtd', 'Mtd'],
                'YTD': ['YTD', 'ytd', 'Ytd'],
                '1YR': ['1YR', '1yr', '1Yr', 'one_year', 'oneYear', '1Y', '1y'],
                '3YR': ['3YR', '3yr', '3Yr', 'three_year', 'threeYear', '3Y', '3y'],
                'ITD': ['ITD', 'itd', 'Itd', 'inception', 'Inception']
            }
            
            for display_name in default_headers:
                # Try to find matching key in column_keys
                found_key = None
                if display_name in key_variations:
                    for variation in key_variations[display_name]:
                        if variation in column_keys:
                            found_key = variation
                            break
                else:
                    # Direct match
                    if display_name in column_keys:
                        found_key = display_name
                
                if found_key:
                    headers.append(found_key)
                    table_header_row.append(display_name)
        
        table_header_row = [convertKeys.get(h,h) for h in table_header_row]  #convert to display header names
        if data[0].get('dataType','').lower() == 'total' and data[1].get('dataType','').lower() != 'benchmark': 
            #if total is not followed by a benchmark, follow with a blank row
            blank = {'dataType': 'Total'}
            data = [data[0], blank, *data[1:]]
            colorDepths = [colorDepths[0],colorDepths[0],*colorDepths[1:]]

        # Build table data
        table_data = []
        row_colors = []
        
        for i, row in enumerate(data):
            colorDepth = colorDepths[i]
            dType = row.get('dataType','').lower()
            if dType != "benchmark": #benchmark will use previous rounds color
                startColor = (160, 160, 160)
                if dType == "total":
                    color = tuple(
                        int(startColor[i] * 0.8)
                        for i in range(3)
                    )
                else:
                    cRange     = 255 - startColor[0]
                    color = tuple(
                        int(startColor[i] + cRange * colorDepth)
                        for i in range(3)
                    )
                # Set bg_color to the hexadecimal version of color using HexColor
                bg_color = HexColor('#%02x%02x%02x' % color)
            elif i == 0:
                #need to give a color to the benchmark as it is at the top of the table
                bg_color = HexColor(0xfafafa) #slightly darker than white
            row_colors.append(bg_color)
            
            investment = row.get('', '')
            if not investment or str(investment).lower() in ['nan', 'none', '']:
                investment = 'None'
            elif investment == 'Total':
                investment = 'Total Portfolio'
            
            # Build row data dynamically based on headers (using actual keys)
            row_data = []
            for header_key in headers:
                value = row.get(header_key)
                bold = dType not in ('benchmark', 'total target name')
                benchmark = dType == 'benchmark'
                # Try to format based on common patterns
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    row_data.append('')
                elif header_key in percent_headers:
                    if dType !='benchmark':
                        row_data.append(self._format_percent_holdings(value, bold = 'total target name' != dType, fontSize = fontSize))
                    else:
                        row_data.append(self._format_percent_holdings(value, benchmark = True, fontSize = fontSize))
                elif header_key == '':
                    row_data.append(self._format_portfolio_text(investment,bold=bold,fontSize=fontSize, benchmark = benchmark))
                elif header_key in textCols:
                    row_data.append(self._format_portfolio_text(value, fontSize=fontSize, benchmark = benchmark, styleInput = 'CustomNormalRight' if not bold else 'CustomBoldRight'))
                elif header_key not in fraction_headers:
                    row_data.append(self._format_portfolio_num(value, is_currency=True, bold = bold, fontSize = fontSize) if value is not None else '-')
                else:
                    # Default: format as number if numeric, otherwise as string
                    if isinstance(value, (int, float)) and not pd.isna(value):
                        row_data.append(self._format_portfolio_num(value, bold=bold, fontSize = fontSize))
                    else:
                        row_data.append(self._format_portfolio_num(str(value) if value is not None else '', bold=bold, fontSize = fontSize))
            
            table_data.append(row_data)

        
        # Calculate column widths dynamically (first column wider, rest evenly distributed)
        num_cols = len(headers)
        if num_cols == 0:
            return
        # Compute column widths based on header type:
        # - first column: 30% of total content width
        # - for other columns: if header in pHeaders, "percent column" gets 1 unit; otherwise "non-percent column" gets 3 units
        first_col_width = CONTENT_WIDTH * 0.32 * (fontSize / standardFontSize) #scale down if the font is smaller
        remaining_width = CONTENT_WIDTH - first_col_width

        units, total_units = headerUnits(headers[1:])
        
        col_widths = [first_col_width]
        if total_units > 0:
            per_unit_width = remaining_width / total_units
            col_widths += [per_unit_width * u for u in units]
        # Default alignments: first column left, rest right
        alignments = ['LEFT'] + ['RIGHT'] * (num_cols - 1)
        
        # Create table with special styling for holdings (use display headers for header row)
        table = Table([table_header_row] + table_data)
        table._argW[0] = first_col_width
        table_style = TableStyle([
            ('BOX', (0, 0), (-1, -1), 0, colors.white),  # No outer border
            ('BACKGROUND', (0, 0), (-1, 0), COLOR_GRAY_HEADER),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Tahoma-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), fontSize),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 1), (-1, -1), 'Tahoma'),
            ('FONTSIZE', (0, 1), (-1, -1), fontSize),
            ('LEFTPADDING', (0, 0), (-1, 0), 1),
            ('RIGHTPADDING', (0, 0), (-1, 0), 1),
            ('TOPPADDING', (0, 0), (-1, -1), rowPadding),
            ('BOTTOMPADDING', (0, 0), (-1, -1), rowPadding),
        ])
        
        for i, h in enumerate(headers):
            if h in smallHeaders: #small padding
                table_style.add('LEFTPADDING', (i, 1), (i, -1), 1)
                table_style.add('RIGHTPADDING', (i, 1), (i, -1), 1)
            else: #larger padding for numbers to not merge
                table_style.add('LEFTPADDING', (i, 1), (i, -1), 3)
                table_style.add('RIGHTPADDING', (i, 1), (i, -1), 2)


        # Add dynamic alignments for data rows
        for i, align in enumerate(alignments):
            if align == 'LEFT':
                table_style.add('ALIGN', (i, 1), (i, -1), 'LEFT')
            elif align == 'CENTER':
                table_style.add('ALIGN', (i, 1), (i, -1), 'CENTER')
            else:  # RIGHT
                table_style.add('ALIGN', (i, 1), (i, -1), 'RIGHT')
        
        
        # Add horizontal lines between all rows
        for i in range(1, len(table_data) + 1):
            if data[i - 1].get('dataType','').lower() == 'total target name': #only add line after investments. Header colors differentiate otherwise
                table_style.add('LINEBELOW', (0, i), (-1, i), 0.5, COLOR_TOTAL)
        
        # Add row colors and bold for totals
        for i, (row, color) in enumerate(zip(data, row_colors), start=1):
            if color:
                table_style.add('BACKGROUND', (0, i), (-1, i), color)
            row_type = row.get('type', '').lower()
            if row_type not in ('total target name','benchmark'): #bold for grouping totals
                table_style.add('FONTNAME', (0, i), (-1, i), f'{fontName}-Bold')
            elif 'benchmark' in row_type: #blue text
                table_style.add('TEXTCOLOR', (0, i), (-1, i), colors.blue)
                table_style.add('FONTNAME', (0, i), (-1, i), italicFontName)
                
        
        table.setStyle(table_style)
        self.story.append(table)
    def _format_portfolio_text(self,value, bold = False, fontSize = None, benchmark = False, styleInput = None):
        if styleInput:
            style = self.styles[styleInput]
        elif bold:
            style = self.styles['holdingsText-bold']
        else:
            style = self.styles['CustomNormal']
        if fontSize: #override font size
            style.fontSize = fontSize
        if benchmark: #prevents text wrapping. Slightly upwards, but the style is handled elsewhere
            return value or ''
        if value is None:
            value =  ""
        return Paragraph(f'{value}', style)
    def _format_portfolio_num(self, value, is_percent=False, is_currency=False, decimals=2, bold = False, fontSize = None):
        """Format numbers"""
        if bold:
            style = self.styles['CustomBoldRight']
        else:
            style = self.styles['CustomNormalRight']
        if fontSize: #override font size
            style.fontSize = fontSize
        if value is None or (isinstance(value, float) and pd.isna(value)):
            text =  ""
        
        if isinstance(value, str):
            text =   value
        
        if is_percent:
            if value < 0:
                text =   f"({abs(value):.{decimals}f}%)"
            else:
                text =   f"{value:.{decimals}f}%"
        
        if is_currency:
            if value < 0:
                text =   f"({abs(value):,.0f})"
            else:
                text =   f"{value:,.0f}"
        
        text =   f"{value:,.{decimals}f}"
        return Paragraph(f'{text}', style)
    def _format_percent_holdings(self, value, benchmark = False, bold=False, fontSize = None):
        """Format percentage for holdings table. Returns Paragraph or empty string."""
        # Use right-aligned style for percentages to match table cell alignment
        if benchmark:
            style = self.styles['CustomBenchmark%']
        elif bold:
            style = self.styles['CustomBoldRight']
        else:
            style = self.styles['CustomNormalRight']
        if fontSize: #override font size
            style.fontSize = fontSize
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return Paragraph('', style)  # Return empty Paragraph instead of empty string
        if isinstance(value, str):
            return Paragraph(f'<font color="red">{value}</font>', style)
        if value < 0:
            return Paragraph(f'<font color="red">{value:.1f}%</font>', style)
        if benchmark:
            return Paragraph(f"<font color='blue'><i>{value:.1f}%</i></font>", style)
        else:
            return Paragraph(f"{value:.1f}%", style)
    
    def add_benchmark_table(self, data: List[Dict]):
        """Add Benchmark table."""
        headers = ['Benchmark', 'Info', 'MTD', 'QTD', 'YTD', '1Y', '3Y', '5Y', '10Y']
        table_data = []
        
        for row in data:
            table_data.append([
                row.get('Benchmark', ''),
                row.get('Info', ''),
                self._format_benchmark_value(row.get('MTD')),
                self._format_benchmark_value(row.get('QTD')),
                self._format_benchmark_value(row.get('YTD')),
                self._format_benchmark_value(row.get('1Y')),
                self._format_benchmark_value(row.get('3Y')),
                self._format_benchmark_value(row.get('5Y')),
                self._format_benchmark_value(row.get('10Y'))
            ])
        
        col_widths = [
            CONTENT_WIDTH * 0.25,  # Benchmark
            CONTENT_WIDTH * 0.30,  # Info
            CONTENT_WIDTH * 0.064, # MTD
            CONTENT_WIDTH * 0.064, # QTD
            CONTENT_WIDTH * 0.064, # YTD
            CONTENT_WIDTH * 0.064, # 1Y
            CONTENT_WIDTH * 0.064, # 3Y
            CONTENT_WIDTH * 0.064, # 5Y
            CONTENT_WIDTH * 0.064  # 10Y
        ]
        alignments = ['LEFT', 'LEFT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT', 'RIGHT']
        
        caption = self._create_table_caption('Benchmark Table')
        caption_style = TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ])
        caption.setStyle(caption_style)
        
        table = self._create_panel_table(headers, table_data, col_widths, alignments=alignments)
        self.story.append(caption)
        self.story.append(table)
    
    def _format_benchmark_value(self, value):
        """Format benchmark percentage values. Returns Paragraph for proper HTML rendering."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return Paragraph('-', self.styles['CustomNormal'])  # Return Paragraph instead of string
        val_pct = value * 100
        if val_pct < 0:
            return Paragraph(f'<font color="red">({abs(val_pct):.1f}%)</font>', self.styles['CustomNormal'])
        return Paragraph(f"{val_pct:.1f}%", self.styles['CustomNormal'])
    
    def add_benchmark_charts(self, chart_mtd_path: str = None, chart_1y_path: str = None):
        """Add benchmark charts side by side."""
        if chart_mtd_path and Path(chart_mtd_path).exists():
            img_mtd = Image(chart_mtd_path, width=CONTENT_WIDTH * 0.48, height=CONTENT_WIDTH * 0.4)
        else:
            img_mtd = None
        
        if chart_1y_path and Path(chart_1y_path).exists():
            img_1y = Image(chart_1y_path, width=CONTENT_WIDTH * 0.48, height=CONTENT_WIDTH * 0.4)
        else:
            img_1y = None
        
        if img_mtd or img_1y:
            chart_data = []
            if img_mtd:
                chart_data.append(img_mtd)
            else:
                chart_data.append(Spacer(CONTENT_WIDTH * 0.48, CONTENT_WIDTH * 0.4))
            
            if img_1y:
                chart_data.append(img_1y)
            else:
                chart_data.append(Spacer(CONTENT_WIDTH * 0.48, CONTENT_WIDTH * 0.4))
            
            chart_table = Table([chart_data], colWidths=[CONTENT_WIDTH * 0.48, CONTENT_WIDTH * 0.48])
            chart_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            self.story.append(chart_table)
            self.story.append(Spacer(1, 0.3*inch))
    
    def add_jpm_tables(self, usage_data: List[Dict], collateral_data: List[Dict], 
                      letters_of_credit: float = None, commentary_html: str = '', report_date: str = ''):
        """Add JPM LOC tables."""
        # Header with logos
        jpm_logo_path = self.assets_dir / "jpm logo.png"
        hf_logo_path = self.assets_dir / "logo.png"
        
        logo_cells = []
        if jpm_logo_path.exists():
            jpm_logo = Image(str(jpm_logo_path), width=1.5*inch, height=1.0*inch)
            logo_cells.append(jpm_logo)
        else:
            logo_cells.append(Spacer(1, 1.0*inch))
        
        if hf_logo_path.exists():
            hf_logo = Image(str(hf_logo_path), width=1.5*inch, height=1.2*inch)
            logo_cells.append(hf_logo)
        else:
            logo_cells.append(Spacer(1, 1.2*inch))
        
        header_title = Paragraph(
            '<font size="20" color="#1f4e78">Current JP Morgan Line of Credit & Collateral Positions</font>',
            self.styles['CustomNormal']
        )
        
        header_table = Table([
            [header_title, logo_cells]
        ], colWidths=[CONTENT_WIDTH * 0.7, CONTENT_WIDTH * 0.3])
        header_table.setStyle(TableStyle([
            ('LINEBELOW', (0, 0), (-1, 0), 2, COLOR_PRIMARY),
            ('VALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ]))
        self.story.append(header_table)
        self.story.append(Spacer(1, 0.15*inch))
        
        # Usage and Collateral tables side by side
        usage_headers = ['Type', 'Size', 'Cost', 'Amount Drawn']
        usage_table_data = []
        usage_row_colors = []
        
        for row in usage_data:
            row_type = str(row.get('type', '')).lower()
            is_secured = row_type == 'secured'
            is_total = row_type == 'total'
            
            usage_row_colors.append(HexColor(0xdbead5) if is_secured else (COLOR_TOTAL if is_total else None))
            
            usage_table_data.append([
                row.get('type', ''),
                self._format_number(row.get('size'), is_currency=True),
                row.get('cost', ''),
                self._format_number(row.get('amount_drawn'), is_currency=True, decimals=1) if row.get('amount_drawn') else ''
            ])
        
        usage_col_widths = [CONTENT_WIDTH * 0.48 * 0.25, CONTENT_WIDTH * 0.48 * 0.25, 
                           CONTENT_WIDTH * 0.48 * 0.25, CONTENT_WIDTH * 0.48 * 0.25]
        
        usage_caption = Paragraph(
            '<font size="14">Current Usage of Line of Credit Facility</font><br/>'
            f'<font size="10" color="#666666">As of {report_date}; Millions of USD</font>',
            self.styles['CustomNormal']
        )
        
        usage_table = self._create_panel_table(usage_headers, usage_table_data, usage_col_widths,
                                              alignments=['CENTER', 'CENTER', 'CENTER', 'CENTER'],
                                              row_colors=usage_row_colors)
        
        # Collateral table
        collateral_headers = ['Collateral Account', 'Market Value', 'Lending Value', '%']
        collateral_table_data = []
        collateral_row_colors = []
        
        for row in collateral_data:
            is_total = str(row.get('account', '')).lower() == 'total'
            collateral_row_colors.append(COLOR_TOTAL if is_total else None)
            
            collateral_table_data.append([
                row.get('account', ''),
                self._format_number(row.get('market_value'), is_currency=True, decimals=1),
                self._format_number(row.get('lending_value'), is_currency=True, decimals=2),
                self._format_number(row.get('pct'), is_percent=True, decimals=0)
            ])
        
        collateral_col_widths = [CONTENT_WIDTH * 0.48 * 0.35, CONTENT_WIDTH * 0.48 * 0.22,
                                CONTENT_WIDTH * 0.48 * 0.22, CONTENT_WIDTH * 0.48 * 0.21]
        
        collateral_caption = Paragraph(
            '<font size="14">Current Collateral Against Secured Line of Credit</font><br/>'
            f'<font size="10" color="#666666">As of {report_date}; Millions of USD</font>',
            self.styles['CustomNormal']
        )
        
        # Create side-by-side layout
        side_by_side_data = [
            [usage_caption, collateral_caption],
            [usage_table, self._create_panel_table(collateral_headers, collateral_table_data, 
                                                  collateral_col_widths,
                                                  alignments=['LEFT', 'RIGHT', 'RIGHT', 'CENTER'],
                                                  row_colors=collateral_row_colors)]
        ]
        
        side_by_side_table = Table(side_by_side_data, colWidths=[CONTENT_WIDTH * 0.48, CONTENT_WIDTH * 0.48])
        side_by_side_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        self.story.append(side_by_side_table)
        
        # Letters of Credit box
        if letters_of_credit:
            loc_box = Table([[
                Paragraph('<font size="10"><b>Letters of Credit</b></font>', self.styles['CustomNormal']),
                Paragraph(f'<font size="10">{self._format_number(letters_of_credit, is_currency=True, decimals=1)} <sup>(1)</sup></font>', 
                         self.styles['CustomNormal'])
            ]], colWidths=[CONTENT_WIDTH * 0.3, CONTENT_WIDTH * 0.7])
            loc_box.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), HexColor(0xeeff)),
                ('BOX', (0, 0), (-1, -1), 1, HexColor(0xcceeff)),
                ('LEFTPADDING', (0, 0), (-1, -1), 15),
                ('RIGHTPADDING', (0, 0), (-1, -1), 15),
                ('TOPPADDING', (0, 0), (-1, -1), 15),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))
            self.story.append(Spacer(1, 0.15*inch))
            self.story.append(loc_box)
        
        # Commentary
        if commentary_html:
            commentary_title = Paragraph(
                '<font size="14" color="#1f4e78">JPMorgan Unsecured Advised Line & Bank of America Key Requirements</font>',
                self.styles['CustomNormal']
            )
            self.story.append(Spacer(1, 0.1*inch))
            self.story.append(commentary_title)
            self.story.append(Spacer(1, 0.05*inch))
            
            commentary_para = Paragraph(
                commentary_html.replace('<p>', '').replace('</p>', '<br/>'),
                ParagraphStyle(
                    name='Commentary',
                    parent=self.styles['Normal'],
                    fontName=f'{fontName}',
                    fontSize=FONT_SIZE_NORMAL,
                    leading=FONT_SIZE_NORMAL * 1.25,
                    textColor=HexColor(0x444444)
                )
            )
            self.story.append(commentary_para)
    
    def build(self):
        """Build the PDF document with two-pass build for page numbering."""
        # Create document with custom canvas
        doc = BaseDocTemplate(
            str(self.output_path),
            pagesize=landscape(letter),
            rightMargin=MARGIN_RIGHT,
            leftMargin=MARGIN_LEFT,
            topMargin=MARGIN_TOP,
            bottomMargin=MARGIN_BOTTOM
        )
        
        # Create standard single-column frame
        standard_frame = Frame(
            MARGIN_LEFT, MARGIN_BOTTOM, CONTENT_WIDTH, CONTENT_HEIGHT,
            leftPadding=0, bottomPadding=0, rightPadding=0, topPadding=0,
            id='standard'
        )
        
        # Use standard single-column template
        standard_template = PageTemplate(id='Standard', frames=[standard_frame])
        doc.addPageTemplates([standard_template])
        
        # Build with two-pass for page numbering
        doc.build(self.story, canvasmaker=lambda filename, **kwargs: NumberedCanvas(filename, footerData=self.footerData, **kwargs))


if __name__ == '__main__':
    """Just a test for fonts"""
    class testCanvas(canvas.Canvas):
        #Test to see fonts
        def __init__(self, *args, **kwargs):
            canvas.Canvas.__init__(self, *args, **kwargs)
            fonts = self.getAvailableFonts()
            print(f' fonts for blah: {fonts}')
            self.setFont('Tahoma-Bold',5)
            self.drawString(10, 150, "Some text encoded in UTF-8")
            self.drawString(10, 100, "In the Vera TT Font!")
            canvas.Canvas.save(self)
    main = testCanvas('testOutputs/auto.pdf')
