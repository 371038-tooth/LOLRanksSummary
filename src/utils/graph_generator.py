import matplotlib
# Use Agg backend for thread safety and headless environments
matplotlib.use('Agg')

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter, FixedLocator
from datetime import date
import io
import os
from typing import List, Dict, Any

import logging
# Silence matplotlib font warnings
logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)

# --- Style Constants ---
BG_COLOR = '#0b0f19'
AXIS_COLOR = '#334155'
TEXT_COLOR = '#f8fafc'
SECONDARY_TEXT = '#94a3b8'
HEADER_BG = '#1e293b'
ROW_EVEN = '#111827'
UP_COLOR = '#4ade80'   # Emerald-400
DOWN_COLOR = '#fb7185' # Rose-400
COLORS = ['#06b6d4', '#10b981', '#8b5cf6', '#f59e0b', '#ef4444', '#ec4899', '#3b82f6', '#14b8a6']
APEX_TIERS = {"MASTER", "GRANDMASTER", "CHALLENGER"}

# Set Japanese font for Windows and Linux (Railway)
# Use local font file for better portability
font_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'assets', 'fonts', 'JapaneseFont.otf')
if os.path.exists(font_path):
    from matplotlib import font_manager
    font_manager.fontManager.addfont(font_path)
    prop = font_manager.FontProperties(fname=font_path)
    matplotlib.rcParams['font.family'] = [prop.get_name(), 'Meiryo', 'MS Gothic', 'Yu Gothic', 'sans-serif']
else:
    # Fallback
    matplotlib.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'Yu Gothic', 'sans-serif']

# Rank Mapping
TIER_ORDER = [
    "IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", 
    "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER"
]

DIV_MAP = {"I": 3, "II": 2, "III": 1, "IV": 0}

def rank_to_numeric(tier: str, division: str, lp: int) -> int:
    """Convert Tier/Division/LP to a single numeric value for graphing."""
    tier = tier.upper()
    if tier not in TIER_ORDER:
        return 0
    
    tier_val = TIER_ORDER.index(tier) * 400
    
    # Apex tiers don't have divisions
    if tier in APEX_TIERS:
        return tier_val + lp
    
    div_val = DIV_MAP.get(division, 0) * 100
    return tier_val + div_val + lp

def numeric_to_rank(val: int) -> str:
    """Convert numeric value back to a human-readable rank label (approximate)."""
    tier_idx = val // 400
    if tier_idx >= len(TIER_ORDER):
        tier_idx = len(TIER_ORDER) - 1
    
    tier = TIER_ORDER[tier_idx]
    tier_offset = val % 400
    
    if tier in APEX_TIERS:
        if tier_offset == 0:
            return f"{tier}"
        else:
            return f"+{tier_offset}LP"
    
    div_idx = tier_offset // 100
    div_names = ["IV", "III", "II", "I"]
    div = div_names[div_idx] if div_idx < 4 else "I"
    
    return f"{tier} {div}"

def _aggregate_rows(rows: List[Dict[str, Any]], period_type: str) -> List[Dict[str, Any]]:
    """Aggregate rows by period (weekly/monthly) and filter to latest year."""
    if not rows:
        return []
    
    if period_type == 'weekly':
        weeks = {}
        for r in rows:
            year, week, _ = r['fetch_date'].isocalendar()
            weeks[(year, week)] = r
        rows = sorted(weeks.values(), key=lambda x: x['fetch_date'])
    elif period_type == 'monthly':
        months = {}
        for r in rows:
            key = (r['fetch_date'].year, r['fetch_date'].month)
            months[key] = r
        rows = sorted(months.values(), key=lambda x: x['fetch_date'])

    latest_date = max(r['fetch_date'] for r in rows)
    earliest_date = min(r['fetch_date'] for r in rows)
    if earliest_date.year < latest_date.year:
        start_filter = date(latest_date.year, 1, 1)
        rows = [r for r in rows if r['fetch_date'] >= start_filter]

    return rows


def generate_rank_graph(user_data: Dict[str, List[Dict[str, Any]]], period_type: str, title_suffix: str = "") -> io.BytesIO:
    """
    Generate a rank history graph for one or more users with a stylish neon dark-mode design.
    """
    if not user_data:
        return None

    fig = Figure(figsize=(12, 7), facecolor=BG_COLOR)
    FigureCanvasAgg(fig)
    ax = fig.add_subplot(111)
    ax.set_facecolor(BG_COLOR)
    
    all_dates = []
    all_values = []
    today_obj = date.today()
    
    # Pre-aggregate all user data once
    aggregated_data = {}
    for riot_id, rows in user_data.items():
        processed = _aggregate_rows(rows, period_type)
        if processed:
            aggregated_data[riot_id] = processed
    
    # Draw lines
    has_today = False
    latest_fetch_time = None
    label_items = []
    
    for i, (riot_id, rows) in enumerate(aggregated_data.items()):
        dates = [r['fetch_date'] for r in rows]
        values = [rank_to_numeric(r['tier'], r['rank'], r['lp']) for r in rows]
        all_dates.extend(dates)
        all_values.extend(values)
        
        color = COLORS[i % len(COLORS)]
        name = riot_id.split('#')[0]
        
        # Neon Glow Effect: Plot multiple times with different alpha and width
        ax.plot(dates, values, color=color, linewidth=12, alpha=0.05, zorder=2)
        ax.plot(dates, values, color=color, linewidth=8, alpha=0.1, zorder=3)
        ax.plot(dates, values, color=color, linewidth=4, alpha=0.2, zorder=4)
        ax.plot(dates, values, color=color, linewidth=2.5, linestyle='-', marker='o', 
                markersize=4, markerfacecolor='white', markeredgewidth=1.5, zorder=5, label=name)
        
        # Single-user LP labels
        if len(aggregated_data) == 1:
            for j, r in enumerate(rows):
                ax.annotate(f"{r['lp']}LP", (dates[j], values[j]), 
                            textcoords="offset points", xytext=(0, 10), ha='center', 
                            fontsize=9, color=TEXT_COLOR, alpha=0.9, weight='bold')

        # Track latest fetch time for title
        for r in rows:
            if r['fetch_date'] == today_obj:
                has_today = True
                if 'reg_date' in r and r['reg_date'] is not None:
                    if latest_fetch_time is None or r['reg_date'] > latest_fetch_time:
                        latest_fetch_time = r['reg_date']

        # Prepare multi-user label data
        if len(aggregated_data) > 1:
            last_r = rows[-1]
            label_items.append({
                'name': name,
                'lp': last_r['lp'],
                'y': rank_to_numeric(last_r['tier'], last_r['rank'], last_r['lp']),
                'color': color,
                'x': last_r['fetch_date']
            })

    # Draw multi-user labels with overlap adjustment
    if len(aggregated_data) > 1:
        label_items.sort(key=lambda x: x['y'], reverse=True)
        THRESHOLD = 25 # Minimum vertical gap in rank points
        for i in range(1, len(label_items)):
            diff = label_items[i-1]['y'] - label_items[i]['y']
            if diff < THRESHOLD:
                label_items[i]['y'] = label_items[i-1]['y'] - THRESHOLD
        
        for item in label_items:
            ax.annotate(f"{item['name']}: {item['lp']}LP", (item['x'], item['y']), 
                        textcoords="offset points", xytext=(0, 12), ha='center', 
                        fontsize=9, color=item['color'], weight='bold')

    title_time_str = ""
    if latest_fetch_time:
        title_time_str = f"({latest_fetch_time.hour}:{latest_fetch_time.minute:02d}時点)"
    
    title_prefix = "Rank History"
    if has_today:
        title_prefix += f" {title_time_str}" if title_time_str else " (現在)"
    
    title_full = f"{title_prefix}{title_suffix} ({period_type})"
    ax.set_title(title_full, fontsize=22, color=TEXT_COLOR, pad=35, weight='bold')

    if has_today:
        fig.text(0.97, 0.03, "※当日分は定期実行時のデータです", ha='right', fontsize=13, 
                 color=TEXT_COLOR, weight='bold')

    ax.set_xlabel("Date", fontsize=12, color=SECONDARY_TEXT, labelpad=12)
    ax.set_ylabel("Rank", fontsize=12, color=SECONDARY_TEXT, labelpad=12)

    ax.tick_params(colors=SECONDARY_TEXT, labelsize=10, length=0) # length=0 hides tick dashes
    for spine in ax.spines.values():
        spine.set_visible(False) # Hide all spines for a cleaner look
    
    ax.grid(True, linestyle='--', alpha=0.1, color=SECONDARY_TEXT)

    # Date Formatting
    if period_type == 'daily':
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator())
    elif period_type in ['weekly', 'monthly']:
        unique_dates = sorted(list(set(all_dates)))
        if unique_dates:
            ax.xaxis.set_major_locator(FixedLocator(mdates.date2num(unique_dates)))
        
        if period_type == 'weekly':
            def format_weekly(x, pos):
                d = mdates.num2date(x)
                week_num = (d.day - 1) // 7 + 1
                return f"{d.month}月/{week_num}週目"
            ax.xaxis.set_major_formatter(FuncFormatter(format_weekly))
        else: # monthly
            def format_monthly(x, pos):
                d = mdates.num2date(x)
                return f"{d.month}月"
            ax.xaxis.set_major_formatter(FuncFormatter(format_monthly))

    ax.tick_params(axis='x', rotation=45)

    if all_values:
        min_v, max_v = min(all_values), max(all_values)
        y_min = (min_v // 100) * 100
        y_max = (max_v // 100 + 1) * 100
        
        # Apex Tier scaling: If max is MASTER/GM, try to show the next tier threshold
        max_tier_idx = int(max_v // 400)
        if max_tier_idx < len(TIER_ORDER) - 1:
            tier_name = TIER_ORDER[max_tier_idx]
            if tier_name in {"MASTER", "GRANDMASTER"}:
                # Force top to next tier boundary
                y_max = (max_tier_idx + 1) * 400
        ax.set_ylim(y_min, y_max)
        
        y_ticks = list(range(int(y_min), int(y_max) + 1, 100))
        y_labels = [numeric_to_rank(t) for t in y_ticks]
        
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels, fontsize=10)

    if len(aggregated_data) > 1:
        # Move legend outside the plot area to the right
        leg = ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), 
                         facecolor=BG_COLOR, edgecolor=AXIS_COLOR, borderaxespad=0)
        for text in leg.get_texts():
            text.set_color(TEXT_COLOR)
            text.set_weight('bold')

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=False, dpi=120)
    buf.seek(0)
    del fig  # Release Figure resources to prevent memory leak
    return buf

def generate_report_image(headers: List[str], data: List[List[Any]], title: str, col_widths: List[float] = None) -> io.BytesIO:
    """
    Generate a modern minimalist table image.
    """
    if not data:
        return None

    row_height = 0.6
    header_height = 0.8
    fig_height = header_height + (len(data) * row_height) + 1.2
    
    # Dynamic figsize based on number of columns
    base_width = max(14, len(headers) * 1.8)
    fig = Figure(figsize=(base_width, max(4, fig_height)), facecolor=BG_COLOR)
    FigureCanvasAgg(fig)
    ax = fig.add_subplot(111)
    ax.axis('off')

    # Create table setup
    table_kwargs = {
        'cellText': data,
        'colLabels': headers,
        'loc': 'center',
        'cellLoc': 'center'
    }
    if col_widths is not None:
        table_kwargs['colWidths'] = col_widths

    table = ax.table(**table_kwargs)

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    
    # We will call auto_set_column_width AFTER setting padding for each cell
    table.scale(1.0, 3.5) 

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(HEADER_BG)
        cell.set_linewidth(0.5)
        # Increase padding to prevent text from touching edges
        cell.set_text_props(ha='center', va='center')
        cell.PAD = 0.1 # increased padding (default is usually too small)
        
        if row == 0:
            cell.set_facecolor(HEADER_BG)
            cell.set_text_props(weight='bold', color='white', fontsize=13)
        else:
            cell.set_facecolor(BG_COLOR if row % 2 == 0 else ROW_EVEN)
            cell.set_text_props(color=TEXT_COLOR)
            
            # Trend Coloring based on header name
            header_text = headers[col]
            if "比" in header_text or "差" in header_text:
                val_text = str(data[row-1][col])
                if '+' in val_text:
                    cell.get_text().set_color(UP_COLOR)
                    cell.get_text().set_weight('bold')
                elif '-' in val_text:
                    cell.get_text().set_color(DOWN_COLOR)
                    cell.get_text().set_weight('bold')

            if col == 0:
                cell.get_text().set_horizontalalignment('left')
                # Add offset to prevent touching the left frame
                cell.get_text().set_position((0.05, 0.5))

    # Finally perform auto column adjustment if no fixed widths provided
    if col_widths is None:
        table.auto_set_column_width(col=list(range(len(headers))))

    ax.set_title(title, fontsize=24, color='white', pad=45, weight='bold')

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=False, dpi=120, facecolor=BG_COLOR)
    buf.seek(0)
    del fig  # Release Figure resources to prevent memory leak
    return buf
