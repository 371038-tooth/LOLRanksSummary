import matplotlib
# Use Agg backend for thread safety and headless environments
matplotlib.use('Agg')

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.dates as mdates
from datetime import date, timedelta
import io
import os

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
from typing import List, Dict, Any

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
    if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        # We give Master 2800, GM 3200, Challenger 3600 base? 
        # Or just use the index.
        # Let's say Master starts at 2800.
        return tier_val + lp
    
    div_val = DIV_MAP.get(division, 0) * 100
    return tier_val + div_val + lp

def numeric_to_rank(val: int) -> str:
    """Convert numeric value back to a human-readable rank label (approximate)."""
    tier_idx = val // 400
    if tier_idx >= len(TIER_ORDER):
        tier_idx = len(TIER_ORDER) - 1
    
    tier = TIER_ORDER[tier_idx]
    
    if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        lp = val % 400 # This is a bit arbitrary for Apex
        return f"{tier}"
    
    rem = val % 400
    div_idx = rem // 100
    lp = rem % 100
    
    div_names = ["IV", "III", "II", "I"]
    div = div_names[div_idx] if div_idx < 4 else "I"
    
    return f"{tier} {div}"

def generate_rank_graph(user_data: Dict[str, List[Dict[str, Any]]], period_type: str, title_suffix: str = "") -> io.BytesIO:
    """
    Generate a rank history graph for one or more users.
    user_data: Dict mapping riot_id -> List of historical entries {'fetch_date', 'tier', 'rank', 'lp'}
    period_type: 'daily', 'weekly', 'monthly'
    title_suffix: Optional suffix for the title
    """
    if not user_data:
        return None

    # Use Figure object directly instead of pyplot state
    fig = Figure(figsize=(12, 7))
    FigureCanvasAgg(fig) # Attach canvas
    ax = fig.add_subplot(111)
    
    ax.set_facecolor('#2c3e50')
    fig.set_facecolor('#34495e')

    # Color palette
    colors = ['#1abc9c', '#3498db', '#9b59b6', '#f1c40f', '#e67e22', '#e74c3c', '#ecf0f1', '#95a5a6']
    
    all_dates = []
    all_values = []
    
    for i, (riot_id, rows) in enumerate(user_data.items()):
        if not rows:
            continue

        # --- Aggregation logic for Weekly/Monthly ---
        if period_type == 'weekly':
            weeks = {}
            for r in rows:
                year, week, _ = r['fetch_date'].isocalendar()
                weeks[(year, week)] = r # Latest entry in each week
            rows = sorted(weeks.values(), key=lambda x: x['fetch_date'])
        elif period_type == 'monthly':
            months = {}
            for r in rows:
                key = (r['fetch_date'].year, r['fetch_date'].month)
                months[key] = r # Latest entry in each month
            rows = sorted(months.values(), key=lambda x: x['fetch_date'])

        # Filter by year logic (consistent with previous requirement)
        latest_date = max(r['fetch_date'] for r in rows)
        earliest_date = min(r['fetch_date'] for r in rows)
        if earliest_date.year < latest_date.year:
            start_filter = date(latest_date.year, 1, 1)
            rows = [r for r in rows if r['fetch_date'] >= start_filter]
            if not rows: continue

        dates = [r['fetch_date'] for r in rows]
        values = [rank_to_numeric(r['tier'], r['rank'], r['lp']) for r in rows]
        
        all_dates.extend(dates)
        all_values.extend(values)
        
        color = colors[i % len(colors)]
        name = riot_id.split('#')[0]
        
        # Plot line
        ax.plot(dates, values, marker='o', linestyle='-', color=color, linewidth=2, markersize=5, label=name)
        
        # Add LP annotations only for the latest point if multiple users, or all points if single user
        if len(user_data) == 1:
            for j, r in enumerate(rows):
                ax.annotate(f"{r['lp']}LP", (dates[j], values[j]), 
                            textcoords="offset points", xytext=(0, 10), ha='center', 
                            fontsize=9, color='white', alpha=0.8)
        else:
            # Annotate only the last point for clarity in multi-user graphs
            last_r = rows[-1]
            ax.annotate(f"{name}: {last_r['lp']}LP", (dates[-1], values[-1]), 
                        textcoords="offset points", xytext=(0, 10), ha='center', 
                        fontsize=9, color=color, weight='bold')

    # Title and Labels
    title = f"Rank History{title_suffix} ({period_type})"
    ax.set_title(title, fontsize=18, color='white', pad=25, weight='bold')
    ax.set_xlabel("Date", fontsize=12, color='white', labelpad=10)
    ax.set_ylabel("Rank", fontsize=12, color='white', labelpad=10)

    # Tick colors and sizes
    ax.tick_params(colors='white', labelsize=10)
    for spine in ax.spines.values():
        spine.set_color('#7f8c8d')

    # Date Formatting
    from matplotlib.ticker import FuncFormatter, FixedLocator

    if period_type == 'daily':
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator())
    elif period_type in ['weekly', 'monthly']:
        # Use FixedLocator to ensure ticks are exactly on the data points
        unique_dates = sorted(list(set(all_dates)))
        if unique_dates:
            ax.xaxis.set_major_locator(FixedLocator(mdates.date2num(unique_dates)))
        
        if period_type == 'weekly':
            def format_weekly(x, pos):
                d = mdates.num2date(x)
                # Adjust mapping to match scheduler logic: week_num = (d.day - 1) // 7 + 1
                week_num = (d.day - 1) // 7 + 1
                return f"{d.month}月/{week_num}週目"
            ax.xaxis.set_major_formatter(FuncFormatter(format_weekly))
        else: # monthly
            def format_monthly(x, pos):
                d = mdates.num2date(x)
                return f"{d.month}月"
            ax.xaxis.set_major_formatter(FuncFormatter(format_monthly))

    # Rotation should be set on the axes tick labels
    # plt.xticks(rotation=45) -> ax.tick_params(axis='x', rotation=45)
    ax.tick_params(axis='x', rotation=45)

    # Y-axis range and labels
    if all_values:
        min_v, max_v = min(all_values), max(all_values)
        # Use division-based limits without extra padding
        y_min = (min_v // 100) * 100
        y_max = (max_v // 100 + 1) * 100
        
        ax.set_ylim(y_min, y_max)
        
        y_ticks = list(range(int(y_min), int(y_max) + 1, 100))
        y_labels = [numeric_to_rank(t) for t in y_ticks]
        
        # Adjust Y-axis font size if too many ticks
        fontsize = 10 if len(y_ticks) < 15 else 8
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels, fontsize=fontsize)

    # Legend
    if len(user_data) > 1:
        leg = ax.legend(loc='upper left', bbox_to_anchor=(1, 1), facecolor='#34495e', edgecolor='#7f8c8d')
        for text in leg.get_texts():
            text.set_color('white')

    ax.grid(True, linestyle='--', alpha=0.1, color='#95a5a6')

    buf = io.BytesIO()
    # Use fig.savefig instead of plt.savefig
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=False, dpi=110)
    buf.seek(0)
    return buf

def generate_report_image(headers: List[str], data: List[List[Any]], title: str, col_widths: List[float] = None) -> io.BytesIO:
    """
    Generate a clean table image using matplotlib.
    """
    if not data:
        return None

    # Calculate figure height based on number of rows
    row_height = 0.5
    header_height = 0.6
    fig_height = header_height + (len(data) * row_height) + 1.0
    
    # Use Figure object directly
    # Riot ID column is usually longest
    fig = Figure(figsize=(14, max(4, fig_height)))
    FigureCanvasAgg(fig) # Attach canvas
    ax = fig.add_subplot(111)
    
    ax.axis('off')
    fig.set_facecolor('#34495e')

    # Color configuration
    header_color = '#2c3e50'
    row_colors = ['#34495e', '#2c3e50']
    text_color = 'white'

    # Create table
    # We estimate column widths based on headers and generic needs if not provided
    if col_widths is None:
        col_widths = [0.15] + [0.08] * (len(headers) - 4) + [0.22, 0.22, 0.1]
    
    table = ax.table(
        cellText=data,
        colLabels=headers,
        loc='center',
        cellLoc='center',
        colColours=[header_color] * len(headers),
        colWidths=col_widths
    )

    # Style table
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.0, 2.5) # Scale height for readability

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('#7f8c8d')
        if row == 0:
            cell.set_text_props(weight='bold', color=text_color)
        else:
            cell.set_facecolor(row_colors[row % len(row_colors)])
            cell.set_text_props(color=text_color)
            # Make Riot ID (column 0) left-aligned for better readability
            if col == 0:
                cell.get_text().set_horizontalalignment('left')
                # Add a small offset for padding
                cell.get_text().set_position((0.05, 0.5))

    ax.set_title(title, fontsize=18, color=text_color, pad=30, weight='bold')

    buf = io.BytesIO()
    # Use fig.savefig with facecolor
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=False, dpi=120, facecolor='#34495e')
    buf.seek(0)
    return buf
