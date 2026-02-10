import unicodedata

TIER_ORDER = {
    "IRON": 0,
    "BRONZE": 1,
    "SILVER": 2,
    "GOLD": 3,
    "PLATINUM": 4,
    "EMERALD": 5,
    "DIAMOND": 6,
    "MASTER": 7,
    "GRANDMASTER": 8,
    "CHALLENGER": 9
}

RANK_ORDER = {
    "IV": 0,
    "III": 1,
    "II": 2,
    "I": 3
}

# Base LP for each Tier (400 LP per tier for standard tiers)
# Master+ starts after Diamond I (which is Diamond Base + 300LP + 100LP to promo)
# So Master Base = Diamond Base + 400
TIER_BASE_LP = {
    "IRON": 0,
    "BRONZE": 400,
    "SILVER": 800,
    "GOLD": 1200,
    "PLATINUM": 1600,
    "EMERALD": 2000,
    "DIAMOND": 2400,
    "MASTER": 2800,
    "GRANDMASTER": 2800, # GM and Challenger share the same base as Master (sort of, raw LP distinguishes them)
    "CHALLENGER": 2800
}

def get_total_lp(tier: str, rank: str, lp: int) -> int:
    """
    Calculate the normalized total LP for comparison.
    """
    tier_upper = tier.upper()
    if tier_upper not in TIER_BASE_LP:
        return 0
    
    base = TIER_BASE_LP[tier_upper]
    
    if tier_upper in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        # Apex tiers do not have divisions (I, II, III, IV) usually, or if they do, we ignore for LP calc?
        # API returns "I" for rank in Apex tiers usually.
        # We just add LP to base.
        return base + lp
    
    # Standard tiers
    rank_upper = rank.upper()
    rank_val = RANK_ORDER.get(rank_upper, 0)
    
    return base + (rank_val * 100) + lp

def format_rank_diff(diff: int) -> str:
    """
    Format the LP difference (e.g., "+99LP", "±0LP", "-51LP").
    """
    if diff > 0:
        return f"+{diff}LP"
    elif diff < 0:
        return f"{diff}LP"
    else:
        return "±0LP"

def calculate_diff_text(old_data: dict, new_data: dict, include_prefix: bool = True) -> str:
    """
    Calculate textual representation of rank change.
    e.g. "Tier DII⇒DI LP: +99LP" or "Tier:変化なし LP:±0LP"
    """
    if not old_data or not new_data:
        return "-"
        
    old_total = get_total_lp(old_data['tier'], old_data['rank'], old_data['lp'])
    new_total = get_total_lp(new_data['tier'], new_data['rank'], new_data['lp'])
    
    diff = new_total - old_total
    
    old_str = f"{shorten_tier(old_data['tier'])}{old_data['rank']}"
    new_str = f"{shorten_tier(new_data['tier'])}{new_data['rank']}"
    
    # For Apex tiers, rank might be "I", often omitted in display but kept in logic
    if old_data['tier'] in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        old_str = shorten_tier(old_data['tier'])
    if new_data['tier'] in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        new_str = shorten_tier(new_data['tier'])

    lp_diff_str = format_rank_diff(diff)
    
    if old_str == new_str:
        content = f"Tier:変化なし LP:{lp_diff_str}"
    else:
        content = f"Tier {old_str} → {new_str} LP: {lp_diff_str}"

    # Add Win/Loss record
    w = new_data.get('wins', 0) - old_data.get('wins', 0)
    l = new_data.get('losses', 0) - old_data.get('losses', 0)
    g = w + l
    
    if g > 0:
        rate = int((w / g) * 100)
        content += f" ({w}勝{l}敗 {rate}%)"

    if not include_prefix:
        return content

    return content

def shorten_tier(tier: str) -> str:
    """Shorten Tier name for display (e.g. DIAMOND -> D)."""
    mapping = {
        "IRON": "I",
        "BRONZE": "B",
        "SILVER": "S",
        "GOLD": "G",
        "PLATINUM": "P",
        "EMERALD": "E",
        "DIAMOND": "D",
        "MASTER": "M",
        "GRANDMASTER": "GM",
        "CHALLENGER": "C"
    }
    return mapping.get(tier.upper(), tier[0])

def format_rank_display(tier: str, rank: str, lp: int) -> str:
    """Format rank for table cell (e.g. 'DII 21LP')."""
    short_tier = shorten_tier(tier)
    if tier.upper() in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
        return f"{short_tier} {lp}LP"
    return f"{short_tier}{rank} {lp}LP"

def get_display_width(s):
    """Calculate display width considering full-width and ambiguous characters."""
    width = 0
    for char in str(s):
        eaw = unicodedata.east_asian_width(char)
        # 'W' (Wide), 'F' (Fullwidth) are 2 cells.
        # 'A' (Ambiguous) characters like ±, ⇒ are treated as 1 cell for safer Discord monospaced font support.
        if eaw in ('W', 'F'):
            width += 2
        else:
            width += 1
    return width

def pad_string(s, width):
    """Pad string with spaces to reach visual width."""
    s_str = str(s)
    current_w = get_display_width(s_str)
    return s_str + (" " * max(0, width - current_w))
