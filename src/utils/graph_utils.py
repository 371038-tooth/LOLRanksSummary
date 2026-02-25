from typing import List, Dict, Any
from .rank_calculator import get_total_lp

def split_user_data_by_rank(user_data: Dict[str, List[Dict[str, Any]]], max_users: int = 8, max_rank_diff: int = 1200) -> List[Dict[str, List[Dict[str, Any]]]]:
    """
    Split user data into groups based on rank proximity and user count.
    
    Args:
        user_data: Dict mapping riot_id to list of rank history rows.
        max_users: Maximum number of users per group.
        max_rank_diff: Maximum rank difference (LP) within a group.
        
    Returns:
        List of user_data dicts, each representing a group.
    """
    if not user_data:
        return []

    # 1. Get latest rank for each user and sort
    user_latest = []
    for riot_id, rows in user_data.items():
        if not rows:
            continue
        # Assume rows are sorted by date or use the last one
        last_r = rows[-1]
        lp_total = get_total_lp(last_r['tier'], last_r['rank'], last_r['lp'])
        user_latest.append({
            'riot_id': riot_id,
            'lp': lp_total,
            'rows': rows
        })
    
    # Sort by LP descending (strongest first)
    user_latest.sort(key=lambda x: x['lp'], reverse=True)
    
    groups = []
    current_group = {}
    group_top_lp = None
    
    for user in user_latest:
        riot_id = user['riot_id']
        lp = user['lp']
        rows = user['rows']
        
        # Determine if we should start a new group
        should_split = False
        
        if len(current_group) >= max_users:
            should_split = True
        elif group_top_lp is not None and (group_top_lp - lp) > max_rank_diff:
            should_split = True
            
        if should_split and current_group:
            groups.append(current_group)
            current_group = {}
            group_top_lp = None
            
        if not current_group:
            group_top_lp = lp
            
        current_group[riot_id] = rows
        
    if current_group:
        groups.append(current_group)
        
    return groups
