from datetime import datetime, date
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")

def get_now_jst() -> datetime:
    """Returns the current JST datetime as a naive datetime (for DB compatibility)."""
    return datetime.now(JST).replace(tzinfo=None)

def get_today_jst() -> date:
    """Returns the current JST date."""
    return datetime.now(JST).date()
