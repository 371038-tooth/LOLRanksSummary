
"""
Compatibility layer for opgg.py library across version 2.x and 3.x.
"""
try:
    # Try v2 structure (used in older versions or specific setups)
    from opgg.v2.params import Region
    from opgg.v2.summoner import Summoner
    from opgg.v2.opgg import OPGG
    from opgg.v2.utils import Utils
    IS_V2 = True
except ImportError:
    # Try standard structure (v3+)
    try:
        from opgg.params import Region
        from opgg.summoner import Summoner
        from opgg.opgg import OPGG
        # Utils might be moved or removed in v3, we'll handle that in opgg_client
        try:
            from opgg.utils import Utils
        except ImportError:
            Utils = None
        IS_V2 = False
    except ImportError:
        # Fallback or error
        raise ImportError("Could not find opgg.py library. Please ensure it is installed.")

__all__ = ['Region', 'Summoner', 'OPGG', 'Utils', 'IS_V2']
