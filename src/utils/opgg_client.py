from src.utils.opgg_compat import Summoner, Region, Utils, IS_V2, OPGG
import logging
import asyncio
import aiohttp
from datetime import datetime

logger = logging.getLogger(__name__)

class OPGGClient:
    def __init__(self):
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Origin": "https://www.op.gg",
            "Referer": "https://www.op.gg/"
        }
        self._session = None
        if not IS_V2:
            try:
                self.opgg_instance = OPGG()
                # v3 might have different attributes, we'll try to map them
                self._search_api_url = getattr(self.opgg_instance, "SEARCH_API_URL", None)
                self._summary_api_url = getattr(self.opgg_instance, "SUMMARY_API_URL", None)
            except Exception:
                self.opgg_instance = None
        else:
            self.opgg_instance = None
        self._bypass_api_url = "https://lol-api-summoner.op.gg/api"
        if not getattr(self, "_search_api_url", None):
            self._search_api_url = f"{self._bypass_api_url}/v3/{{region}}/summoners?riot_id={{summoner_name}}%23{{tagline}}"
        if not getattr(self, "_summary_api_url", None):
            self._summary_api_url = f"{self._bypass_api_url}/{{region}}/summoners/{{summoner_id}}/summary"

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create a shared aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session

    async def close(self):
        """Close the shared aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _prepare_opgg_params(self, url):
        return {
            "base_api_url": url,
            "headers": self._headers
        }

    async def get_summoner(self, name: str, tag: str, region: Region = Region.JP):
        """Fetch summoner info by name and tag (Async)."""
        query = f"{name}#{tag}"
        logger.info(f"Searching for summoner: {query} (Region: {region}, IS_V2: {IS_V2})")
        
        # v3 logic (if instance exists and is not v2)
        if not IS_V2 and self.opgg_instance:
            try:
                # Prefer search_async
                search_method = self.opgg_instance.search
                if hasattr(self.opgg_instance, 'search_async'):
                    search_method = self.opgg_instance.search_async
                    logger.info("Using search_async method")
                
                # Try Region object
                res = await search_method(query, region=region)
                
                if not res:
                    # Try region string
                    region_str = region.value.lower() if hasattr(region, 'value') else str(region).lower()
                    logger.info(f"v3 search returned nothing for {query} with {region}, trying with string '{region_str}'")
                    res = await search_method(query, region=region_str)
                
                if res and len(res) > 0:
                    logger.info(f"v3 search found {len(res)} results for {query}")
                    # In v3 SearchResult has .summoner
                    return res[0].summoner if hasattr(res[0], 'summoner') else res[0]
                else:
                    logger.info(f"v3 search returned no results for {query}")
            except Exception as e:
                logger.error(f"v3 search error for {query}: {e}")

        # v2 or Fallback logic
        region_str = region.value.lower() if hasattr(region, 'value') else str(region).lower()
        url_template = self._search_api_url.format(
            region=region_str,
            summoner_name=name,
            tagline=tag
        )
        params = self._prepare_opgg_params(url_template)
        logger.info(f"Using fallback search for {query} (URL: {url_template})")
        
        try:
            # 1. Try Utils if available
            if Utils and hasattr(Utils, '_single_region_search'):
                try:
                    params["base_api_url"] = self._search_api_url
                    results = await Utils._single_region_search(query, region, params)
                    if results:
                        logger.info(f"Fallback search (Utils) found {len(results)} results")
                        summoner_data = results[0]["summoner"]
                        return Summoner(summoner_data)
                except Exception as e:
                    logger.debug(f"Utils search failed, falling back to raw: {e}")

            # 2. Try raw aiohttp request (Last resort)
            logger.info(f"Trying raw aiohttp search for {query}")
            session = await self._get_session()
            url = url_template
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    results = data.get('data', [])
                    if results:
                        logger.info(f"Raw aiohttp search found {len(results)} results")
                        summoner_data = results[0]
                        return Summoner(summoner_data)
                else:
                    logger.error(f"Raw aiohttp search failed with status {response.status}")

        except Exception as e:
            logger.error(f"Fallback search error for {query}: {e}")
            
        return None

    async def get_rank_info(self, summoner: Summoner):
        """Fetch rank info for a summoner (Async)."""
        try:
            region_str = "jp"
            url = self._summary_api_url.format(
                region=region_str,
                summoner_id=summoner.summoner_id
            )
            
            profile_data = None
            # Direct aiohttp fetch
            logger.info(f"Fetching rank info via aiohttp: {url}")
            session = await self._get_session()
            async with session.get(url) as resp:
                logger.info(f"Rank info response status: {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    profile_data = data.get('data', {})
                    # Log ALL keys for debugging
                    logger.info(f"Profile data keys: {list(profile_data.keys()) if isinstance(profile_data, dict) else 'not a dict'}")
                    
                    # Log summoner sub-keys if present
                    if 'summoner' in profile_data:
                        summoner_data = profile_data['summoner']
                        logger.info(f"summoner sub-keys: {list(summoner_data.keys()) if isinstance(summoner_data, dict) else summoner_data}")
                        # Check for league_stats inside summoner
                        if 'league_stats' in summoner_data:
                            logger.info(f"Found league_stats inside summoner object!")

            if not profile_data:
                logger.warning(f"No profile_data found for summoner {summoner.summoner_id}")
                return "UNRANKED", "", 0, 0, 0
            
            # Try multiple locations for league_stats
            stats = profile_data.get('league_stats', [])
            if not stats and 'summoner' in profile_data:
                # Try inside summoner object
                summoner_obj = profile_data.get('summoner', {})
                stats = summoner_obj.get('league_stats', [])
                if stats:
                    logger.info(f"Found league_stats inside summoner: {len(stats)} entries")
                # Also check for solo_tier_info directly
                if not stats and 'solo_tier_info' in summoner_obj:
                    tier_info = summoner_obj['solo_tier_info']
                    logger.info(f"Found solo_tier_info directly: {tier_info}")
                    if tier_info:
                        tier = tier_info.get('tier', 'UNRANKED').upper()
                        division = tier_info.get('division') or tier_info.get('rank') or ""
                        lp = tier_info.get('lp', 0)
                        return tier, self.division_to_roman(division), lp, 0, 0
            
            logger.info(f"Found {len(stats)} league_stats entries")
            
            for i, stat in enumerate(stats):
                # Log full stat structure for debugging
                stat_keys = list(stat.keys()) if isinstance(stat, dict) else str(stat)
                logger.info(f"Stat {i} keys: {stat_keys}")
                
                # Try different ways to identify queue type
                queue_info = stat.get('queue_info', {})
                game_type = queue_info.get('game_type', '').upper()
                
                # Alternative: check for queue_type key directly
                if not game_type:
                    game_type = stat.get('queue_type', '').upper()
                # Alternative: check for tier_info.queue_type
                if not game_type:
                    tier_info = stat.get('tier_info', {})
                    if isinstance(tier_info, dict):
                        game_type = tier_info.get('queue_type', '').upper()
                
                logger.info(f"Stat {i}: game_type='{game_type}', tier_info={stat.get('tier_info')}")
                
                # Check for various Solo Queue identifiers or just take the first ranked one
                if game_type in ['SOLORANKED', 'RANKED_SOLO_5X5', 'SOLO']:
                    tier_info = stat.get('tier_info') or stat
                    logger.info(f"tier_info keys: {list(tier_info.keys()) if isinstance(tier_info, dict) else tier_info}")
                    
                    tier = tier_info.get('tier', 'UNRANKED').upper()
                    division = tier_info.get('division') or tier_info.get('rank') or ""
                    lp = tier_info.get('lp', 0)
                    # Try plural 'wins', 'losses' first, then 'win', 'lose'. Use None check for 0 handling.
                    wins = stat.get('wins') if stat.get('wins') is not None else stat.get('win', 0)
                    losses = stat.get('losses') if stat.get('losses') is not None else stat.get('lose', 0)
                    
                    logger.info(f"Extracted: tier={tier}, division={division}, lp={lp}, W={wins}, L={losses}")
                    return tier, self.division_to_roman(division), lp, wins, losses
            
            # If no specific queue type was matched, try to find any ranked data
            for i, stat in enumerate(stats):
                tier_info = stat.get('tier_info')
                if tier_info and isinstance(tier_info, dict):
                    tier = tier_info.get('tier', '').upper()
                    if tier and tier != 'UNRANKED':
                        division = tier_info.get('division') or tier_info.get('rank') or ""
                        lp = tier_info.get('lp', 0)
                        wins = stat.get('wins') if stat.get('wins') is not None else stat.get('win', 0)
                        losses = stat.get('losses') if stat.get('losses') is not None else stat.get('lose', 0)
                        logger.info(f"Found ranked data in stat {i}: {tier} {division} {lp}LP (W={wins}, L={losses})")
                        return tier, self.division_to_roman(division), lp, wins, losses
            
            logger.warning(f"No SOLORANKED stats found in league_stats")
            return "UNRANKED", "", 0, 0, 0
        except Exception as e:
            logger.error(f"Error fetching rank info: {e}", exc_info=True)
            return "UNRANKED", "", 0, 0, 0

    async def get_win_loss(self, summoner: Summoner):
        _, _, _, w, l = await self.get_rank_info(summoner)
        return w, l

    async def renew_summoner(self, summoner: Summoner):
        """Request OP.GG to renew/refresh summoner data (Async)."""
        try:
            region_str = "jp"
            # Verified endpoint via test script
            url = f"https://lol-api-summoner.op.gg/api/{region_str}/summoners/{summoner.summoner_id}/renewal"
            
            # Use specific referer for this summoner
            game_name = getattr(summoner, 'game_name', '')
            tagline = getattr(summoner, 'tagline', '')
            headers = self._headers.copy()
            headers["Referer"] = f"https://www.op.gg/summoners/jp/{game_name}-{tagline}"
            headers["Origin"] = "https://www.op.gg"

            if hasattr(summoner, 'renewable_at'):
                logger.info(f"Renewal check: renewable_at={summoner.renewable_at}, current_time={datetime.now()}")
            
            logger.info(f"Requesting data renewal for {game_name}#{tagline} (URL: {url})")
            session = await self._get_session()
            async with session.post(url, headers=headers) as resp:
                logger.info(f"Renewal request status: {resp.status}")
                if resp.status in [200, 201, 202, 204]:
                    try:
                        data = await resp.json()
                        # Handle response format like {'data': {'finish': False, 'delay': 1000, ...}}
                        resp_data = data.get('data', {})
                        msg = resp_data.get('message', 'Success')
                        finish = resp_data.get('finish')
                        delay = resp_data.get('delay')
                        logger.info(f"Renewal response: message='{msg}', finish={finish}, delay={delay}")
                    except Exception:
                        logger.info(f"Renewal request sent successfully (status: {resp.status})")
                    return True
                else:
                    logger.warning(f"Renewal request failed with status {resp.status}")
                    return False
        except Exception as e:
            logger.error(f"Error in renew_summoner: {e}")
            return False

    def division_to_roman(self, division):
        if not division:
            return ""
        if isinstance(division, int):
            mapping = {1: "I", 2: "II", 3: "III", 4: "IV"}
            return mapping.get(division, str(division))
        
        div_str = str(division).upper()
        if div_str in ["I", "II", "III", "IV"]:
            return div_str
        
        if div_str == "1": return "I"
        if div_str == "2": return "II"
        if div_str == "3": return "III"
        if div_str == "4": return "IV"
        return div_str

    async def get_tier_history(self, summoner_id: str, region: Region):
        region_str = region.value.lower() if hasattr(region, 'value') else str(region).lower()
        # Use lol-api-summoner.op.gg as it's more reliable than lol-web-api.op.gg
        url = f"https://lol-api-summoner.op.gg/api/{region_str}/summoners/{summoner_id}/tier-history"
        headers = self._headers
        try:
            logger.info(f"Fetching tier history via aiohttp: {url}")
            session = await self._get_session()
            async with session.get(url) as response:
                logger.info(f"Tier history response status: {response.status}")
                if response.status != 200:
                    logger.error(f"Failed to fetch tier history: HTTP {response.status}")
                    return []
                data = await response.json()
                history_list = data.get('data', [])
                results = []
                for entry in history_list:
                    updated_at_str = entry.get('created_at')
                    if not updated_at_str: continue
                    
                    # Try to find tier_info
                    tier_info = entry.get('tier_info')
                    if not tier_info:
                        # Maybe it's flat in entry?
                        tier_info = entry
                    
                    try:
                        updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
                    except Exception: continue
                    
                    tier = tier_info.get('tier', 'UNRANKED').upper()
                    # Some versions use 'division', others 'rank'
                    division = tier_info.get('division') or tier_info.get('rank') or ""
                    lp = tier_info.get('lp', 0)
                    
                    results.append({
                        'tier': tier,
                        'rank': self.division_to_roman(division),
                        'lp': lp,
                        'wins': 0,
                        'losses': 0,
                        'updated_at': updated_at
                    })
                return results
        except Exception as e:
            logger.error(f"Error in get_tier_history: {e}")
            return []

# Global instance
opgg_client = OPGGClient()
