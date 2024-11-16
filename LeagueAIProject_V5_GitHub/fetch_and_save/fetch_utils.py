import aiohttp
import logging

API_BASE_URL = "https://americas.api.riotgames.com/lol/match/v5/matches"
API_KEY = "RGAPI-f4a16e23-06c0-422e-a96c-f6ad7e1a9cb5"  # Replace with your actual API key

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com"
}

async def fetch_match_ids(puuid, start=0, count=20):
    """
    Fetch match IDs for a given PUUID.
    """
    params = {
        "start": start,
        "count": count,
        "api_key": API_KEY
    }
    url = f"{API_BASE_URL}/by-puuid/{puuid}/ids"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.error(f"Error fetching match IDs: {response.status}")
                return []

async def fetch_match_data(match_id):
    """
    Fetch match data for a given match ID.
    """
    url = f"{API_BASE_URL}/{match_id}"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url, params={"api_key": API_KEY}) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.error(f"Error fetching match data for {match_id}: {response.status}")
                return None
