import asyncio
import logging
import os
import json
from logging.handlers import RotatingFileHandler
from fetch_utils import fetch_match_ids, fetch_match_data
from puuid_management import get_user_puuid, log_used_puuid, repopulate_puuid_file

# Define the base directory
BASE_DIR = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data"

# Directory to store raw JSON files
SHARED_DIR = os.path.join(BASE_DIR, "shared_json_data")
os.makedirs(SHARED_DIR, exist_ok=True)

# Log file path
LOG_FILE = os.path.join(BASE_DIR, "fetch_and_save.log")

# Retry delay for 429 errors
RETRY_DELAY = 60  # Sleep for 60 seconds on 429 Too Many Requests

def setup_logging():
    """
    Configure logging for the program.
    """
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), file_handler]
    )

class RateLimiter:
    def __init__(self, request_limit, window_seconds):
        """
        Initialize the RateLimiter with:
        - request_limit: Total requests allowed in the window (e.g., 99).
        - window_seconds: Time window for the limit in seconds (e.g., 120 seconds).
        """
        self.request_limit = request_limit
        self.window_seconds = window_seconds
        self.request_times = []

    async def acquire(self):
        """
        Enforce the rate limit by tracking request timestamps.
        """
        now = asyncio.get_event_loop().time()

        # Remove timestamps outside the window
        self.request_times = [t for t in self.request_times if now - t < self.window_seconds]

        if len(self.request_times) >= self.request_limit:
            sleep_time = self.window_seconds - (now - self.request_times[0])
            logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
            await asyncio.sleep(sleep_time)

        self.request_times.append(now)

rate_limiter = RateLimiter(request_limit=99, window_seconds=120)

async def fetch_and_save_json(puuid):
    """
    Fetch match IDs and save match data to JSON files.
    """
    try:
        logging.info(f"Processing PUUID: {puuid}")

        # Fetch match IDs
        match_ids = await fetch_match_ids(puuid)
        logging.info(f"Fetched {len(match_ids)} match IDs for PUUID {puuid}")

        extracted_puuids = set()

        for match_id in match_ids:
            while True:  # Keep retrying the same match_id if 429 is encountered
                await rate_limiter.acquire()  # Enforce rate limit for each request
                try:
                    match_data = await fetch_match_data(match_id)

                    if match_data is None:
                        logging.error(f"Failed to fetch data for match ID {match_id}. Retrying...")
                        continue  # Retry the same match ID

                    # Save the match data to a JSON file
                    file_path = os.path.join(SHARED_DIR, f"{match_id}.json")
                    with open(file_path, "w") as json_file:
                        json.dump(match_data, json_file)
                    logging.info(f"Saved match data for {match_id} to {file_path}")

                    # Extract PUUIDs from the match data
                    for participant in match_data.get("info", {}).get("participants", []):
                        extracted_puuids.add(participant.get("puuid"))

                    break  # Exit retry loop for this match_id after success

                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        logging.warning(f"Received 429 Too Many Requests. Sleeping for {RETRY_DELAY} seconds...")
                        await asyncio.sleep(RETRY_DELAY)
                    else:
                        logging.error(f"Error fetching match data for {match_id}: {e}")
                        raise

        # Update the PUUID file
        extracted_puuids.discard(puuid)  # Avoid re-adding the current PUUID
        if extracted_puuids:
            repopulate_puuid_file(extracted_puuids)
            logging.info(f"Repopulated PUUID file with {len(extracted_puuids)} new PUUIDs.")

        # Log the used PUUID
        log_used_puuid(puuid)

    except Exception as e:
        logging.error(f"Error processing PUUID {puuid}: {e}")
        raise

async def shutdown(loop):
    """
    Gracefully shutdown the event loop.
    """
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]

    logging.info("Cancelling pending tasks...")
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("Closing event loop...")
    loop.stop()

def main():
    """
    Continuously fetch and process match data in a loop using a persistent event loop.
    """
    setup_logging()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            puuid = get_user_puuid()
            loop.run_until_complete(fetch_and_save_json(puuid))
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting gracefully...")
        loop.run_until_complete(shutdown(loop))
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
    finally:
        loop.close()
        logging.info("Event loop closed. Exiting program...")

if __name__ == "__main__":
    main()
