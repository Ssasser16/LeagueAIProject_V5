import os
import json
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

# Directories and files
RAW_JSON_DIR = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/shared_json_data"
PROCESSED_CSV_DIR = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/processed_csv_data"
LOG_FILE = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/logs/processed_ids.log"

# Create directories if they don't exist
os.makedirs(PROCESSED_CSV_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Logging setup
def setup_logging():
    """
    Set up logging with rotating log files.
    """
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Log to console
            file_handler              # Log to file
        ]
    )

setup_logging()

def read_processed_ids():
    """
    Read processed match IDs from the log file.
    """
    if not os.path.exists(LOG_FILE):
        return set()

    with open(LOG_FILE, "r") as log:
        return set(line.strip() for line in log.readlines())

def log_processed_id(match_id):
    """
    Log a processed match ID to the log file.
    """
    with open(LOG_FILE, "a") as log:
        log.write(match_id + "\n")

def preprocess_value(value):
    """
    Handle unsupported JSON types by converting them to strings or handling None values.
    """
    if isinstance(value, (dict, list)):
        return json.dumps(value)  # Convert to JSON string
    elif value is None:
        return None
    else:
        return value

def process_match_json(file_path):
    """
    Convert a single JSON file into Player_Data, Match_Data, and Misc_Data CSV files.
    """
    try:
        with open(file_path, "r") as file:
            match_data = json.load(file)  # Will throw error if JSON is malformed

        match_id = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join(PROCESSED_CSV_DIR, f"{match_id}_data")
        os.makedirs(output_dir, exist_ok=True)

        match_info = match_data.get("info", {})
        match_metadata = match_data.get("metadata", {})

        # Player_Data CSV
        player_data = []
        for participant in match_info.get("participants", []):
            player_row = {key: preprocess_value(value) for key, value in participant.items()}
            player_row["matchId"] = match_id
            player_data.append(player_row)
        pd.DataFrame(player_data).to_csv(os.path.join(output_dir, "Player_Data.csv"), index=False)

        # Match_Data CSV
        match_row = {
            "matchId": match_id,
            "gameId": match_info.get("gameId"),
            "gameDuration": match_info.get("gameDuration"),
            "gameMode": match_info.get("gameMode"),
            "gameType": match_info.get("gameType"),
            "gameVersion": match_info.get("gameVersion"),
            "mapId": match_info.get("mapId"),
        }
        pd.DataFrame([match_row]).to_csv(os.path.join(output_dir, "Match_Data.csv"), index=False)

        # Misc_Data CSV
        misc_row = {
            "matchId": match_id,
            "dataVersion": match_metadata.get("dataVersion"),
            "participants": json.dumps(match_metadata.get("participants", [])),
        }
        pd.DataFrame([misc_row]).to_csv(os.path.join(output_dir, "Misc_Data.csv"), index=False)

        logging.info(f"Processed and saved CSV files for match {match_id}")

        # Add a 10-second delay before deleting the processed JSON file
        time.sleep(10)
        os.remove(file_path)
        logging.info(f"Deleted processed file: {file_path}")

    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        # In case of an error, move the file to an 'error' folder for further inspection
        error_dir = os.path.join(os.path.dirname(file_path), "error_files")
        os.makedirs(error_dir, exist_ok=True)
        os.rename(file_path, os.path.join(error_dir, os.path.basename(file_path)))
        logging.error(f"Moved invalid file {file_path} to error folder.")

class NewFileHandler(FileSystemEventHandler):
    """
    Watches for new files and processes them.
    """
    def __init__(self, processed_ids):
        self.processed_ids = processed_ids

    def on_created(self, event):
        """
        Handles new files added to the directory.
        """
        if event.is_directory or not event.src_path.endswith(".json"):
            return

        file_path = event.src_path
        match_id = os.path.splitext(os.path.basename(file_path))[0]

        logging.info(f"New file detected: {file_path}")
        process_match_json(file_path)
        log_processed_id(match_id)
        self.processed_ids.add(match_id)

class ExistingFileHandler(FileSystemEventHandler):
    """
    Handles already existing files and processes them without deleting them again.
    """
    def __init__(self, processed_ids):
        self.processed_ids = processed_ids

    def on_created(self, event):
        """
        Handles already existing files (from the initial directory scan).
        """
        if event.is_directory or not event.src_path.endswith(".json"):
            return

        file_path = event.src_path
        match_id = os.path.splitext(os.path.basename(file_path))[0]

        if match_id not in self.processed_ids:
            logging.info(f"Processing existing file: {file_path}")
            process_match_json(file_path)
            log_processed_id(match_id)
            self.processed_ids.add(match_id)

def process_existing_files(processed_ids):
    """
    Processes all existing JSON files in the directory.
    """
    logging.info("Processing existing JSON files...")
    for file_name in os.listdir(RAW_JSON_DIR):
        if not file_name.endswith(".json"):
            continue

        file_path = os.path.join(RAW_JSON_DIR, file_name)
        match_id = os.path.splitext(file_name)[0]

        # Process the file if it's not already processed
        logging.info(f"Processing existing file: {file_path}")
        process_match_json(file_path)
        log_processed_id(match_id)
        processed_ids.add(match_id)

    logging.info("Finished processing existing files.")

def monitor_directory():
    """
    Processes existing files and then monitors the directory for new JSON files.
    """
    processed_ids = read_processed_ids()
    process_existing_files(processed_ids)

    event_handler = NewFileHandler(processed_ids)
    observer = Observer()
    observer.schedule(event_handler, RAW_JSON_DIR, recursive=False)

    observer.start()
    logging.info(f"Monitoring directory: {RAW_JSON_DIR}")
    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        logging.info("Shutting down directory monitoring...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    monitor_directory()
