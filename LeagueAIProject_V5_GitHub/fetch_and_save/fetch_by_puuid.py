import os
import csv
import requests
import json
import time
import random

# Define the path to your directories
processed_csv_data_dir = 'C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/processed_csv_data/'

# Riot API key (replace with your actual API key)
API_KEY = 'RGAPI-f4a16e23-06c0-422e-a96c-f6ad7e1a9cb5'  # Replace with your actual API key

# List of regions for Riot's API
REGIONS = ['Americas', 'Europe', "Asia", "Esports"]

# Function to read PUUIDs from the Player_Data.csv file
def read_puuids_from_csv(csv_file):
    puuids = []
    try:
        with open(csv_file, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                puuid = row.get('puuid')
                if puuid:
                    puuids.append(puuid)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
    return puuids


# Fetch gameName and tagLine using Riot API from all regions
def fetch_game_info_from_riot(puuid):
    for region in REGIONS:
        url = f'https://{region}.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}'
        headers = {'X-Riot-Token': API_KEY}

        # Enforce a random delay between 5 and 10 seconds between requests
        delay = 10
        print(f"Enforcing a delay of {delay} seconds before sending request to avoid hitting rate limits.")
        time.sleep(delay)  # Random delay between 5 and 10 seconds

        print(f"Querying Riot API for PUUID {puuid} in region {region}...")  # Debugging output

        retries = 5
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    game_name = data.get('gameName')
                    tag_line = data.get('tagLine')
                    if game_name and tag_line:
                        return game_name, tag_line
                    else:
                        print(f"Missing gameName or tagLine for PUUID: {puuid}")
                        break
                elif response.status_code == 400:
                    print(f"Bad Request (400) for PUUID {puuid}. This may be due to an invalid PUUID or malformed request.")
                    break
                elif response.status_code == 429:  # Handle rate limiting
                    print(f"Rate limit exceeded (429). Retrying...")
                    time.sleep(60)  # Wait 1 minute and retry
                else:
                    print(f"Error fetching data for PUUID {puuid} in region {region}: {response.status_code}")
                    time.sleep(2 ** attempt + random.uniform(0, 1))  # Exponential backoff with jitter

            except Exception as e:
                print(f"Error connecting to Riot API for PUUID {puuid} in region {region}: {e}")
                time.sleep(2 ** attempt + random.uniform(0, 1))  # Exponential backoff with jitter

    return None, None  # Return None if not found in any region


# Function to save the gameName and tagLine in a JSON file
def save_game_info_to_json(match_id, puuid, game_name, tag_line):
    game_info = {
        'puuid': puuid,
        'gameName': game_name,
        'tagLine': tag_line
    }

    # Path to save the JSON file
    json_file_path = os.path.join(processed_csv_data_dir, match_id, f'{puuid}_game_info.json')

    try:
        with open(json_file_path, 'w') as json_file:
            json.dump(game_info, json_file, indent=4)
        print(f"Saved game info for PUUID {puuid} to {json_file_path}")
    except Exception as e:
        print(f"Error saving game info for PUUID {puuid} to {json_file_path}: {e}")


# Function to process each match directory and get game info for PUUIDs
def scan_and_process_directories():
    # Scan through all match ID directories, including subdirectories in processed_csv_data_dir
    for root, dirs, files in os.walk(processed_csv_data_dir):
        print(f"Scanning directory: {root}")
        print(f"Found files: {files}")

        # Skip directories missing required files
        if 'Player_Data.csv' not in files:
            print(f"Skipping {root} as Player_Data.csv is missing.")
            continue  # Skip to the next directory

        print(f"Found Player_Data.csv in {root}, processing...")

        match_id = os.path.basename(root)  # Extract match ID from directory path
        player_data_csv = os.path.join(root, 'Player_Data.csv')

        # Extract PUUIDs from the CSV file
        puuids = read_puuids_from_csv(player_data_csv)

        # For each PUUID, fetch game info and save it
        for puuid in puuids:
            game_name, tag_line = fetch_game_info_from_riot(puuid)
            if game_name and tag_line:
                save_game_info_to_json(match_id, puuid, game_name, tag_line)
            else:
                print(f"Failed to fetch game info for PUUID {puuid}")


# Run the main function
if __name__ == '__main__':
    scan_and_process_directories()
