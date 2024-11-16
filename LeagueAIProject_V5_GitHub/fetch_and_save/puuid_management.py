import os

PUUID_FILE = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/fetcher/fetcher/puuids.txt"
USED_PUUID_FILE = "C:/Users/ssass/PyCharmProjects/LeagueAIProject_V4/match_id_and_data/fetcher/fetcher/used_puuids.txt"

def get_user_puuid():
    """
    Get a PUUID to process from the PUUID file.
    """
    if not os.path.exists(PUUID_FILE):
        print(f"{PUUID_FILE} not found. Creating it now...")
        puuid = input("Please enter a PUUID to start the process: ").strip()
        with open(PUUID_FILE, "w") as file:
            file.write(puuid + "\n")
        return puuid

    with open(PUUID_FILE, "r") as file:
        lines = file.readlines()

    if not lines:
        raise ValueError(f"{PUUID_FILE} is empty. Add PUUIDs to the file.")

    puuid = lines[0].strip()

    # Remove the used PUUID
    with open(PUUID_FILE, "w") as file:
        file.writelines(lines[1:])

    return puuid

def log_used_puuid(puuid):
    """
    Log a PUUID as processed in the used_puuids.txt file.
    """
    with open(USED_PUUID_FILE, "a") as file:
        file.write(puuid + "\n")

def repopulate_puuid_file(puuids):
    """
    Overwrite the PUUID file with a new list of PUUIDs.
    """
    with open(PUUID_FILE, "w") as file:
        for puuid in puuids:
            file.write(puuid + "\n")
