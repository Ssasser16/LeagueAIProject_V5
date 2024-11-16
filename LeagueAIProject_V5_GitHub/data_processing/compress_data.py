import os
import pandas as pd


def split_and_save_by_puuid(root_dir, output_dir):
    """
    Scan directories for Player_Data.csv, split data by puuid,
    and save each subset to a separate file.
    """
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            if file == 'Player_Data.csv':
                file_path = os.path.join(subdir, file)
                try:
                    # Load the CSV file
                    df = pd.read_csv(file_path)

                    # Check for required columns
                    required_columns = {'puuid', 'summonerName', 'matchId'}
                    missing_columns = required_columns - set(df.columns)
                    if missing_columns:
                        print(f"\nFile: {file_path}")
                        print(f"Missing columns: {', '.join(missing_columns)}")
                        user_input = input("Do you want to skip this file? (yes/no): ").strip().lower()
                        if user_input == 'yes':
                            print("Skipping file...")
                            continue
                        elif user_input == 'no':
                            print("Attempting to process the file anyway...")
                        else:
                            print("Invalid input. Skipping file by default...")
                            continue

                    # Group by puuid and process each group
                    grouped = df.groupby('puuid')
                    for puuid, group in grouped:
                        summoner_name = group['summonerName'].iloc[0]
                        match_id = group['matchId'].iloc[0]

                        # Create a unique and clean file name
                        file_name = f"{summoner_name}_{match_id}.csv".replace(" ", "_")
                        output_path = os.path.join(output_dir, file_name)

                        # Save the group data to the output path
                        group.to_csv(output_path, index=False)
                        print(f"Saved: {output_path}")

                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")


# Define input and output directories
input_root_dir = "C:/Users/ssass/PycharmProjects/LeagueAIProject_V4/match_id_and_data/processed_csv_data"
output_directory = "C:/Users/ssass/PycharmProjects/LeagueAIProject_V4/ai_data/match_data"

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Execute the function
split_and_save_by_puuid(input_root_dir, output_directory)
