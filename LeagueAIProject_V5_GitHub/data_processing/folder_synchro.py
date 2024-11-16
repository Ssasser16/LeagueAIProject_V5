import os
import pandas as pd
import json


def parse_challenges_column(df):
    """Optimized parsing of JSON-like 'challenges' column with duplicate column handling."""
    if 'challenges' in df.columns:
        try:
            # Validate JSON strings
            valid_json_rows = df['challenges'].apply(lambda x: isinstance(x, str) and x.strip().startswith('{'))

            # Parse valid JSON rows using pd.json_normalize
            parsed_data = pd.json_normalize(df.loc[valid_json_rows, 'challenges'].apply(json.loads))

            # Rename parsed columns to avoid conflicts
            parsed_data.columns = [f"challenges_{col}" for col in parsed_data.columns]

            # Reset indices for safe concatenation
            df = df.reset_index(drop=True)
            parsed_data = parsed_data.reset_index(drop=True)

            # Concatenate DataFrame while avoiding duplicate columns
            df = pd.concat([df, parsed_data], axis=1)
        except Exception as e:
            print(f"Error parsing challenges column: {e}")
    else:
        print("Challenges column is missing.")
    return df


def feature_engineering(df):
    """Create derived metrics to enhance the dataset."""
    try:
        # Handle missing 'teamKills' and 'teamObjectives' with defaults
        df['teamKills'] = df.get('teamKills', df['kills'].sum() if 'kills' in df.columns else 1)
        df['teamObjectives'] = df.get('teamObjectives', (
                df.get('turretTakedowns', 0) + df.get('baronKills', 0) + df.get('dragonKills', 0)
        ))

        # Derived metrics with safeguards for numeric operations
        df['KillParticipation'] = (df['kills'] + df['assists']) / (
            df['teamKills'].replace({0: 1}) if 'teamKills' in df.columns else 1)
        df['ObjectiveEfficiency'] = (
                (df.get('turretTakedowns', 0) + df.get('baronKills', 0) + df.get('dragonKills', 0))
                / (df['teamObjectives'].replace({0: 1}) if 'teamObjectives' in df.columns else 1)
        )
        df['GoldEfficiency'] = df.get('goldEarned', 0) / (
            df.get('gameDuration', 1).replace({0: 1}) if 'gameDuration' in df.columns else 1)

    except KeyError as e:
        print(f"Missing columns for feature engineering: {e}")
    except Exception as e:
        print(f"Unexpected error during feature engineering: {e}")
    return df


def process_single_file(file_path):
    """Process a single match file, parse challenges, and add features."""
    try:
        # Load the file
        df = pd.read_csv(file_path)

        # Parse challenges and add derived metrics
        df = parse_challenges_column(df)
        df = feature_engineering(df)

        return df
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def streamline_for_ai(df, target_column='win', drop_na_threshold=0.7):
    """
    Streamline dataset for AI training:
    - Retain relevant features and the target column.
    - Handle missing values and standardize numeric columns.
    """
    # Drop columns with too many missing values
    missing_threshold = int(len(df) * drop_na_threshold)
    df = df.dropna(thresh=missing_threshold, axis=1)

    # Retain numeric and boolean columns for features
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    boolean_cols = df.select_dtypes(include=['bool']).columns

    # Target column
    target_col = [target_column] if target_column in df.columns else []

    # Combine features and target columns
    selected_columns = list(numeric_cols) + list(boolean_cols) + target_col
    df = df[selected_columns]

    # Handle missing values (e.g., fill with 0 for numeric columns)
    df = df.fillna(0)

    # Standardize numeric columns
    numeric_features = df.select_dtypes(include=['int64', 'float64']).columns
    df[numeric_features] = (df[numeric_features] - df[numeric_features].mean()) / df[numeric_features].std()

    # Ensure target column is the last column
    if target_column in df.columns:
        target = df[target_column]  # Extract target as a Series
        df = df.drop(columns=[target_column])  # Drop the target column
        df[target_column] = target  # Add the target column back at the end

    return df


def aggregate_player_data(input_dir, output_aggregated_file, output_ai_file):
    """
    Parse all player data files in a directory, expand challenges,
    add derived metrics, and combine into a single dataset without duplicates.
    """
    combined_df = pd.DataFrame()

    for file in os.listdir(input_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(input_dir, file)
            try:
                # Process each file
                df = process_single_file(file_path)
                if df is not None:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    print(f"Processed file: {file_path}")
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    # Remove duplicates based on unique identifiers (e.g., puuid, matchId)
    if {'puuid', 'matchId'}.issubset(combined_df.columns):
        combined_df = combined_df.drop_duplicates(subset=['puuid', 'matchId'])
        print("Duplicates removed based on 'puuid' and 'matchId'.")

    # Save the aggregated dataset
    combined_df.to_csv(output_aggregated_file, index=False)
    print(f"Aggregated dataset saved to {output_aggregated_file}")

    # Streamline for AI
    ai_ready_data = streamline_for_ai(combined_df, target_column='win')
    ai_ready_data.to_csv(output_ai_file, index=False)
    print(f"AI-ready dataset saved to {output_ai_file}")


# Define input and output paths
input_directory = "C:/Users/ssass/PycharmProjects/LeagueAIProject_V4/ai_data/match_data"
output_aggregated_file = "C:/Users/ssass/PycharmProjects/LeagueAIProject_V4/ai_data/aggregated_player_data.csv"
output_ai_file = "C:/Users/ssass/PycharmProjects/LeagueAIProject_V4/ai_data/ai_ready_dataset.csv"

# Ensure output directory exists
os.makedirs(os.path.dirname(output_aggregated_file), exist_ok=True)

# Execute the aggregation and AI streamlining
aggregate_player_data(input_directory, output_aggregated_file, output_ai_file)
