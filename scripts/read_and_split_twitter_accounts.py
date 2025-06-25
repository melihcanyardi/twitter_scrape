import pandas as pd
import gdown
import json
import sys
import math
import os
import logging

# Logging setup
LOG_FILE = "logs/twitter_accounts_processing_logs.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)


# Load configuration
CONFIG_PATH = "config/config.json"

def load_config():
    """Loads the configuration from a JSON file."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file '{CONFIG_PATH}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("Failed to parse the configuration file.")
        sys.exit(1)


# Download file from Google Drive
def download_file(file_id, file_path):
    """Downloads an Excel file from Google Drive."""
    url = f"https://drive.google.com/uc?id={file_id}"
    try:
        gdown.download(url, file_path, quiet=False)
        logging.info(f"File downloaded successfully.")
    except Exception as e:
        logging.error(f"Failed to download file from Google Drive: {e}")
        sys.exit(1)


# Read Excel file into a DataFrame
def read_excel_file(file_path):
    """Reads an Excel file and returns a Pandas DataFrame."""
    try:
        return pd.read_excel(file_path)
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading Excel file '{file_path}': {e}")
        sys.exit(1)


# Process Twitter accounts
def process_twitter_accounts(df, num_accounts):
    """Extracts Twitter account details from DataFrame."""
    twitter_accounts = []

    for i in range(min(num_accounts, len(df))):
        try:
            twitter_accounts.append({
                "name": df.at[i, "name"],
                "username": df.at[i, "username"].lstrip("@"),
                "email": df.at[i, "email"],
                "password": df.at[i, "password"],
                "birthday": str(df.at[i, "birthday"]),
                "gender": df.at[i, "gender"]
            })
        except KeyError as e:
            logging.error(f"Missing expected column in the Excel file: {e}")
            sys.exit(1)

    logging.info(f"Extracted {len(twitter_accounts)} Twitter accounts.")
    return twitter_accounts


# Split and save accounts to JSON files
def split_and_save_accounts(twitter_accounts, num_files, output_folder):
    """Splits accounts into multiple JSON files."""
    accounts_per_file = math.ceil(len(twitter_accounts) / num_files)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for i in range(num_files):
        start_index = i * accounts_per_file
        end_index = start_index + accounts_per_file
        accounts_slice = twitter_accounts[start_index:end_index]

        file_name = f"{output_folder}/twitter_accounts_{i+1:03}.json"
        with open(file_name, "w", encoding="utf-8") as json_file:
            json.dump(accounts_slice, json_file, indent=4, ensure_ascii=True)

        logging.info(f"Created file: {file_name} with {len(accounts_slice)} accounts")
        print(f"File {file_name} created with {len(accounts_slice)} accounts.")


# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: python read_and_split_twitter_accounts.py <num_accounts> <num_files>")
        print("Usage: python read_and_split_twitter_accounts.py <num_accounts> <num_files>")
        sys.exit(1)

    num_accounts = sys.argv[1]
    num_files = sys.argv[2]

    if not num_accounts.isdigit() or not num_files.isdigit():
        logging.error("Both <num_accounts> and <num_files> must be integers.")
        sys.exit(1)

    num_accounts = int(num_accounts)
    num_files = int(num_files)

    # Load configuration
    config = load_config()
    file_id = config["twitter_accounts_file_id"]
    
    # Specify output folder
    output_folder = "output/twitter_accounts"

    # Download and process accounts
    file_path = "output/twitter_accounts.xlsx"
    download_file(file_id, file_path)
    df = read_excel_file(file_path)
    twitter_accounts = process_twitter_accounts(df, num_accounts)

    if twitter_accounts:
        split_and_save_accounts(twitter_accounts, num_files, output_folder)
    else:
        logging.warning("No accounts found. Exiting.")
        print("No accounts found in the dataset.")
