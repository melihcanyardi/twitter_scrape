import argparse
import os
import json
import pandas as pd
import subprocess
import logging
from datetime import datetime

# Logging setup
LOG_FILE = "logs/gather_data.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# Load configuration
CONFIG_PATH = "config/config.json"

def load_config():
    """Loads configuration from config.json."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file '{CONFIG_PATH}' not found.")
        exit(1)
    except json.JSONDecodeError:
        logging.error("Failed to parse the configuration file.")
        exit(1)

def load_server_details():
    """Loads Hetzner server details from the Excel file."""
    try:
        df = pd.read_excel("output/hetzner_servers.xlsx")
        return dict(zip(df["Name"], df["IP"]))
    except FileNotFoundError:
        logging.error("Hetzner servers file not found.")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading server details: {e}")
        exit(1)

def create_local_data_folder(descriptor):
    """Creates a timestamped folder inside 'data/' for storing collected data and logs."""
    today_date = datetime.today().strftime("%y%m%d")  # Format: YYMMDD
    folder_name = f"{today_date}_{descriptor}"
    local_path = os.path.join("data", folder_name)

    data_folder = os.path.join(local_path)  # Store tweets/user_infos/user_tweets here
    logs_folder = os.path.join(local_path, "logs")  # Store logs

    os.makedirs(data_folder, exist_ok=True)
    os.makedirs(logs_folder, exist_ok=True)

    logging.info(f"Created local data folder: {data_folder}")
    logging.info(f"Created local logs folder: {logs_folder}")
    
    return local_path, logs_folder  # Return the main folder and logs folder

def sync_data_from_server(ip_address, ssh_path, destination_path, local_data_path, logs_folder, batch_no, data_type):
    """Uses rsync to sync data from remote servers to the local data folder."""
    remote_path = f"root@{ip_address}:{destination_path}"
    
    # Define the remote folders and files to sync
    remote_data_folder = os.path.join(remote_path, data_type)  # e.g., tweets, user_infos, or user_tweets
    remote_log_file = os.path.join(remote_path, f"logs/{data_type}_{batch_no}.log")

    local_data_folder = os.path.join(local_data_path, data_type)
    local_log_file = os.path.join(logs_folder, f"{data_type}_{batch_no}.log")

    os.makedirs(local_data_folder, exist_ok=True)

    # Rsync command for data folder
    rsync_command = [
        "rsync", "-avz", "-e", f"ssh -i {ssh_path}",
        remote_data_folder + "/", local_data_folder + "/"
    ]

    # Rsync command for log file
    rsync_log_command = [
        "rsync", "-avz", "-e", f"ssh -i {ssh_path}",
        remote_log_file, local_log_file
    ]

    # Execute rsync for data folder
    logging.info(f"Syncing {data_type} data from {ip_address} (batch {batch_no})...")
    subprocess.run(rsync_command, check=True)
    
    # Execute rsync for log file
    logging.info(f"Syncing {data_type} log file from {ip_address}...")
    subprocess.run(rsync_log_command, check=True)

def main(data_type, descriptor):
    """Main function to sync data from remote servers."""
    config = load_config()
    servers = load_server_details()
    ssh_path = config["ssh_path"]
    destination_path = config["destination_path"]

    local_data_path, logs_folder = create_local_data_folder(descriptor)

    for server_name, ip_address in servers.items():
        batch_no = server_name.split("-")[-1]  # Extract batch number from server name
        try:
            sync_data_from_server(ip_address, ssh_path, destination_path, local_data_path, logs_folder, batch_no, data_type)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error syncing data from {ip_address}: {e}")

    logging.info(f"Data gathering completed. Files stored in: {local_data_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gather collected data from remote servers using rsync.")
    parser.add_argument("--data", required=True, choices=["tweets", "user_infos", "user_tweets"],
                        help="Type of data to retrieve (tweets, user_infos, user_tweets).")
    parser.add_argument("--desc", required=True, help="Short descriptor for the data folder.")

    args = parser.parse_args()
    main(args.data, args.desc)
