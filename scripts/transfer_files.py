import json
import pandas as pd
import os
import sys
import glob
import argparse
import subprocess
import logging

# Logging setup
LOG_FILE = "logs/transfer_files.log"
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# Load configuration
CONFIG_PATH = "config/config.json"

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file '{CONFIG_PATH}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("Failed to parse the configuration file.")
        sys.exit(1)

def load_server_details():
    try:
        df = pd.read_excel("output/hetzner_servers.xlsx")
        return dict(zip(df["Name"], df["IP"]))
    except FileNotFoundError:
        logging.error("Server details file 'output/hetzner_servers.xlsx' not found.")
        sys.exit(1)

def run_scp(sources, destination, server_ip, ssh_path):
    if isinstance(sources, str):
        sources = [sources]
    scp_command = ["scp", "-i", ssh_path] + sources + [f"root@{server_ip}:{destination}"]
    try:
        subprocess.run(scp_command, check=True)
        logging.info(f"Transferred {sources} → {server_ip}:{destination}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to transfer {sources} → {server_ip}:{destination}. Error: {e}")

def transfer_files(batch_types):
    config = load_config()
    servers = load_server_details()

    ssh_path = config["ssh_path"]
    source_path = config["source_path"]
    destination_path = config["destination_path"]

    logging.info(f"Starting file transfer: batch={batch_types}")

    if "scripts" in batch_types:
        python_scripts = glob.glob(f"{source_path}remote-scripts/*.py")
        if python_scripts:
            for server_name, server_ip in servers.items():
                run_scp(python_scripts, destination_path, server_ip, ssh_path)
        else:
            logging.warning("No Python scripts found in remote-scripts.")

    if "accounts" in batch_types:
        for server_name, server_ip in servers.items():
            run_scp(f"{source_path}output/twitter_accounts/twitter_accounts_{server_name.split('-')[-1]}.json",
                    destination_path, server_ip, ssh_path)

    for server_name, server_ip in servers.items():
        server_idx = server_name.split("-")[-1]

        if "user" in batch_types or "all" in batch_types:
            run_scp(f"{source_path}output/user_batches/user_ids_{server_idx}.txt", destination_path, server_ip, ssh_path)

        if "tweet" in batch_types or "all" in batch_types:
            run_scp(f"{source_path}output/tweet_batches/tweet_ids_{server_idx}.txt", destination_path, server_ip, ssh_path)

        if "keyword" in batch_types or "all" in batch_types:
            run_scp(f"{source_path}output/keyword_batches/keyword_batch_{server_idx}.json",
                    f"{destination_path}keyword_batches/", server_ip, ssh_path)

    logging.info("File transfer process completed.")

# CLI parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer files from source to destination servers.")
    parser.add_argument(
        "--batch",
        nargs="+",
        choices=["scripts", "accounts", "user", "tweet", "keyword", "all"],
        required=True,
        help="Specify which types of files to transfer."
    )
    args = parser.parse_args()
    transfer_files(args.batch)
