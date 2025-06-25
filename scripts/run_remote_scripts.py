import argparse
import pandas as pd
import os
import json
import logging
from fabric import Connection
import sys

# Logging setup
LOG_FILE = "logs/run_remote_script.log"
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
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("Failed to parse the configuration file.")
        sys.exit(1)

def load_server_details():
    """Loads Hetzner server details from the Excel file."""
    try:
        df = pd.read_excel("output/hetzner_servers.xlsx")
        return dict(zip(df["Name"], df["IP"]))
    except FileNotFoundError:
        logging.error("Hetzner servers file not found.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading server details: {e}")
        sys.exit(1)

def run_remote_command(ip_address, ssh_path, command):
    """Runs a command on a remote server using SSH."""
    try:
        conn = Connection(
            host=ip_address,
            user="root",
            connect_kwargs={"key_filename": ssh_path}
        )
        conn.run(command)
        logging.info(f"Executed command on {ip_address}: {command}")
    except Exception as e:
        logging.error(f"Failed to execute command on {ip_address}: {e}")

def execute_script_on_server(ip_address, ssh_path, destination_path, script_name, batch_no):
    """Executes a Python script inside a screen session on a remote server."""
    screen_session = script_name.replace(".py", "")
    
    command = (
        f"screen -dmS {screen_session} bash -c 'cd {destination_path} && "
        f"python3 {script_name} {batch_no}; exec bash'"
    )
    
    run_remote_command(ip_address, ssh_path, command)

def main(script):
    """Main execution flow: Runs the selected script on all servers."""
    config = load_config()
    servers = load_server_details()
    ssh_path = config["ssh_path"]
    destination_path = config["destination_path"]

    for server_name, ip_address in servers.items():
        batch_no = server_name.split("-")[-1]  # Extract batch number from server name

        logging.info(f"Starting {script}.py on {server_name} ({ip_address}) with batch {batch_no}")
        execute_script_on_server(ip_address, ssh_path, destination_path, f"{script}.py", batch_no)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a remote Python script inside a screen session on all servers.")
    parser.add_argument("--script", required=True, choices=["login", "get_tweet_info", "get_user_info", "get_user_tweets"],
                        help="Specify which script to run (login, get_tweet_info, get_user_info, get_user_tweets)")

    args = parser.parse_args()
    
    main(args.script)
