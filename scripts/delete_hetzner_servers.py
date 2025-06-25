import json
import sys
import os
import logging
from hcloud import Client

# Logging setup
LOG_FILE = "logs/hetzner_server_delete_logs.log"
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


# Delete servers
def delete_servers(client, server_name):
    """Deletes all servers with the specified name prefix."""
    servers = client.servers.get_all()
    matching_servers = [server for server in servers if server.name.startswith(server_name)]

    if not matching_servers:
        logging.info(f"No servers found with prefix '{server_name}'")
        print(f"No servers found with prefix '{server_name}'")
        return

    print("\nThe following servers will be deleted:")
    for server in matching_servers:
        print(f" - {server.name} (ID: {server.id})")

    confirm = input("\nAre you sure you want to delete these servers? (yes/no): ").strip().lower()
    if confirm != "yes":
        logging.info("Server deletion aborted by user.")
        print("Aborting deletion.")
        return

    for server in matching_servers:
        try:
            server.delete()
            logging.info(f"Deleted server: {server.name} (ID: {server.id})")
            print(f"Deleted server: {server.name} (ID: {server.id})")
        except Exception as e:
            logging.error(f"Failed to delete server {server.name} (ID: {server.id}): {e}")
            print(f"Error: Could not delete {server.name} (ID: {server.id})")


# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python delete_hetzner_servers.py <server_name_prefix>")
        print("Usage: python delete_hetzner_servers.py <server_name_prefix>")
        sys.exit(1)

    server_name = sys.argv[1]

    # Load configuration
    config = load_config()

    # Retrieve API token directly from config
    api_token = config.get("hetzner_api_token")
    if not api_token:
        logging.error("Hetzner API token not found in config.json")
        sys.exit(1)

    # Authenticate client
    client = Client(token=api_token)

    # Delete servers
    delete_servers(client, server_name)
