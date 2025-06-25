import json
import pandas as pd
import sys
import os
import logging
from hcloud import Client
from hcloud.images import Image
from hcloud.server_types import ServerType
from hcloud.ssh_keys.client import SSHKey
from hcloud.locations.client import Location


# Logging setup
LOG_FILE = "logs/hetzner_server_logs.log"
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


# Create servers
def create_servers(client, num_servers, server_name, config):
    """Creates a specified number of servers on Hetzner."""
    created_servers = []

    for i in range(1, num_servers + 1):
        server_label = f"{server_name}-{i:03}"
        try:
            response = client.servers.create(
                name=server_label,
                server_type=ServerType(name=config["server_type"]),
                image=Image(id=config["image_id"]),
                ssh_keys=[SSHKey(name=config["ssh_key_name"])],
                labels={"role": "data-collection", "batch": f"batch-{i:03}"},
                location=Location(name=config["location"])
            )
            server = response.server
            logging.info(f"Created server: {server.name} (Status: {server.status})")
            created_servers.append(server.name)
        except Exception as e:
            logging.error(f"Error creating server {server_label}: {e}")

    return created_servers


# Fetch and save server details
def fetch_and_save_server_data(client, server_name, output_file):
    """Fetches all Hetzner servers matching the given prefix and saves details to an Excel file."""
    try:
        servers = client.servers.get_all()
        server_data = []

        for server in servers:
            if server.name.startswith(server_name):
                ip = server.public_net.ipv4.ip
                server_data.append({"Name": server.name, "IP": ip})

        df = pd.DataFrame(server_data)

        if not os.path.exists("output"):
            os.makedirs("output")

        df.to_excel(output_file, index=False)
        logging.info(f"Server details saved to {output_file}")

    except Exception as e:
        logging.error(f"Error fetching server data: {e}")


# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: python create_hetzner_servers.py <num_servers> <server_name>")
        sys.exit(1)

    num_servers = sys.argv[1]
    server_name = sys.argv[2]

    if not num_servers.isdigit():
        logging.error("<num_servers> must be an integer.")
        sys.exit(1)

    num_servers = int(num_servers)

    # Load configuration
    config = load_config()

    # Retrieve API token directly from config
    api_token = config.get("hetzner_api_token")
    if not api_token:
        logging.error("Hetzner API token not found in config.json")
        sys.exit(1)

    # Authenticate client
    client = Client(token=api_token)

    # Create servers
    created_servers = create_servers(client, num_servers, server_name, config)

    if created_servers:
        # Fetch and save server data
        fetch_and_save_server_data(client, server_name, "output/hetzner_servers.xlsx")
    else:
        logging.warning("No servers were created.")
