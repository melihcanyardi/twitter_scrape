# Twitter Data Collection with `twscrape`
This repository implements a distributed data collection system for large-scale Twitter scraping using the `twscrape` library and Hetzner cloud servers. The pipeline is designed to collect tweet content, user profiles, and user tweets efficiently using multiple remote servers in parallel. Data is collected from Twitter by rotating multiple account logins and handling rate limits automatically.

## Key Features:
- Distributed scraping via Hetzner servers
- Automated account-based login using `twscrape`
- Parallel collection of:
  - Tweet content (`tweets`)
  - User info (`user_infos`)
  - User tweets and replies (`user_tweets`)
- Structured logging for progress tracking
- rsync-based retrieval of collected data back to the source server

## Folder Structure:
- `config/`: Configuration files (e.g., config.json)
- `data/`: Final collected data batches (e.g., 250101_mockData)
- `logs/`: Log files created by source-level operations
- `output/`: Output folders and batches:
  - `tweet_batches/`: Files like tweet_ids_001.txt
  - `user_batches/`: Files like user_ids_001.txt
  - `keyword_batches/`: Files like keyword_batch_001.json
  - `twitter_accounts/`: Account batch files like twitter_accounts_001.json
- `remote-scripts/`: Scripts executed on remote Hetzner servers
- `scripts/`: Scripts for local orchestration (e.g., transfer, gathering, provisioning)

## Main Scripts:
- `create_hetzner_servers.py`: Creates remote servers on Hetzner
- `delete_hetzner_servers.py`: Deletes remote servers
- `read_and_split_twitter_accounts.py`: Splits raw account Excel into batches
- `transfer_files.py`: Sends tweet/user/keyword batches, accounts and scripts to all servers
- `run_remote_scripts.py`: Runs remote scripts like get_tweet_info.py or login.py
- `gather_data.py`: Collects scraped data and logs back to the source server

## Example Usage:

- Transfer tweet ID batches and Python scripts to servers:
```bash
python scripts/transfer_files.py --batch tweet
```
- Run the tweet collection script on all remote servers:
```bash
python scripts/run_remote_scripts.py --script get_tweet_info
```
- Gather the collected tweets and logs back to the source:
```bash
python scripts/gather_data.py --data tweets --description authorID
```

## Requirements:
- Python 3.9+
- `twscrape`
- `pandas`
- `fabric`
- `openpyxl`
- `rsync` installed on both local and remote systems

## Configuration:
The configuration is managed in `config/config.json`:
```json
{
    "twitter_accounts_file_id": "",
    "hetzner_api_token": "",
    "server_type": "",
    "image_id": "",
    "ssh_key_name": "",
    "location": "",
    "ssh_path": "",
    "source_path": "",
    "destination_path": ""
}
```
