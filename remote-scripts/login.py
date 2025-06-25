import asyncio
from twscrape import API
from twscrape.logger import set_log_level, logger
import json
import sys

# Set log level globally
set_log_level("DEBUG")


def load_twitter_accounts(batch_no_str):
    """Load Twitter accounts from the JSON file for the given batch."""
    logger.add(f"logs/login_{batch_no_str}.log", level="DEBUG")  # Save logs to a file

    try:
        with open(f"twitter_accounts_{batch_no_str}.json", "r") as f:
            twitter_accounts = json.load(f)
        return twitter_accounts
    except FileNotFoundError:
        logger.critical(f"Error: File 'twitter_accounts_{batch_no_str}.json' not found!")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.critical(f"Error: Failed to parse 'twitter_accounts_{batch_no_str}.json'.")
        sys.exit(1)


async def main(batch_no_str):
    """Main function to handle login process."""
    twitter_accounts = load_twitter_accounts(batch_no_str)
    total_accounts = len(twitter_accounts)

    api = API()
    logger.info(f"Starting login process for batch {batch_no_str} with {total_accounts} accounts.")

    # Add accounts to the pool
    for account in twitter_accounts:
        username = account["username"]
        password = account["password"]
        email = account["email"]
        await api.pool.add_account(username, password, email, password)
        logger.info(f"Added account: {username}")

    # Log in to all accounts
    logger.info(f"Attempting to log in all accounts for batch {batch_no_str}...")
    await api.pool.login_all()

    # Process and log login results
    accounts_info = await api.pool.accounts_info()
    logged_in_accounts = [acc["username"] for acc in accounts_info if acc["logged_in"]]
    success_count = len(logged_in_accounts)
    failed_count = total_accounts - success_count

    logger.success(f"Login results for batch {batch_no_str}:")
    logger.success(f"  - Total accounts: {total_accounts}")
    logger.success(f"  - Successfully logged in: {success_count}")
    logger.error(f"  - Failed logins: {failed_count}")

    # Log logged-in accounts only if there are any
    if success_count > 0:
        logger.info(f"  - Logged-in accounts: {', '.join(logged_in_accounts)}")

    logger.info(f"Login process completed for batch {batch_no_str}.")


if __name__ == "__main__":
    # Validate and parse batch number
    if len(sys.argv) != 2:
        logger.error("Usage: python login.py <batch_no>")
        sys.exit(1)

    batch_no = sys.argv[1]
    if not batch_no.isdigit():
        logger.error("<batch_no> must be an integer.")
        sys.exit(1)

    batch_no = int(batch_no)
    batch_no_str = str(batch_no).zfill(3)

    try:
        asyncio.run(main(batch_no_str))  # Pass batch_no_str to the main function
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
