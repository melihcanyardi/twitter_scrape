import asyncio
from twscrape import API
from twscrape.logger import set_log_level, logger
import os
import sys

# Set log level globally
set_log_level("DEBUG")


def load_user_data(batch_no_str):
    """Load user data, check collected users, and prepare file paths."""
    logger.add(f"logs/user_infos_{batch_no_str}.log", level="DEBUG")  # Save logs to a file

    # Create "user_infos" folder if it doesn't exist
    user_infos_folder = "user_infos"
    os.makedirs(user_infos_folder, exist_ok=True)

    # Load user IDs
    with open(f"user_ids_{batch_no_str}.txt", "r") as f:
        user_ids = f.read().strip().split("\n")

    user_ids = [int(user_id) for user_id in user_ids]

    # Check already collected user IDs
    def get_collected_user_ids(folder):
        """Retrieve a set of user IDs already collected."""
        collected_ids = set()
        for filename in os.listdir(folder):
            if filename.endswith(".json"):
                try:
                    collected_ids.add(int(filename.replace(".json", "")))
                except ValueError:
                    logger.warning(f"Invalid file name in user infos folder: {filename}")
        return collected_ids

    collected_user_ids = get_collected_user_ids(user_infos_folder)

    # Filter out already collected user IDs
    remaining_user_ids = [user_id for user_id in user_ids if user_id not in collected_user_ids]

    if not remaining_user_ids:
        logger.info("No new users to fetch. Exiting.")
        sys.exit(0)

    logger.info(f"Total user IDs loaded: {len(user_ids)}")
    logger.info(f"Remaining user IDs to fetch: {len(remaining_user_ids)}")

    return user_infos_folder, remaining_user_ids


async def main(batch_no_str):
    """Main function to fetch user details."""
    user_infos_folder, remaining_user_ids = load_user_data(batch_no_str)
    api = API()

    # Fetch user info and save each user info in a separate JSON file
    for idx, user_id in enumerate(remaining_user_ids, start=1):
        # Log progress every 1,000 users
        if idx % 1_000 == 0:
            logger.info(f"{idx} users processed so far...")

        try:
            user_info = await api.user_by_id(user_id)
            if user_info:
                file_path = os.path.join(user_infos_folder, f"{user_id}.json")  # Create file path
                with open(file_path, "w") as f:
                    f.write(user_info.json())  # Write user info to individual JSON file
        except Exception as e:
            logger.error(f"Error fetching user {user_id}: {e}")

    logger.info(f"Finished fetching user details. Files saved to the '{user_infos_folder}' folder.")


if __name__ == "__main__":
    # Validate and parse batch number
    if len(sys.argv) != 2:
        logger.error("Usage: python script.py <batch_no>")
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
        logger.error(f"Unexpected error: {e}")
