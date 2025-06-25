import asyncio
import os
import sys
from twscrape import API
from twscrape.logger import set_log_level, logger

# Set log level globally
set_log_level("DEBUG")

def load_user_data(batch_no_str):
    """Load user data, check collected users, and prepare file paths."""
    logger.add(f"logs/user_tweets_{batch_no_str}.log", level="DEBUG")

    # Create "user_tweets" folder if it doesn't exist
    user_tweets_folder = "user_tweets"
    os.makedirs(user_tweets_folder, exist_ok=True)

    # Load user IDs
    with open(f"user_ids_{batch_no_str}.txt", "r") as f:
        user_ids = f.read().strip().split("\n")

    user_ids = [int(user_id) for user_id in user_ids]

    # Check already collected user IDs
    def get_collected_user_ids(folder):
        """Retrieve a set of user IDs already collected."""
        collected_ids = set()
        for filename in os.listdir(folder):
            if filename.endswith(".jsonl"):
                try:
                    collected_ids.add(int(filename.replace(".jsonl", "")))
                except ValueError:
                    logger.warning(f"Invalid file name in user tweets folder: {filename}")
        return collected_ids

    collected_user_ids = get_collected_user_ids(user_tweets_folder)

    # Filter out already collected user IDs
    remaining_user_ids = [user_id for user_id in user_ids if user_id not in collected_user_ids]

    if not remaining_user_ids:
        logger.info("No new users to fetch. Exiting.")
        sys.exit(0)

    logger.info(f"Total user IDs loaded: {len(user_ids)}")
    logger.info(f"Remaining user IDs to fetch: {len(remaining_user_ids)}")

    return user_tweets_folder, remaining_user_ids

async def main(batch_no_str):
    """Main function to fetch user tweets."""
    user_tweets_folder, remaining_user_ids = load_user_data(batch_no_str)
    api = API()

    # **Check if accounts are available before fetching tweets**
    test_account = await api.pool.get_for_queue_or_wait("UserTweetsAndReplies")
    if test_account is None:
        logger.error("No active accounts available. Exiting.")
        sys.exit(1)  # Stop the script immediately

    # Fetch user tweets and save tweets to JSONL file
    for idx, user_id in enumerate(remaining_user_ids, start=1):
        # Log progress every 1,000 users
        if idx % 1_000 == 0:
            logger.info(f"{idx} users processed so far...")

        try:
            file_path = os.path.join(user_tweets_folder, f"{user_id}.jsonl")
            
            # Check for account exhaustion before making API calls
            current_account = await api.pool.get_for_queue_or_wait("UserTweetsAndReplies")
            if current_account is None:
                logger.error("All accounts are exhausted. Stopping further requests.")
                break  # Stop processing further user IDs

            # Fetch tweets
            tweets = []
            async for tweet in api.user_tweets_and_replies(user_id, limit=3200):
                tweets.append(tweet.json())

            if tweets:
                with open(file_path, "w") as f:
                    f.write("\n".join(tweets))
            else:
                # If request was valid but returned no tweets, create empty JSONL file
                open(file_path, "w").close()
                logger.info(f"User {user_id} has no tweets. Created an empty JSONL file.")

        except Exception as e:
            logger.error(f"Error fetching tweets for user {user_id}: {e}")

    logger.info(f"Finished fetching user tweets. Files saved to the '{user_tweets_folder}' folder.")

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
