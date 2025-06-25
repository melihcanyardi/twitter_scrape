import asyncio
from twscrape import API
from twscrape.logger import set_log_level, logger
import os
import sys

# Set log level globally
set_log_level("DEBUG")


def load_tweet_data(batch_no_str):
    """Load tweet data, check collected tweets, and prepare file paths."""
    logger.add(f"logs/tweets_{batch_no_str}.log", level="DEBUG")  # Save logs to a file

    # Create "tweets" folder if it doesn't exist
    tweets_folder = "tweets"
    os.makedirs(tweets_folder, exist_ok=True)

    # Load tweet IDs
    with open(f"tweet_ids_{batch_no_str}.txt", "r") as f:
        tweet_ids = f.read().strip().split("\n")

    tweet_ids = [int(tweet_id) for tweet_id in tweet_ids]

    # Check already collected tweet IDs
    def get_collected_tweet_ids(folder):
        """Retrieve a set of tweet IDs already collected."""
        collected_ids = set()
        for filename in os.listdir(folder):
            if filename.endswith(".json"):
                try:
                    collected_ids.add(int(filename.replace(".json", "")))
                except ValueError:
                    logger.warning(f"Invalid file name in tweets folder: {filename}")
        return collected_ids

    collected_tweet_ids = get_collected_tweet_ids(tweets_folder)

    # Filter out already collected tweet IDs
    remaining_tweet_ids = [tweet_id for tweet_id in tweet_ids if tweet_id not in collected_tweet_ids]

    if not remaining_tweet_ids:
        logger.info("No new tweets to fetch. Exiting.")
        sys.exit(0)

    logger.info(f"Total tweet IDs loaded: {len(tweet_ids)}")
    logger.info(f"Remaining tweet IDs to fetch: {len(remaining_tweet_ids)}")

    return tweets_folder, remaining_tweet_ids


async def main(batch_no_str):
    """Main function to fetch tweet details."""
    tweets_folder, remaining_tweet_ids = load_tweet_data(batch_no_str)
    api = API()

    # Fetch tweet details and save each tweet in a separate JSON file
    for idx, tweet_id in enumerate(remaining_tweet_ids, start=1):
        # Log progress every 1,000 tweets
        if idx % 1_000 == 0:
            logger.info(f"{idx} tweets processed so far...")

        try:
            tweet = await api.tweet_details(tweet_id)
            if tweet:
                file_path = os.path.join(tweets_folder, f"{tweet_id}.json")  # Create file path
                with open(file_path, "w") as f:
                    f.write(tweet.json())  # Write tweet to individual JSON file
        except Exception as e:
            logger.error(f"Error fetching tweet {tweet_id}: {e}")

    logger.info(f"Finished fetching tweet details. Files saved to the '{tweets_folder}' folder.")


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
