"""
consumer_webb.py

Consume json messages from a live data file and track keyword popularity by hour of day.
Insert the processed keyword counts into a database.

This consumer analyzes when different keywords are most popular throughout the day.
For each message, it extracts the hour from the timestamp and increments the count
for that keyword in that specific hour.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database stores: hour_of_day, keyword, count, last_updated
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sqlite3
import sys
import time
from datetime import datetime

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Database Functions
#####################################


def init_keyword_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    create the 'keyword_popularity' table if it doesn't exist.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling keyword init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS keyword_popularity;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS keyword_popularity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hour_of_day INTEGER,
                    keyword TEXT,
                    count INTEGER,
                    last_updated TEXT,
                    UNIQUE(hour_of_day, keyword)
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Keyword database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize keyword database at {db_path}: {e}")


def update_keyword_count(hour: int, keyword: str, db_path: pathlib.Path) -> None:
    """
    Update (or insert) keyword count for a specific hour.
    If the hour/keyword combination exists, increment the count.
    If not, create a new record with count=1.

    Args:
    - hour (int): Hour of day (0-23)
    - keyword (str): The keyword mentioned
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info(f"Updating keyword count: hour={hour}, keyword={keyword}")

    STR_PATH = str(db_path)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            
            # Try to update existing record
            cursor.execute(
                """
                UPDATE keyword_popularity 
                SET count = count + 1, last_updated = ?
                WHERE hour_of_day = ? AND keyword = ?
                """,
                (current_time, hour, keyword)
            )
            
            # If no rows were updated, insert new record
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO keyword_popularity (hour_of_day, keyword, count, last_updated)
                    VALUES (?, ?, 1, ?)
                    """,
                    (hour, keyword, current_time)
                )
                logger.info(f"Inserted new keyword record: {keyword} at hour {hour}")
            else:
                logger.info(f"Updated existing keyword record: {keyword} at hour {hour}")
            
            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to update keyword count: {e}")


#####################################
# Function to process a single message
#####################################


def process_message(message: dict) -> dict:
    """
    Process and analyze a single JSON message for keyword popularity.
    Extracts hour from timestamp and returns processed data.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    try:
        # Extract the data we need
        timestamp_str = message.get("timestamp")
        keyword = message.get("keyword_mentioned")
        
        if not timestamp_str or not keyword:
            logger.warning("Message missing timestamp or keyword, skipping")
            return None
            
        # Parse the timestamp and extract hour
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        hour_of_day = timestamp.hour
        
        processed_data = {
            "hour_of_day": hour_of_day,
            "keyword": keyword
        }
        
        logger.info(f"Processed message: keyword='{keyword}' at hour={hour_of_day}")
        return processed_data
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Consume Messages from Live Data File
#####################################


def consume_messages_from_file(live_data_path, sql_path, interval_secs, last_position):
    """
    Consume new messages from a file and process them for keyword analysis.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    - last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    logger.info("1. Initialize the keyword database.")
    init_keyword_db(sql_path)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                # Move to the last read position
                file.seek(last_position)
                for line in file:
                    # If we strip whitespace and there is content
                    if line.strip():

                        # Use json.loads to parse the stripped line
                        message = json.loads(line.strip())

                        # Call our process_message function
                        processed_data = process_message(message)

                        # If we have processed data, update keyword count
                        if processed_data:
                            update_keyword_count(
                                processed_data["hour_of_day"],
                                processed_data["keyword"],
                                sql_path
                            )

                # Update the last position that's been read to the current file position
                last_position = file.tell()

                # Return the last position to be used in the next iteration
                return last_position

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the keyword analysis consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Keyword Popularity Consumer to run continuously.")
    logger.info("This consumer tracks when different keywords are most popular.")

    logger.info("STEP 1. Read environment variables using config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        
        # Use a custom database name for keyword analysis
        base_path = config.get_base_data_path()
        keyword_db_path = base_path / "keyword_popularity.sqlite"
        
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if keyword_db_path.exists():
        try:
            keyword_db_path.unlink()
            logger.info("SUCCESS: Deleted previous keyword database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new keyword database with an empty table.")
    try:
        init_keyword_db(keyword_db_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create keyword db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and analyzing keyword popularity.")
    try:
        consume_messages_from_file(live_data_path, keyword_db_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Keyword Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Keyword Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()