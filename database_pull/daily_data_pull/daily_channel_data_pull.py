import os
import pandas as pd
from googleapiclient.discovery import build
from sqlalchemy import create_engine
from datetime import datetime

def read_channel_ids(csv_file_path):
    """
    Read channel IDs from a CSV file into a pandas DataFrame.

    Args:
    - csv_file_path (str): Path to the CSV file containing channel IDs.

    Returns:
    - DataFrame: A DataFrame containing the channel IDs.
    """
    channel_df = pd.read_csv(csv_file_path, header=None, names=["channel_id"])
    return channel_df

def create_youtube_client(api_key):
    """
    Create and return a client for accessing the YouTube Data API.

    Args:
    - api_key (str): The API key for accessing the YouTube Data API.

    Returns:
    - Resource: A client for accessing the YouTube Data API.
    """
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

def get_channel_stats(youtube, channel_ids):
    """
    Retrieve statistics for YouTube channels.

    Args:
    - youtube (Resource): A client for accessing the YouTube Data API.
    - channel_ids (list): List of YouTube channel IDs.

    Returns:
    - DataFrame: A DataFrame containing statistics for the specified channels.
    """
    all_data = []

    try:
        # Split channel_ids list into chunks of up to 50 ids each
        id_chunks = [channel_ids[i:i + 50] for i in range(0, len(channel_ids), 50)]

        for id_chunk in id_chunks:
            request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(id_chunk)
            )
            response = request.execute()

            for item in response.get('items', []):
                data = {
                    'channel_name': item['snippet']['title'],
                    'channel_id': item['id'],
                    'subscriber_count': item['statistics']['subscriberCount'],
                    'view_count': item['statistics']['viewCount'],
                    'video_count': item['statistics']['videoCount'],
                }
                all_data.append(data)
    except Exception as e:
        print(f"Error occurred: {e}")

    all_data = pd.DataFrame(all_data)
    all_data['etl_date'] = datetime.today().strftime('%Y-%m-%d')

    return all_data

def save_to_database(dataframe, database_path):
    """
    Save DataFrame to a SQLite database.

    Args:
    - dataframe (DataFrame): The DataFrame to be saved.
    - database_path (str): Path to the SQLite database.

    Returns:
    - None
    """
    # Create engine
    engine = create_engine(f'sqlite:///{database_path}', echo=True)

    # Create connection
    conn = engine.connect()

    # Push DataFrame to database
    dataframe.to_sql(name="channel_stats_daily", con=engine, if_exists='append', index=False)

    # Close the connection
    conn.close()
    engine.dispose()

def main():
    """
    Main function to orchestrate the process of fetching channel statistics and saving them to a database.
    """
    # File paths and API key
    script_directory = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the script
    csv_file_path = os.path.join(script_directory, '..', 'channels', 'channel_ids.csv')
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    database_path = os.path.join(script_directory, '..', '..', 'db', 'youtube.db')

    # Read channel IDs from CSV
    channel_df = read_channel_ids(csv_file_path)

    # Create YouTube Data API client
    youtube_client = create_youtube_client(YOUTUBE_API_KEY)

    # Get channel statistics
    channels_df = get_channel_stats(youtube_client, channel_df['channel_id'])

    # Save data to database
    save_to_database(channels_df, database_path)

if __name__ == "__main__":
    main()