import pandas as pd
import os
from googleapiclient.discovery import build
import isodate
from sqlalchemy import create_engine,text
### YouTube API credentials
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
api_service_name = "youtube"
api_version = "v3"

# Get credentials and create an API client
youtube = build(
    api_service_name, api_version, developerKey=YOUTUBE_API_KEY)
### Import channel IDs from CSV
# File path for CSV import
csv_file_path = '../channels/channel_ids.csv'

# Read channel IDs from CSV file into a list
import_channel_df = pd.read_csv(csv_file_path, header=None, names=["channel_id"])
import_channel_df
def get_channel_stats(youtube, channel_ids):
    all_data = []

    try:
        # Split channel_ids list into chunks of up to 50 ids each
        id_chunks = [channel_ids[i:i+50] for i in range(0, len(channel_ids), 50)]

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
                    'description': item['snippet']['description'],
                    'subscriber_count': item['statistics']['subscriberCount'],
                    'view_count': item['statistics']['viewCount'],
                    'video_count': item['statistics']['videoCount'],
                    'playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
                    'start_date': item['snippet']['publishedAt'],
                    'country': item['snippet'].get('country', None),
                }
                all_data.append(data)
    except Exception as e:
        print(f"Error occurred: {e}")

    return pd.DataFrame(all_data)
channels_df = get_channel_stats(youtube, import_channel_df['channel_id'])
channels_df
from datetime import datetime
channels_df['etl_date'] = datetime.today().strftime('%Y-%m-%d')
channels_df
