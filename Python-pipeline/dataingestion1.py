import os
import pandas as pd
from datetime import datetime, timedelta
import googleapiclient.discovery
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURATION ---
YOUTUBE_API_KEY_FILE = r"D:/News-team-individual dashboards/YTauthentication/youtube_api_key.json"
GOOGLE_SHEET_CREDS_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"
CHANNELS_JSON_FILE = r"D:/News-team-individual dashboards/Documents/channel.json"
GOOGLE_SHEET_NAME = "Individual Dashboard Rimsha - News team"
WORKSHEET_NAME = "Dataset"

# --- AUTHENTICATION ---
def authenticate_google_sheets():
    """Authenticates with Google Sheets using a service account."""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEET_CREDS_FILE, scope)
        gc = gspread.authorize(creds)
        print("✅ Google Sheets authentication successful.")
        return gc
    except Exception as e:
        print(f"❌ Error authenticating with Google Sheets: {e}")
        return None

def authenticate_youtube():
    """Authenticates with the YouTube Data API using API key JSON."""
    try:
        with open(YOUTUBE_API_KEY_FILE, 'r') as f:
            api_key_data = json.load(f)
            api_key = api_key_data[0]['api_key']
        
        youtube = googleapiclient.discovery.build(
            "youtube", "v3", developerKey=api_key
        )
        print("✅ YouTube API authentication successful.")
        return youtube
    except Exception as e:
        print(f"❌ Error authenticating with YouTube API: {e}")
        return None

# --- HELPER FUNCTIONS ---
def load_channels(json_file=CHANNELS_JSON_FILE):
    """Load channels from JSON once and return as dictionary for fast lookup."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            channels = json.load(f)
        return {
            str(ch.get("Channel ID", "")).strip(): str(ch.get("Channel Name", "")).strip()
            for ch in channels if "Channel ID" in ch and "Channel Name" in ch
        }
    except Exception as e:
        print(f"❌ Error loading channel file: {e}")
        return {}

def get_videos_for_channel(youtube, channel_id, start_date, end_date):
    """Fetches videos for a given channel within a date range."""
    videos = []
    page_token = None
    
    start_date_iso = start_date.isoformat("T") + "Z"
    end_date_iso = end_date.isoformat("T") + "Z"

    while True:
        try:
            request = youtube.search().list(
                part="snippet",
                channelId=channel_id,
                publishedAfter=start_date_iso,
                publishedBefore=end_date_iso,
                type="video",
                order="date",
                maxResults=50,
                pageToken=page_token
            )
            response = request.execute()

            for item in response.get("items", []):
                video_id = item['id']['videoId']
                video_title = item['snippet']['title']
                published_at = item['snippet']['publishedAt']
                videos.append({
                    "Video ID": video_id,
                    "Video Title": video_title,
                    "Published Date": published_at,
                    "Channel ID": channel_id
                })

            page_token = response.get("nextPageToken")
            if not page_token:
                break
        except Exception as e:
            print(f"❌ Error fetching videos for channel {channel_id}: {e}")
            break
            
    return videos

# --- MAIN SCRIPT EXECUTION ---
def main():
    try:
        # 1. Load channels
        print(f"📂 Reading channel IDs from '{CHANNELS_JSON_FILE}'...")
        channels_dict = load_channels()
        channel_ids = list(channels_dict.keys())
        print(f"📌 Found {len(channel_ids)} channel IDs.")
        
        # 2. Authenticate
        gc = authenticate_google_sheets()
        youtube = authenticate_youtube()
        if not gc or not youtube:
            return

        # 3. Date range → exactly 4 days ago
        today = datetime.utcnow().date()
        end_date = datetime(today.year, today.month, today.day, 23, 59, 59) - timedelta(days=3)
        start_date = datetime(today.year, today.month, today.day, 0, 0, 0) - timedelta(days=4)

        print(f"📅 Fetching videos published between {start_date.isoformat()} and {end_date.isoformat()} UTC.")
        
        # 4. Fetch video data
        all_video_data = []
        for channel_id in channel_ids:
            print(f"🔍 Processing channel: {channel_id}...")
            channel_name = channels_dict.get(channel_id, "Unknown Channel")
            channel_videos = get_videos_for_channel(youtube, channel_id, start_date, end_date)
            
            for video in channel_videos:
                video['Channel Name'] = channel_name
                all_video_data.append(video)
        
        if not all_video_data:
            print("⚠️ No new videos found. Google Sheet not updated.")
            return

        # 5. DataFrame (only 4 columns: A–D)
        df_videos = pd.DataFrame(all_video_data)
        df_videos = df_videos[["Video ID", "Video Title", "Channel Name", "Published Date"]]

        print("\n📊 Video data collected:")
        print(df_videos.head())

        # 6. Update Google Sheet
        try:
            sh = gc.open(GOOGLE_SHEET_NAME)
            ws = sh.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            print(f"⚠️ Worksheet '{WORKSHEET_NAME}' not found. Creating new one.")
            ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=100, cols=10)
        except gspread.SpreadsheetNotFound:
            print(f"❌ Google Sheet '{GOOGLE_SHEET_NAME}' not found. Please create it first.")
            return

        # Read existing data
        existing_data = ws.get_all_records()
        if existing_data:
            df_existing = pd.DataFrame(existing_data)
        else:
            df_existing = pd.DataFrame(columns=df_videos.columns)

        # Merge + deduplicate
        df_combined = pd.concat([df_existing, df_videos], ignore_index=True)
        before_count = len(df_combined)
        df_cleaned = df_combined.drop_duplicates(subset=["Video ID"], keep="last")
        after_count = len(df_cleaned)
        removed_count = before_count - after_count

        # Clear old data, write cleaned back (only A–D columns)
        ws.clear()
        set_with_dataframe(ws, df_cleaned, include_column_header=True)

        print(f"✅ Google Sheet updated with {after_count} unique videos ({removed_count} duplicates removed).")
        
    except FileNotFoundError:
        print("❌ Error: Missing required file(s).")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")

if __name__ == "__main__":
    main()
