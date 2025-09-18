import os
import time
import datetime
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
YT_SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/youtubepartner",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly"
]

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SERVICE_ACCOUNT_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"
SHEET_ID = "YOUR SHEET ID"
SHEET_NAME = "Dataset"

# --- AUTHENTICATION ---
def authenticate_youtube_analytics():
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=YT_SCOPES
    )
    youtube_analytics = build("youtubeAnalytics", "v2", credentials=credentials)
    return youtube_analytics

def authenticate_gspread():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)

# --- INIT CLIENTS ---
youtube_analytics = authenticate_youtube_analytics()
sheets_client = authenticate_gspread()
print("✅ Authentication successful!")

# --- FETCH VIDEO IDS ---
def get_video_ids_from_sheet(sheet_id, sheet_name):
    try:
        sheet = sheets_client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_values()
        if not data:
            print("⚠️ Sheet is empty!")
            return [], []

        video_ids = [row[0] for row in data[1:] if len(row) >= 1]  # Column A
        publish_dates = [row[3] for row in data[1:] if len(row) >= 4]  # Column D
        return video_ids, publish_dates
    except Exception as e:
        print(f"❌ Error fetching video IDs: {e}")
        return [], []

video_ids, publish_dates = get_video_ids_from_sheet(SHEET_ID, SHEET_NAME)

# --- NORMALIZE DATES ---
def normalize_date_safe(date_str):
    try:
        if date_str and date_str.strip():
            return datetime.datetime.strptime(date_str.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        pass
    return "2024-01-01"

video_upload_dates = {vid: normalize_date_safe(date) for vid, date in zip(video_ids, publish_dates)}

# --- FETCH ANALYTICS WITH CONTENT TYPE ---
CONTENT_OWNER_IDS = ["lzAKySq5lKDX-HIMFrfyXg", "qkOVG_8qhUx5x25RaFqd6g", "XwgVpwvw3nd_A2maYITqPA"]

def get_cms_video_analytics(youtube_analytics, video_ids, content_owner_id, video_upload_dates):
    batch_size = 500
    analytics_data = []
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i+batch_size]
        batch_upload_dates = {vid: video_upload_dates.get(vid, "2024-01-01") for vid in batch}
        try:
            request_body = {
                "ids": f"contentOwner=={content_owner_id}",
                "startDate": min(batch_upload_dates.values()),
                "endDate": end_date,
                "metrics": "views,estimatedMinutesWatched,averageViewDuration,comments,likes,shares,estimatedRevenue,cpm,subscribersGained,subscribersLost",
                "dimensions": "video,creatorContentType",
                "filters": f"video=={','.join(batch)}",
                "maxResults": batch_size,
                "sort": "-views"
            }
            response = youtube_analytics.reports().query(**request_body).execute()
            if "rows" in response:
                analytics_data.extend(response["rows"])
            print(f"✅ Processed batch {i//batch_size + 1} ({len(batch)} videos)")
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error fetching analytics for batch {i//batch_size + 1}: {e}")
            time.sleep(5)
    return analytics_data

# Fetch analytics across all content owners
all_analytics_data = []
remaining_video_ids = video_ids.copy()

for owner_id in CONTENT_OWNER_IDS:
    if remaining_video_ids:
        analytics_data = get_cms_video_analytics(youtube_analytics, remaining_video_ids, owner_id, video_upload_dates)
        fetched_ids = {row[0] for row in analytics_data}
        remaining_video_ids = [vid for vid in remaining_video_ids if vid not in fetched_ids]
        all_analytics_data.extend(analytics_data)

# --- WRITE TO SHEET ---
def write_results_to_sheet(sheet_id, sheet_name, data, sheets_client):
    sheet = sheets_client.open_by_key(sheet_id).worksheet(sheet_name)
    metrics = [
        "Views", "Watch Time (Hours)", "Average View Duration", "Comments",
        "Likes", "Shares", "Estimated Revenue", "cpm", "subscribersGained", "subscribersLost"
    ]
    start_col = 16  # Column P

    # Ensure headers exist
    headers = sheet.row_values(1)
    if "Content Type" not in headers:
        sheet.update_acell("O1", "Content Type")
        time.sleep(1)
    for idx, metric in enumerate(metrics):
        col_letter = chr(64 + start_col + idx)
        if metric not in headers:
            sheet.update_acell(f"{col_letter}1", metric)
            time.sleep(1)

    all_video_ids = sheet.col_values(1)[1:]  # Column A
    video_id_to_row = {vid: i+2 for i, vid in enumerate(all_video_ids)}

    update_data = []
    for row in data:
        (
            video_id, content_type, views, watch_time, avd, comments, likes, shares,
            estimated_revenue, cpm, subscribersGained, subscribersLost
        ) = row
        if video_id in video_id_to_row:
            row_index = video_id_to_row[video_id]

            # Write content type to column O
            update_data.append({"range": f"O{row_index}", "values": [[content_type]]})

            # Write metrics starting from column P
            values = [
                views,
                round(watch_time / 60, 2),
                str(datetime.timedelta(seconds=int(avd))),
                comments, likes, shares,
                estimated_revenue, cpm,
                subscribersGained, subscribersLost
            ]
            update_data.append({"range": f"P{row_index}:Y{row_index}", "values": [values]})

    if update_data:
        sheet.batch_update(update_data)
        print(f"✅ Successfully updated {len(update_data)//2} rows in Google Sheets.")
    else:
        print("⚠️ No data to update.")

write_results_to_sheet(SHEET_ID, SHEET_NAME, all_analytics_data, sheets_client)
