import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import json
import string

# --- CONFIGURATION ---
YOUTUBE_API_KEY_FILE = r"D:/News-team-individual dashboards/YTauthentication/youtube_api_key.json"
GOOGLE_SHEET_CREDS_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"
SHOW_NAME_JSON_FILE = r"D:/News-team-individual dashboards/Documents/showname.json"

GOOGLE_SHEET_NAME = "YOUR SHEET NAME"
WORKSHEET_NAME = "Dataset"

# --- Google Sheets Authentication ---
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_SHEET_CREDS_FILE, scope
)

client = gspread.authorize(creds)

# Open Google Sheet
sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)

# Read existing data into a DataFrame
data = sheet.get_all_records()
df = pd.DataFrame(data)

# --- Load Show Name Mapping JSON ---
with open(SHOW_NAME_JSON_FILE, "r", encoding="utf-8") as f:
    show_data = json.load(f)

# Create dictionary for fast lookup by Code
code_map = {item["Code"]: item for item in show_data}

# --- Map Codes from Column G ---
if "Code" not in df.columns:
    raise ValueError("Column 'Code' not found in the sheet! Please check column G header.")

df["Show Name"] = df["Code"].map(lambda x: code_map.get(x, {}).get("Show_Name", ""))
df["Broadcaster"] = df["Code"].map(lambda x: code_map.get(x, {}).get("Broadcaster", ""))
df["Category"] = df["Code"].map(lambda x: code_map.get(x, {}).get("Category", ""))

# --- Figure out the last used column in the sheet ---
header = sheet.row_values(1)
last_col_index = len(header)  # count existing columns

# Convert index -> column letter
def colnum_to_letter(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

start_col_letter = colnum_to_letter(last_col_index + 1)  # next empty col
end_col_letter = colnum_to_letter(last_col_index + 3)   # 3 new cols

# --- Append new columns to sheet ---
rows_to_update = df[["Show Name", "Broadcaster", "Category"]].values.tolist()
sheet.update(
    f"{start_col_letter}1:{end_col_letter}{len(df)+1}",
    [["Show Name", "Broadcaster", "Category"]] + rows_to_update
)

print("✅ New columns added successfully without leaving blanks.")
