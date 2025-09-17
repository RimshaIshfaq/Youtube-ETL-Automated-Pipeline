import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURATION ---
YOUTUBE_API_KEY_FILE = r"D:/News-team-individual dashboards/YTauthentication/youtube_api_key.json"
GOOGLE_SHEET_CREDS_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"
CHANNELS_JSON_FILE = r"D:/News-team-individual dashboards/Documents/channel.json"
EMPLOYEE_JSON_FILE = r"D:/News-team-individual dashboards/Documents/resource_name.json"  # <-- Add your JSON file path
GOOGLE_SHEET_NAME = "Individual Dashboard Rimsha - News team"
WORKSHEET_NAME = "Dataset"

# --- Google Sheets Authentication ---
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_SHEET_CREDS_FILE, scope
)
client = gspread.authorize(creds)

# --- Open Google Sheet by name & worksheet ---
spreadsheet = client.open(GOOGLE_SHEET_NAME)
sheet = spreadsheet.worksheet(WORKSHEET_NAME)

# --- Load existing sheet into DataFrame ---
df = get_as_dataframe(sheet, evaluate_formulas=False, header=0)
df = df.dropna(how="all")

# --- Extract last word from Column B (titles) ---
def extract_last_word(title):
    if pd.isna(title):
        return ""
    parts = str(title).replace("|", " ").split()
    return parts[-1].strip() if parts else ""

# --- Create new columns ---
df["main code"] = df.iloc[:, 1].apply(extract_last_word)   # Column B = index 1
df["Len"] = df["main code"].apply(len)


# --- Keep only words of length 3, 4, or 5 ---
df.loc[~df["Len"].isin([3, 4, 5]), "main code"] = ""

# --- Remove words that are purely digits (e.g., 2025, 123) ---
df.loc[df["main code"].str.isdigit(), "main code"] = ""

# --- Remove words with more than one lowercase letter ---
df.loc[df["main code"].apply(lambda x: sum(1 for c in x if c.islower()) > 1), "main code"] = ""

# --- Recalculate Len after cleaning ---
df["Len"] = df["main code"].apply(len)


# Column: Code
df["Code"] = df.apply(
    lambda x: x["main code"][:2] if x["Len"] == 4 else x["main code"][:3],
    axis=1
)

# Column: resource code (last character of Code)
df["resource code"] = df["main code"].apply(lambda x: x[-1] if x else "")

# --- Load employee mapping JSON ---
with open(EMPLOYEE_JSON_FILE, "r", encoding="utf-8") as f:
    employee_data = json.load(f)

# Convert JSON to dictionary for quick lookup
employee_map = {emp["Employee Code"]: emp["Team"] for emp in employee_data}

# --- Map resource code (H) to employee name (I) ---
df["resource name"] = df["resource code"].map(employee_map).fillna("")

# --- Update sheet with new columns (without clearing old data) ---
# Write starting at column E (5th column)
set_with_dataframe(
    sheet,
    df[["main code", "Len", "Code", "resource code", "resource name"]],
    include_index=False,
    include_column_header=True,
    row=1,
    col=5   # start writing from column E
)

