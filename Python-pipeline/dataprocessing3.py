import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import json

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"
SHEET_ID = "YOUR SHEET ID"
SHEET_NAME = "Dataset"
CPM_JSON_FILE = r"D:/News-team-individual dashboards/Documents/cpmcategory.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Load CPM Category mapping from JSON
with open(CPM_JSON_FILE, "r", encoding="utf-8") as f:
    cpmcategory = json.load(f)

cpm_map = {entry["shows name"]: entry["cpm category"] for entry in cpmcategory}

# Authenticate Google Sheets
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# --- PART 1: Published Date & Time ---
published_times = sheet.col_values(4)  # Column D
utc = pytz.utc
local_tz = pytz.timezone("Asia/Karachi")

date_values, time_values = [], []
for pub_time in published_times[1:]:
    if not pub_time.strip():
        date_values.append([""])
        time_values.append([""])
        continue
    dt_utc = datetime.strptime(pub_time, "%Y-%m-%dT%H:%M:%SZ")
    dt_local = utc.localize(dt_utc).astimezone(local_tz)
    date_values.append([dt_local.strftime("%Y-%m-%d")])
    time_values.append([dt_local.strftime("%H:%M:%S")])

sheet.update(f"M2:M{len(date_values)+1}", date_values)
sheet.update(f"N2:N{len(time_values)+1}", time_values)

# --- PART 2: Net Subscribers (X - Y = Z) ---
subs_gained = sheet.col_values(24)  # X
subs_lost = sheet.col_values(25)    # Y

subscribers = []
for g, l in zip(subs_gained[1:], subs_lost[1:]):
    try:
        gained, lost = int(g or 0), int(l or 0)
        subscribers.append([gained - lost])
    except ValueError:
        subscribers.append([""])
sheet.update(f"Z2:Z{len(subscribers)+1}", subscribers)

# --- PART 3: Engagement Rate, CPV, RPM, CPM Category ---
views = sheet.col_values(16)      # P
comments = sheet.col_values(19)   # S
likes = sheet.col_values(20)      # T
shares = sheet.col_values(21)     # U
revenue = sheet.col_values(22)    # V
show_names = sheet.col_values(10) # J
categories = sheet.col_values(12) # L

engagement_rate, cpv, rpm, cpm_cat = [], [], [], []
for v, c, l, sh, r, show, cat in zip(
    views[1:], comments[1:], likes[1:], shares[1:], revenue[1:], show_names[1:], categories[1:]
):
    try:
        v = int(v or 0)
        c, l, sh = int(c or 0), int(l or 0), int(sh or 0)
        r = float(r or 0)

        # Engagement %
        er = ((c + l + sh) / v * 100) if v > 0 else 0
        engagement_rate.append([round(er, 2)])

        # CPV
        cpv_val = (r / v) if v > 0 else 0
        cpv.append([round(cpv_val, 6)])

        # RPM
        rpm.append([round(cpv_val * 1000, 2)])

        # CPM Category
        if cat.strip() == "International News":
            cpm_cat.append([show])  # take show name directly
        else:
            cpm_cat.append([cpm_map.get(show, "")])

    except ValueError:
        engagement_rate.append([""])
        cpv.append([""])
        rpm.append([""])
        cpm_cat.append([""])

sheet.update(f"AA2:AA{len(engagement_rate)+1}", engagement_rate)
sheet.update(f"AB2:AB{len(cpv)+1}", cpv)
sheet.update(f"AC2:AC{len(rpm)+1}", rpm)
sheet.update(f"AD2:AD{len(cpm_cat)+1}", cpm_cat)

print("✅ M/N (date/time), Z (subs), AA (Engagement%), AB (CPV), AC (RPM), AD (CPM Category) updated")
