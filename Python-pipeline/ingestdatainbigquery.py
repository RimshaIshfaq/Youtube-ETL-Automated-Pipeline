import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = r"D:/News-team-individual dashboards/YTauthentication/google_sheet_api.json"

SHEET_ID = "YOUR GOOGLE SHEET ID"
SHEET_NAME = "Dataset"

BQ_PROJECT= "YOUR BIG QUERY PORJECT"          # GCP Project ID
BQ_DATASET = "YOUR BIG QUERY DATASET"       # BigQuery Dataset
BQ_TABLE   = "YOUR BIG QUERY PROJECT"          # BigQuery Table

# --- AUTHENTICATION ---
# Google Sheets
gs_credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(gs_credentials)

# BigQuery
bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)


def load_sheet_data():
    """Fetch Google Sheet data into pandas DataFrame."""
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(SHEET_NAME)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    return df, ws


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename dataframe columns to be BigQuery-compatible."""
    clean_cols = (
        df.columns.str.strip()
                 .str.replace(r"[^\w]", "_", regex=True)   # replace non-alphanumeric with _
                 .str.replace(r"__+", "_", regex=True)     # collapse multiple underscores
                 .str.strip("_")                          # remove leading/trailing _
    )
    df.columns = clean_cols
    return df


def append_to_bigquery(df: pd.DataFrame):
    """Create table if not exists, else append data into BigQuery table."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        bq_client.get_table(table_id)  # Check if table exists
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        print(f"🔄 Appending {len(df)} rows into {table_id}")
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_EMPTY",  # Create new table
            autodetect=True
        )
        print(f"🆕 Creating table {table_id} and inserting {len(df)} rows")

    load_job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    load_job.result()  # Wait for job to finish
    print(f"✅ Data successfully written to {table_id}")


def deduplicate_bigquery():
    """Deduplicate BigQuery table based on video_id."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    dedup_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_dedup"

    query = f"""
    CREATE OR REPLACE TABLE `{dedup_table}` AS
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT *,
             ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY CURRENT_TIMESTAMP() DESC) AS row_num
      FROM `{table_id}`
    )
    WHERE row_num = 1;

    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT * FROM `{dedup_table}`;
    """

    query_job = bq_client.query(query)
    query_job.result()
    print(f"✅ Deduplicated {table_id} based on video_id")


def clear_google_sheet(ws):
    """Clear all rows except header."""
    all_values = ws.get_all_values()
    if not all_values:
        return

    header = all_values[0]  # first row
    ws.clear()
    ws.append_row(header)
    print("✅ Cleared Google Sheet (kept header)")


def main():
    # Step 1: Load sheet data
    df, ws = load_sheet_data()

    if df.empty:
        print("⚠️ No new data found in Google Sheet.")
        return

    # Step 2: Sanitize headers
    df = sanitize_columns(df)

    # Step 3: Force all values to string (avoid ArrowTypeError)
    df = df.astype(str)

    # Step 4: Append to BigQuery
    append_to_bigquery(df)

    # Step 5: Deduplicate BigQuery
    deduplicate_bigquery()

    # Step 6: Clear sheet (except header)
    clear_google_sheet(ws)


if __name__ == "__main__":
    main()
