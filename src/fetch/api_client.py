import logging
from typing import Optional, List, Dict
import pandas as pd

from config import (
    SERVICE_ACCOUNT_FILE,
    SPREADSHEET_ID,
    WORKSHEET_GID,
    SCOPES,
)

# Configure module-level logger
glogger = logging.getLogger(__name__)

def get_gspread_client(
    service_account_file: str = SERVICE_ACCOUNT_FILE,
    scopes: List[str] = SCOPES
):
    """
    Authenticate with Google API using a service account and return a gspread client.
    """
    # Lazy import so module can be imported without gspread installed
    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_file(
        service_account_file,
        scopes=scopes
    )
    return gspread.authorize(creds)


def fetch_sheet_records(
    spreadsheet_id: str = SPREADSHEET_ID,
    worksheet_gid: int = WORKSHEET_GID,
    client: Optional[object] = None
) -> List[Dict]:
    """
    Open the spreadsheet by its ID and return all rows of the specified worksheet
    as a list of dictionaries.
    """
    # Use injected client for easier testing, or create a new one
    client = client or get_gspread_client()

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
    except Exception as e:
        glogger.error(
            f"Unable to open spreadsheet {spreadsheet_id}: {e}"
        )
        raise

    try:
        worksheet = next(
            ws for ws in spreadsheet.worksheets() if ws.id == worksheet_gid
        )
    except StopIteration:
        error_msg = (
            f"Worksheet with GID={worksheet_gid} not found "
            f"in spreadsheet {spreadsheet_id}."
        )
        glogger.error(error_msg)
        raise ValueError(error_msg)

    return worksheet.get_all_records()


def fetch_dataframe(
    spreadsheet_id: str = SPREADSHEET_ID,
    worksheet_gid: int = WORKSHEET_GID,
    client: Optional[object] = None
) -> pd.DataFrame:
    """
    Fetch sheet records and return them as a pandas DataFrame for further processing.
    """
    records = fetch_sheet_records(
        spreadsheet_id=spreadsheet_id,
        worksheet_gid=worksheet_gid,
        client=client
    )
    return pd.DataFrame(records)
