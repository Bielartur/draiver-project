# src/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Locate .env in the root
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SPREADSHEET_ID      = os.getenv("SPREADSHEET_ID")
WORKSHEET_GID       = int(os.getenv("WORKSHEET_GID"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
