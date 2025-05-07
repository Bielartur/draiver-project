# tests/fetch/test_api_client.py

import pytest
import pandas as pd
from fetch.api_client import fetch_sheet_records, fetch_dataframe

class DummyWorksheet:
    def __init__(self, id, records):
        self.id = id
        self._records = records

    def get_all_records(self):
        return self._records

class DummySpreadsheet:
    def __init__(self, worksheets):
        self._worksheets = worksheets

    def worksheets(self):
        return self._worksheets

class DummyClient:
    def __init__(self, spreadsheet):
        self._spreadsheet = spreadsheet

    def open_by_key(self, spreadsheet_id):
        return self._spreadsheet

def test_fetch_sheet_records_success():
    records = [{'col1': 'value1'}, {'col1': 'value2'}]
    ws = DummyWorksheet(id=42, records=records)
    ss = DummySpreadsheet([ws])
    client = DummyClient(ss)

    result = fetch_sheet_records(
        spreadsheet_id='ignored',
        worksheet_gid=42,
        client=client
    )
    assert result == records

def test_fetch_sheet_records_not_found():
    ws = DummyWorksheet(id=99, records=[])
    ss = DummySpreadsheet([ws])
    client = DummyClient(ss)

    with pytest.raises(ValueError):
        fetch_sheet_records(
            spreadsheet_id='ignored',
            worksheet_gid=100,
            client=client
        )

def test_fetch_dataframe_returns_dataframe():
    records = [{'a': 1}, {'a': 2}]
    ws = DummyWorksheet(id=7, records=records)
    ss = DummySpreadsheet([ws])
    client = DummyClient(ss)

    df = fetch_dataframe(
        spreadsheet_id='ignored',
        worksheet_gid=7,
        client=client
    )
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient='records') == records
