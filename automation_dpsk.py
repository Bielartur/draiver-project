import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime
import json
import os
from dotenv import load_dotenv

from src.fetch.api_client import fetch_dataframe
from src.config import WORKSHEET_GID_2

# Load environment variables
load_dotenv()

class SalesforceMetroUpdater:
    def __init__(self):
        self.sf = self.authenticate()
        self.logs = []
        
    def authenticate(self):
        """Authenticate with Salesforce using OAuth"""
        try:
            return Salesforce(
                username      = os.getenv('SF_USERNAME'),
                password      = os.getenv('SF_PASSWORD'),
                security_token= os.getenv('SF_SECURITY_TOKEN'),
                domain        = os.getenv('SF_DOMAIN', 'login')   # 'login' ou 'test'
            )
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            raise

    def load_and_clean_data(self):
        """Load and clean the metro data from the google sheets API"""
        df = fetch_dataframe(worksheet_gid=WORKSHEET_GID_2)
        
        # Clean data
        df = df[df['metro_area'].notna()]
        df = df[~df['metro_area'].isin(['', 'null'])]
        df.reset_index(drop=True, inplace=True)
        df['id'] = df.index + 1
        
        # Convert numeric columns
        numeric_cols = ['active_drivers', 'engaged_drivers', 'cdl_active_drivers', 
                       'inactive_drivers', 'l07d_itinerary', 'l14d_itinerary', 
                       'l30d_itinerary', 'l60d_itinerary', 'l90d_itinerary']
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        return df

    def find_existing_metro(self, metro_name):
        """Check if metro already exists in Salesforce"""
        query = f"SELECT Id FROM Metro_Area__c WHERE Name = '{metro_name}'"
        result = self.sf.query(query)
        return result['records'][0]['Id'] if result['totalSize'] > 0 else None

    def update_or_create_metro(self, row):
        """Update or create a Metro record in Salesforce"""
        metro_name = row['metro_area']
        payload = {
            'Name': metro_name,
            'Active_Drivers__c': row['active_drivers'],
            'Engaged_Drivers__c': row['engaged_drivers'],
            'CDL_Active_Drivers__c': row['cdl_active_drivers'],
            'Inactive_Drivers__c': row['inactive_drivers'],
            'L7D_Itinerary__c': row['l07d_itinerary'],
            'L14D_Itinerary__c': row['l14d_itinerary'],
            'L30D_Itinerary__c': row['l30d_itinerary'],
            'L60D_Itinerary__c': row['l60d_itinerary'],
            'L90D_Itinerary__c': row['l90d_itinerary']
        }
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "metro_id": row['id'],
            "metro_name": metro_name,
            "action": None,
            "payload": payload,
            "status": None,
            "response": None,
            "salesforce_id": None
        }
        
        try:
            existing_id = self.find_existing_metro(metro_name)
            
            if existing_id:
                # Update existing record
                result = self.sf.Metro_Area__c.update(existing_id, payload)
                log_entry.update({
                    "action": "update",
                    "status": "success",
                    "salesforce_id": existing_id,
                    "response": result
                })
            else:
                # Create new record
                result = self.sf.Metro_Area__c.create(payload)
                log_entry.update({
                    "action": "create",
                    "status": "success",
                    "salesforce_id": result['id'],
                    "response": result
                })
                
        except Exception as e:
            log_entry.update({
                "status": "error",
                "response": str(e)
            })
        
        self.logs.append(log_entry)
        return log_entry

    def process_file(self, batch_size=200):
        """Process the CSV file and update Salesforce in batches"""
        df = self.load_and_clean_data()
        total = len(df)
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1} of {total//batch_size + 1}")
            
            for _, row in batch.iterrows():
                self.update_or_create_metro(row)
        
        print(f"\nProcessing complete. {total} records processed.")
        self.save_logs()
        return self.logs

    def save_logs(self, filename='salesforce_metro_logs.json'):
        """Save the operation logs to a file"""
        with open(filename, 'w') as f:
            json.dump(self.logs, f, indent=2)
        print(f"Logs saved to {filename}")

    def get_logs_by_metro_id(self, metro_id):
        """Retrieve logs for a specific metro ID"""
        return [log for log in self.logs if log['metro_id'] == metro_id]


if __name__ == "__main__":
    # Example usage
    updater = SalesforceMetroUpdater()
    
    # Process the CSV file
    logs = updater.process_file()
    
    # Example: Get logs for metro with ID 1
    metro_1_logs = updater.get_logs_by_metro_id(1)
    print("\nLogs for Metro ID 1:")
    print(json.dumps(metro_1_logs, indent=2))