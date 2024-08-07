import sqlite3
import json
import requests

class DatabaseManager:
    def __init__(self, user_db): 
        self.db = user_db
        self.conn = sqlite3.connect(self.db, timeout=10)
        self.cursor = self.conn.cursor()
    # Fetch data from the API
    def fetch_data_from_api(self):
        url = "https://vmsn-app-planner3test.azurewebsites.net/status/market/bid-result"
        params = {
            "CustomerId": "TestCustomer",
            "ForDate": "2024-02-03",
            "Market": "FCR-D-D1",
            "Country": "Sweden"
        }
        headers = {
            'ApiKey': 'Api-Planner-996ba74c-cdaf-4f66-9448-ffff'
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None
        
    # Create tables in the database
    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                externalId TEXT,
                day TEXT,
                dateOfLastChange TEXT,
                market TEXT,
                status TEXT,
                country TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS series (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_entry_id INTEGER,
                externalId TEXT,
                customerId TEXT,
                status TEXT,
                direction TEXT,
                currency TEXT,
                priceArea TEXT,
                assetId TEXT,
                price REAL,
                startInterval TEXT,
                endInterval TEXT,
                resolution TEXT,
                FOREIGN KEY (market_entry_id) REFERENCES market_entries(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_id INTEGER,
                quantity INTEGER,
                FOREIGN KEY (series_id) REFERENCES series(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_entry_id INTEGER,
                updateTime TEXT,
                fromStatus TEXT,
                toStatus TEXT,
                FOREIGN KEY (market_entry_id) REFERENCES market_entries(id)
            )
        ''')
        self.conn.commit()
    # Insert data into the database
    #It handles the hierarchical structure of the data, 
    # ensuring that foreign keys correctly link market_entries with series, and series with positions.
    def insert_data(self, data):
        # Insert into market_entries
        self.cursor.execute('''
            INSERT INTO market_entries (externalId, day, dateOfLastChange, market, status, country)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data['externalId'], data['day'], data['dateOfLastChange'], data['market'], data['status'], data['country']))
        market_entry_id = self.cursor.lastrowid
        
        # Insert into series and positions
        for series in data['series']:
            self.cursor.execute('''
                INSERT INTO series (market_entry_id, externalId, customerId, status, direction, currency, priceArea, assetId, price, startInterval, endInterval, resolution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (market_entry_id, series['externalId'], series['customerId'], series['status'], series['direction'], series['currency'], series['priceArea'], series['assetId'], series['price'], series['startInterval'], series['endInterval'], series['resolution']))
            series_id = self.cursor.lastrowid
            for position in series['positions']:
                self.cursor.execute('''
                    INSERT INTO positions (series_id, quantity)
                    VALUES (?, ?)
                ''', (series_id, position['quantity']))

        # Insert into update_history
        for update in data['updateHistory']:
            self.cursor.execute('''
                INSERT INTO update_history (market_entry_id, updateTime, fromStatus, toStatus)
                VALUES (?, ?, ?, ?)
            ''', (market_entry_id, update['updateTime'], update['fromStatus'], update['toStatus']))

        self.conn.commit()

# Main function
def main():
    db_manager = DatabaseManager('database.db')
    db_manager.create_tables()
    data = db_manager.fetch_data_from_api()
    if data:
        # Insert the data into the database
        db_manager.insert_data(data)
        print("Data inserted successfully  into the database.")
    else:
        print("Failed to fetch data.")


# Run the main function
if __name__ == '__main__':
    main()

    
