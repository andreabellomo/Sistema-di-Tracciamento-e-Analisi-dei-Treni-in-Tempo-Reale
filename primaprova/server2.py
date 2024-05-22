import json
from datetime import datetime
import os
import time
import socket
import csv
import requests

TCP_IP = 'logstash'
TCP_PORT = 5002
RETRY_DELAY = 60
CSV_FILE_PATH = 'train_data.csv'
LAST_PROCESSED_IDS = set()

def fetch_csv():
    today = datetime.now().strftime("%d_%m_%Y")
    url = f"https://trainstats.altervista.org/exportcsv.php?data={today}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            csv_content = response.text
            with open(CSV_FILE_PATH, 'w') as file:
                file.write(csv_content)
        else:
            print(f"Failed to fetch CSV data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching CSV: {e}")
        time.sleep(60)

def process_csv():
    print("ciao mbare")
    global LAST_PROCESSED_IDS
    if os.path.exists(CSV_FILE_PATH):
        try:
            with open(CSV_FILE_PATH, 'r') as file:
                reader = csv.reader(file)
               
                for row in reader:
                    row_id = row[1]  # Assuming raw[1] contains the unique identifier
                    if row_id not in LAST_PROCESSED_IDS:
                        LAST_PROCESSED_IDS.add(row_id)
                        print(row)
            os.remove(CSV_FILE_PATH)
        except Exception as e:
            print(f"Error processing CSV: {e}")

while True:
    fetch_csv()
    process_csv()
    time.sleep(1800)
