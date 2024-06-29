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
# Converte la data dal formato "16/06/2024 00:09:00" al formato "2024-06-16T16:11:55.270Z"
def convert_to_iso_format(date_str):
   
    date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    iso_format = date_obj.isoformat() + ".000Z"
    return iso_format


def send_data(row):

    
    
    data = {
            "categoria": row[0],
            "numTreno": row[1],
            "stazPart": row[2],
            "oraPart": row[3],
            "ritardoPart": row[4],
            "stazArr": row[5],
            "oraArr": row[6],
            "ritardoArr": row[7],
            "provvedimenti": row[8],
            "variazioni": row[9]
            }

    connected = False
    while not connected:
        try:
            # Create a TCP/IP socket and connect to Logstash
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(json.dumps(data).encode('utf-8'))
            sock.close()
            connected = True
        except ConnectionRefusedError:
            print(f"Connection to {TCP_IP}:{TCP_PORT} refused. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY) 



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
                        row[3] = convert_to_iso_format(row[3])
                        row[6] = convert_to_iso_format(row[6])
                        print(row)
                        send_data(row)
            os.remove(CSV_FILE_PATH)
        except Exception as e:
            print(f"Error processing CSV: {e}")

while True:
    fetch_csv()
    process_csv()
    time.sleep(120)
