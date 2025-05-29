import csv
from db_config import get_db_connection

def load_sample_cdr(csv_file_path):
    
    conn = get_db_connection()
    cur = conn.cursor()

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print("Row: ", row)
            msisdn = row['msisdn']
            destination = row['destination']
            duration = int(row['duration'])
            event_type = row['event_type']
            timestamp = row['timestamp']

            cur.execute("""
                INSERT INTO cdr_raw (msisdn, destination, duration, event_type, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (msisdn, destination, duration, event_type, timestamp))

    conn.commit()
    cur.close()
    conn.close()
    print("CDRs inserted successfully.")

if __name__ == "__main__":
    load_sample_cdr("../data/sample_cdr.csv")
