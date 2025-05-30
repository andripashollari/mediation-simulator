import argparse
from src.db_config import get_db_connection

def process_cdrs(limit=None):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        query = """
            SELECT * FROM cdr_raw
            WHERE id NOT IN (SELECT cdr_id FROM processing_logs)
        """
        if limit:
            query += f" LIMIT {limit}"
        cur.execute(query)
        cdrs = cur.fetchall()

        print(f"Found {len(cdrs)} unprocessed CDRs.")

        for cdr in cdrs:
            cdr_id, msisdn, destination, duration, event_type, timestamp = cdr

            cur.execute("SELECT country, is_roaming, operator_name FROM hlr_data WHERE msisdn = %s", (msisdn,))
            hlr_result = cur.fetchone()

            if hlr_result:
                country, is_roaming, operator_name = hlr_result

                if destination.startswith('355'):
                    zone = 'ALBANIA'
                    base_cost = 0.02
                elif destination.startswith('39'):
                    zone = 'EU'
                    base_cost = 0.05
                else:
                    zone = 'INTERNATIONAL'
                    base_cost = 0.10

                cost = base_cost + 0.05 if is_roaming else base_cost

                cur.execute("""
                    INSERT INTO billing_feed (
                        msisdn, destination, duration, event_type, timestamp,
                        is_roaming, operator_name, zone, cost
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    msisdn, destination, duration, event_type, timestamp,
                    is_roaming, operator_name, zone, cost
                ))

                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'success', f'Processed with zone {zone}, cost {cost:.2f}'))

            else:
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'failed', 'HLR data not found'))

        conn.commit()
        print("CDR processing completed.")

    except Exception as e:
        conn.rollback()
        print("Error:", e)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CDRs with optional test mode.")
    parser.add_argument('--test', action='store_true', help="Run in test mode (limit 5 CDRs)")
    args = parser.parse_args()

    if args.test:
        process_cdrs(limit=5)
    else:
        process_cdrs()