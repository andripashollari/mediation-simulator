from src.db_config import get_db_connection

def process_cdrs():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM cdr_raw LIMIT 5")
        cdrs = cur.fetchall()

        for cdr in cdrs:
            cdr_id, msisdn, destination, duration, event_type, timestamp = cdr

            cur.execute("SELECT country, is_roaming, operator_name FROM hlr_data WHERE msisdn = %s", (msisdn,))
            hlr_result = cur.fetchone()

            if hlr_result:
                country, is_roaming, operator_name = hlr_result

                cur.execute("""
                    INSERT INTO billing_feed (msisdn, destination, duration, event_type, timestamp, is_roaming, operator_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (msisdn, destination, duration, event_type, timestamp, is_roaming, operator_name))

                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'success', 'Processed successfully'))

            else:
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'failed', 'HLR data not found'))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Error:", e)

    finally:
        cur.close()
        conn.close()