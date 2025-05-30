import argparse
import logging
import random
from src.db_config import get_db_connection
from src.validator import validate_cdr

logging.basicConfig(
    filename='logs/processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def process_cdrs(limit=None, simulate_errors=False, verbose=False):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        query = "SELECT * FROM cdr_raw"
        if limit:
            query += f" LIMIT {limit}"
        cur.execute(query)
        cdrs = cur.fetchall()

        for cdr in cdrs:
            cdr_id, msisdn, destination, duration, event_type, timestamp = cdr

            is_valid, validation_message = validate_cdr(cdr)
            if not is_valid:
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'failed', validation_message))
                logging.warning(f"Validation failed for CDR ID {cdr_id} | {validation_message}")
                continue

            if simulate_errors and random.random() < 0.2:
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'failed', 'Simulated random failure'))
                logging.warning(f"Simulated failure for CDR ID {cdr_id}")
                if verbose:
                    print(f"⚠️ Simulated error for CDR ID {cdr_id}")
                continue

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
                logging.info(f"Processed CDR ID {cdr_id} | Zone: {zone}, Cost: {cost:.2f}")
                if verbose:
                    print(f"✅ Success: CDR ID {cdr_id} - Zone: {zone}, Cost: {cost:.2f}")
            else:
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (cdr_id, 'failed', 'HLR data not found'))
                logging.warning(f"HLR not found for CDR ID {cdr_id} | MSISDN: {msisdn}")
                if verbose:
                    print(f"❌ HLR data not found for CDR ID {cdr_id}")

        conn.commit()
        logging.info("Finished processing CDRs.")
        if verbose:
            print("✅ All CDRs processed successfully.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Processing error: {e}")
        print("Error:", e)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CDRs with optional test mode and error simulation.")
    parser.add_argument('--test', action='store_true', help="Run in test mode (limit 5 CDRs)")
    parser.add_argument('--simulate-errors', action='store_true', help="Randomly simulate CDR processing failures")
    parser.add_argument('--verbose', action='store_true', help="Print processing status to stdout")
    args = parser.parse_args()

    process_cdrs(
        limit=5 if args.test else None,
        simulate_errors=args.simulate_errors,
        verbose=args.verbose
    )