import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="mediation_db",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # Lexo 5 CDR nga tabela cdr_raw
    cur.execute("SELECT * FROM cdr_raw LIMIT 5")
    cdrs = cur.fetchall()

    for cdr in cdrs:
        cdr_id, msisdn, destination, duration, event_type, timestamp = cdr
        try:
            # Merr të dhënat nga hlr_data
            cur.execute("SELECT is_roaming, operator_name FROM hlr_data WHERE msisdn = %s", (msisdn,))
            hlr_result = cur.fetchone()

            if hlr_result:
                is_roaming, operator_name = hlr_result
                # Fut të dhënat në billing_feed
                cur.execute("""
                    INSERT INTO billing_feed (
                        msisdn, destination, duration, event_type,
                        timestamp, is_roaming, operator_name
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    msisdn, destination, duration, event_type,
                    timestamp, is_roaming, operator_name
                ))

                # Regjistro suksesin në log
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (
                    cdr_id, "SUCCESS", "CDR processed successfully"
                ))
            else:
                # Nëse nuk gjendet në hlr_data
                cur.execute("""
                    INSERT INTO processing_logs (cdr_id, status, message)
                    VALUES (%s, %s, %s)
                """, (
                    cdr_id, "FAILURE", "HLR data not found"
                ))

        except Exception as e:
            conn.rollback()
            # Nëse ndodh gabim për këtë CDR
            cur.execute("""
                INSERT INTO processing_logs (cdr_id, status, message)
                VALUES (%s, %s, %s)
            """, (
                cdr_id, "FAILURE", str(e)
            ))

    conn.commit()
    print("✅ Batch përpunuar me sukses.")

except Exception as e:
    print(f"❌ Gabim gjatë lidhjes me DB: {e}")

finally:
    cur.close()
    conn.close()
