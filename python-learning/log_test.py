import psycopg2

# Një shembull CDR për testim
cdr = {
    "cdr_id": 1,
    "msisdn": "355681234567",
    "destination": "355682345678",
    "duration": 120,
    "event_type": "voice",
    "is_roaming": False,
    "operator_name": "One"
}

try:
    # Lidhja me databazën
    conn = psycopg2.connect(
        host="localhost",
        database="mediation_db",
        user="postgres",
        password="password"
    )
    cur = conn.cursor()

    # INSERT në billing_feed
    cur.execute("""
        INSERT INTO billing_feed (
            msisdn, destination, duration, event_type,
            timestamp, is_roaming, operator_name
        )
        VALUES (%s, %s, %s, %s, NOW(), %s, %s)
    """, (
        cdr["msisdn"],
        cdr["destination"],
        cdr["duration"],
        cdr["event_type"],
        cdr["is_roaming"],
        cdr["operator_name"]
    ))

    # INSERT në processing_logs
    cur.execute("""
        INSERT INTO processing_logs (cdr_id, status, message)
        VALUES (%s, %s, %s)
    """, (
        cdr["cdr_id"],
        "SUCCESS",
        "Inserted successfully"
    ))

    conn.commit()
    print("✅ CDR u ruajt dhe u log-ua me sukses.")

except Exception as e:
    # Në rast gabimi, regjistro në logs
    conn.rollback()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO processing_logs (cdr_id, status, message)
        VALUES (%s, %s, %s)
    """, (
        cdr["cdr_id"],
        "FAILURE",
        str(e)
    ))
    conn.commit()
    print("❌ Gabim gjatë futjes: u log-ua si FAILURE.")

finally:
    cur.close()
    conn.close()
