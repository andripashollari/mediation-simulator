import psycopg2

# Enriched data nga dita e mëparshme
enriched_cdrs = [
    {
        "msisdn": "355681111111",
        "destination": "355692222222",
        "duration": 60,
        "event_type": "voice",
        "country": "Albania",
        "is_roaming": False,
        "operator_name": "One"
    },
    {
        "msisdn": "355682222222",
        "destination": "355693333333",
        "duration": 300,
        "event_type": "sms",
        "country": "Italy",
        "is_roaming": True,
        "operator_name": "TIM"
    }
]

# Lidhja me databazën
conn = psycopg2.connect(
    host="localhost",
    database="mediation_db",
    user="postgres",
    password="password"
)

# Krijimi i një kursori për të ekzekutuar komanda SQL
cur = conn.cursor()

# Loop për të futur të dhënat një nga një
for cdr in enriched_cdrs:
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

# Ruaj ndryshimet dhe mbyll lidhjen
conn.commit()
cur.close()
conn.close()

print("✅ Të dhënat u ruajtën me sukses në billing_feed.")
