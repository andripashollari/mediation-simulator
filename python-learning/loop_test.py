cdr_list = [
    {"msisdn": "355681111111", "duration": 45},
    {"msisdn": "355682222222", "duration": 130},
    {"msisdn": "355683333333", "duration": 300}
]

for cdr in cdr_list:
    print("Numri:", cdr["msisdn"])
    if cdr["duration"] > 180:
        print("→ Thirrje shumë e gjatë")
    elif cdr["duration"] > 60:
        print("→ Thirrje e gjatë")
    else:
        print("→ Thirrje e shkurtër")
