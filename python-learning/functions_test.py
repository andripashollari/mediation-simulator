cdr_list = [
    {"msisdn": "355681111111", "duration": 45},
    {"msisdn": "355682222222", "duration": 130},
    {"msisdn": "355683333333", "duration": 300}
]

def klasifiko_thirrjen(duration):
    if duration > 180:
        return "Thirrje shumë e gjatë"
    elif duration > 60:
        return "Thirrje e gjatë"
    else:
        return "Thirrje e shkurtër"

for cdr in cdr_list:
    print("Numri:", cdr["msisdn"])
    print("→", klasifiko_thirrjen(cdr["duration"]))