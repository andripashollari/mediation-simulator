cdr_list = []

# Hap file-in për lexim
with open("python-learning/test_cdrs.txt", "r") as file:
    lines = file.readlines()

# Përpunim i çdo linje
for line in lines:
    msisdn, destination, duration, event_type = line.strip().split(",")
    
    # Krijo një dict për çdo thirrje
    cdr = {
        "msisdn": msisdn,
        "destination": destination,
        "duration": int(duration),
        "event_type": event_type
    }
    
    cdr_list.append(cdr)

# Printo listën e cdr-ve
for cdr in cdr_list:
    print(cdr)
