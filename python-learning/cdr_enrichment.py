with open("python-learning/test_cdrs.txt", "r") as file:
    lines = file.readlines()

hlr_data = {
    "355681111111": {"country": "Albania", "is_roaming": False, "operator_name": "One"},
    "355682222222": {"country": "Italy", "is_roaming": True, "operator_name": "TIM"},
    "355683333333": {"country": "Germany", "is_roaming": True, "operator_name": "Vodafone"}
}

enriched_cdrs = []
for line in lines:
    msisdn, dest, duration, event_type = line.strip().split(",")

    base_cdr = {
        "msisdn" : msisdn,
        "destination" : dest,
        "duration" : int(duration),
        "event_type" : event_type
    }
    if msisdn in hlr_data:
        enriched = {**base_cdr, **hlr_data[msisdn]}
    else:
        enriched = {**base_cdr, "country": "Unknown", "is_roaming": None, "operator_name": "Unknown"}
    
    enriched_cdrs.append(enriched)

for cdr in enriched_cdrs:
    print(cdr)


