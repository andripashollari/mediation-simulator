def validate_cdr(cdr):
    cdr_id, msisdn, destination, duration, event_type, timestamp = cdr

    if not msisdn or not msisdn.isdigit() or len(msisdn) < 10:
        return False, "Invalid MSISDN"
    
    if not destination or not destination.startswith(('355', '39')):
        return False, "Invalid destination"
    
    if not isinstance(duration, int) or duration < 0:
        return False, "Invalid duration"
    
    if event_type not in ('voice', 'sms', 'data'):
        return False, "Invalid event type"
    
    if not timestamp:
        return False, "Missing timestamp"
    
    return True, "Valid"
