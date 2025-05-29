CREATE TABLE IF NOT EXISTS cdr_raw (
    id SERIAL PRIMARY KEY,
    msisdn VARCHAR(20),
    destination VARCHAR(20),
    duration INTEGER,
    event_type VARCHAR(10),
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hlr_data (
    msisdn VARCHAR(20) PRIMARY KEY,
    country VARCHAR(50),
    is_roaming BOOLEAN,
    operator_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS billing_feed (
    id SERIAL PRIMARY KEY,
    msisdn VARCHAR(20),
    destination VARCHAR(20),
    duration INTEGER,
    event_type VARCHAR(10),
    timestamp TIMESTAMP,
    is_roaming BOOLEAN,
    operator_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS processing_logs (
    id SERIAL PRIMARY KEY,
    cdr_id INTEGER,
    status VARCHAR(20),
    message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
