CREATE TABLE IF NOT EXISTS call_trace_log (
  id SERIAL PRIMARY KEY,
  msg_id VARCHAR(255),
  request_id VARCHAR(255),
  request_no INTEGER,
  time_stamp TIMESTAMP,
  timestamp_delta_ms INTEGER,
  from_service_name VARCHAR(255),
  to_service_name VARCHAR(255),
  from_service_id VARCHAR(255),
  to_service_id VARCHAR(255),
  transmit_duration_ms INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);