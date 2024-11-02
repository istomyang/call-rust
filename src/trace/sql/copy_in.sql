COPY trace_log (
    msg_id,
    request_id,
    request_no,
    request_method,
    time_stamp,
    from_service_name,
    to_service_name,
    from_service_no,
    to_service_no,
    success,
    `description`,
) FROM STDIN BINARY