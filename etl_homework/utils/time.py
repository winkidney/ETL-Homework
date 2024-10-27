def get_last_recent_n_minutes_timestamp(current_timestamp, minutes=5):
    current_minutes = current_timestamp // 60
    nearest_n_minute = (current_minutes // minutes) * minutes
    nearest_timestamp = nearest_n_minute * 60
    return nearest_timestamp
