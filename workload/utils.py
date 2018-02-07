LOG_TRIM = 20
MAX_RETRY_SLEEP = 60


def parse_int(int_str, default=0):
    try:
        return int(int_str)
    except (ValueError, TypeError):
        return default
