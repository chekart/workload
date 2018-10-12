LOG_TRIM = 20
MAX_RETRY_SLEEP = 60
DEFAULT_CHUNK_SIZE = 100


def parse_int(int_str, default=0):
    try:
        return int(int_str)
    except (ValueError, TypeError):
        return default


def chunk_workload(workload, size):
    """
    Splits workload into multiple chunks,
    to iteratively add large amount of data into redis
    Currently split happens only by number of elements
    """
    chunk = []
    for item in workload:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
