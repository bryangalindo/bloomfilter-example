import os
import time
import uuid

from pybloomfilter import BloomFilter
from redis import StrictRedis


# Bloom Filter Parameters
BLOOM_FILTER_FILE = 'filter.bloom'
MAX_RECORDS = 500_000
ERROR_RATE = 0.01  # 1%

redis_client = StrictRedis().from_url('redis://localhost:6379')
bloom_filter = BloomFilter(capacity=MAX_RECORDS,
                           error_rate=ERROR_RATE,
                           filename=BLOOM_FILTER_FILE)

# Create list of unique ids (e.g., '27f2a19f-c582-462b-8156-20715b10181d')
unique_ids = [uuid.uuid4() for i in range(0, MAX_RECORDS)]


def get_bloom_filter_stats():
    start = time.time()
    false_positives = 0

    for i, element in enumerate(unique_ids):
        if element in bloom_filter:
            print(f'false positive for id={element} at index={i}')
            false_positives += 1

        # hashes element, and stores in bit array
        bloom_filter.add(element)

    elapsed_time = time.time() - start
    return {'execution_time': elapsed_time, 'false_positives': false_positives}


def get_redis_execution_time():
    start = time.time()
    for element in unique_ids:
        redis_client.get(str(element))
    elapsed_time = time.time() - start
    return elapsed_time


if __name__ == '__main__':
    stats = get_bloom_filter_stats()
    false_positives = stats['false_positives']
    bloom_execution_time = stats['execution_time']

    expected_error_rate = '{:.2f}'.format(ERROR_RATE * 100)
    actual_error_rate = '{:.4f}'.format(false_positives / MAX_RECORDS * 100)
    emulated_requests = '{:,}'.format(MAX_RECORDS)
    formatted_false_positives = '{:,}'.format(false_positives)
    num_requests = '{:,}'.format(MAX_RECORDS)
    file_size = '{:.2f}'.format(os.stat(BLOOM_FILTER_FILE).st_size / float(1 << 20))

    rds_execution_time = get_redis_execution_time()

    print(f'\n## Bloom Statistics ##'
          f'\nemulated_requests: {emulated_requests}'
          f'\nfalse_positives: {formatted_false_positives}'
          f'\nexpected_error_rate: {expected_error_rate}%'
          f'\nactual_error_rate: {actual_error_rate}%'
          f'\nbloom filter file_size: {file_size}MB'
          f'\n\n## Bloom vs Redis ##'
          f'\nBloom execution_time: {int(bloom_execution_time * 1000)}ms'
          f'\nRedis execution_time: {int(rds_execution_time)}s'
          f'\n\n## Conclusion ##'
          f'\nBloom is {int(rds_execution_time / bloom_execution_time)}x faster than Redis')

