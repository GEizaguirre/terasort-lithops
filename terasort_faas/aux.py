from lithops import FunctionExecutor
from terasort_faas.df import deserialize
from terasort_faas.config import OUTPUT_PREFIX
import sys
import polars as pl
import logging
from terasort_faas.config import *
import hashlib
import base64

console_logger = logging.getLogger(CONSOLE_LOGGER)

def check_output(bucket, prefix):
    
    # Receives the bucket and prefix of the outputs as arguments
    bucket = sys.argv[1]
    prefix = sys.argv[2]

    executor = FunctionExecutor()

    object_keys = executor.storage.list_keys(bucket, prefix = prefix+f"/{OUTPUT_PREFIX}")
    object_keys.sort()

    all_keys = []

    for key in object_keys:
        
        object = executor.storage.get_object(bucket, key)
        data = deserialize(object)
        print(data)
        keys = data.select(["0"]).to_series()
        print(keys)
        is_ascending = keys.is_sorted()
        print(f"{key} is sorted: {is_ascending} ({len(keys)} registers).")
        
        all_keys.append(keys)

    all_keys = pl.concat(all_keys)
    # print(all_keys)
    is_ascending = all_keys.is_sorted()
    print(f"Global sorting: {is_ascending}. ({len(all_keys)} registers)")


def remove_intermediates(executor, bucket, prefixes):

    for prefix in prefixes:
        keys = executor.storage.list_objects(bucket, prefix=prefix)
        keys = [ k["Key"] for k in keys ]

        executor.storage.delete_objects(bucket, keys)


def warm_up_functions(runtime, runtime_memory):

    console_logger.info("Warming up functions.")

    executor = FunctionExecutor(runtime=runtime, runtime_memory=runtime_memory)

    def foo(x):
        return x
    
    fts = executor.map(foo, range(1000))
    executor.wait(fts)


def hash_to_5_chars(num):
    # Convert the integer to bytes
    num_bytes = num.to_bytes((num.bit_length() + 7) // 8, 'big')

    # Compute the SHA-256 hash
    hash_bytes = hashlib.sha256(num_bytes).digest()

    # Encode the hash in base64
    base64_hash = base64.b64encode(hash_bytes)

    # Take the first 5 characters
    return base64_hash[:5].decode()