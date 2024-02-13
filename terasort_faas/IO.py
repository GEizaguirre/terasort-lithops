import http
from lithops import Storage
from typing import Tuple, Union, List, TextIO, BinaryIO
from math import floor
import time
from botocore.exceptions import ClientError
from lithops.storage.utils import StorageNoSuchKeyError
from terasort_faas.config import *
import logging


console_logger = logging.getLogger(CONSOLE_LOGGER)


def get_data_size(storage: Storage,
                  bucket: str,
                  path: str) \
        -> int:
    
    return int(storage.head_object(bucket, path)['content-length'])


def get_read_range(storage: Storage,
                    bucket: str, 
                    key: str,
                    partition_id: int,
                    num_partitions: int) \
        -> Tuple[int, int]:
    """
    Calculate byte range to read from a dataset, given the id of the partition.
    """

    total_size = get_data_size(storage, bucket, key)
    total_registers = total_size/100

    registers_per_worker = total_registers // num_partitions

    lower_bound = partition_id * registers_per_worker * 100
    
    if partition_id == (num_partitions - 1):
        upper_bound = total_size - 1
    else:
        upper_bound = lower_bound + registers_per_worker * 100 - 1

    return int(lower_bound), int(upper_bound)


class IncrementalWaiter:
    def __init__(self):
        self.wait_time = 0.25
        
    def wait(self):
        time.sleep(self.wait_time)
        self.wait_time *= 2    


def reader(key: int,
           bucket: str,
           storage: Storage) \
        -> bytes:
            
    retry = 0
    waiter = IncrementalWaiter()

    before_readt = time.time()

    while retry < MAX_RETRIES:

        try:

            data = storage.get_object(
                bucket = bucket,
                key = key
            )

            return data, time.time() - before_readt, len(data)


        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                if time.time() - before_readt > MAX_READ_TIME:
                    return b"", -1, -1
                
            waiter.wait()
            continue

        except StorageNoSuchKeyError as ex:
            if time.time() - before_readt > MAX_READ_TIME:
                return b"", -1, -1
            waiter.wait()
            continue

        except (http.client.IncompleteRead) as e:
            if retry == MAX_RETRIES:
                return b"", -1, -1
            retry += 1
            continue

        except Exception as e:
            console_logger.info(f"{e}")
            return b"", -1, -1 
    
    return b"", -1, -1 
        
def timed_put(storage, bucket, key, body):
    
    retry = 0
    waiter = IncrementalWaiter()

    then = time.time()

    while retry < MAX_RETRIES:

        try:

            storage.put_object(bucket, key, body)
            elapsed = time.time() - then

            return elapsed


        except ClientError as ex:
            if time.time() - then > MAX_READ_TIME:
                return -1
                
            waiter.wait()
            continue

        except Exception as e:
            console_logger.info(f"{e}")
            return -1
        
    return -1
        

    
    
    
