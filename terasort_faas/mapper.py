import gc
from typing import Dict
from lithops import Storage
from terasort_faas.df import construct_df,  serialize_partitions
from terasort_faas.read_terasort_data import read_terasort_data
import concurrent
from terasort_faas.IO import get_read_range
from terasort_faas.config import *
import logging
import time
import numpy as np

class Mapper:

    storage: Storage
    partitions: Dict[int, bytes]

    def __init__(self, 
                 partition_id: int,
                 map_partitions: int,
                 reduce_partitions: int,
                 timestamp_prefix: str,
                 bucket: str,
                 key: str
                 ):
        
        self.partition_id = partition_id
        self.map_partitions = map_partitions
        self.reduce_partitions = reduce_partitions
        self.timestamp_prefix = timestamp_prefix
        self.bucket = bucket
        self.key = key
        self.execution_data = dict()
        self.execution_data["worker_id"] = partition_id


    def run(self):

        self.execution_data["start_time"] = time.time()

        self.storage = Storage()

        self.read()

        self.execution_data["scan_time"] = time.time()
        self.execution_data["read_size"] = len(self.data)

        self.construct()

        self.execution_data["construct_time"] = time.time()
        self.execution_data["total_rows"] = len(self.df)
        row_counts = np.unique(self.partitioning, return_counts=True)
        self.execution_data["partition_rows"] = { k: v for k, v
                                                 in zip(row_counts[0], row_counts[1])}

        self.serialize()

        self.execution_data["partition_sizes"] = { p_id: len(p) for p_id, p in self.partitions.items() }
        self.execution_data["exchange_start"] = time.time()


        self.exchange()


        self.execution_data["end_time"] = time.time()

        return {"mapper_%d"%(self.partition_id): self.execution_data}

    
    def read(self):
        
        lower_bound, upper_bound = get_read_range(
            self.storage,
            self.bucket,
            self.key,
            self.partition_id,
            self.map_partitions
        )

        self.data: bytes = self.storage.get_object(self.bucket, self.key,
                                   extra_get_args={"Range": ''.join(
                                       ['bytes=', str(lower_bound), '-',
                                        str(upper_bound)])
                                   })
        
        print("Read %d bytes"%(len(self.data)))
        

    def construct(self):
        
        keys_list, values_list, self.partitioning = read_terasort_data(self.data, self.reduce_partitions)

        del self.data
        gc.collect()

        self.df = construct_df(keys_list, values_list)
        print("Read %d row"%(len(self.df)))
        del keys_list
        del values_list
        gc.collect()


    def serialize(self):

        self.partitions = serialize_partitions(
            self.reduce_partitions,
            self.df, 
            self.partitioning
        )

        del self.df
        del self.partitioning
        gc.collect()


    def exchange(self):
        
        base_id = self.reduce_partitions * self.partition_id

        futures = []

        def timed_put(bucket, key, body):
            then = time.time()
            self.storage.put_object(bucket, key, body)
            elapsed = time.time() - then
            return elapsed

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.partitions)) as executor:

            for reducer_id, data in self.partitions.items():

                subpartition_id = base_id + reducer_id

                future = executor.submit(
                     timed_put,  
                     self.bucket,
                     f"{self.timestamp_prefix}/intermediates/{subpartition_id}", 
                     data)
                
                futures.append(future)

        self.execution_data["write_times"] = { future_id: future.result() for future_id, future in enumerate(futures) }


def run_mapper(mapper: Mapper):
    mapper.run()


