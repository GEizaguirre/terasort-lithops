from lithops import Storage
import random
import concurrent
from terasort_faas.config import OUTPUT_PREFIX
from terasort_faas.IO import reader
from terasort_faas.df import serialize, concat_progressive
import time


class Reducer():

    storage: Storage

    def __init__(self, 
                 partition_id,
                 map_partitions,
                 reduce_partitions,
                 timestamp_prefix,
                 bucket,
                 key):

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

        self.exchange()

        self.execution_data["exchange_end"] = time.time()

        self.construct()

        self.execution_data["aggregation_time"] = time.time()
        self.execution_data["total_rows"] = len(self.df)

        self.sort()

        self.execution_data["sort_time"] = time.time()

        self.write()

        self.execution_data["end_time"] = time.time()

        return {"reducer_%d"%(self.partition_id): self.execution_data}

    
    def exchange(self):

        map_parts = [ (map_partition * self.reduce_partitions) 
                     + self.partition_id for map_partition in range(self.map_partitions) ]
        
        # random.shuffle(map_parts)

        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(map_parts)) as executor:

            for subpartition_id in map_parts:

                future = executor.submit(
                     reader, 
                     f"{self.timestamp_prefix}/intermediates/{subpartition_id}", 
                     self.bucket,
                     self.storage)
                
                futures.append(future)

        results = [ future.result() for future in futures ]
        self.partitions = [ f[0] for f in results ]
        self.execution_data["read_times"] = { f_id: f[1] for f_id, f in enumerate(results) }
        self.execution_data["read_sizes"] = { f_id: f[2] for f_id, f in enumerate(results) }
    

    def construct(self):
        
        self.df = concat_progressive(self.partitions)



    def sort(self):
        
        self.df = self.df.sort("0")

    
    def write(self):

        serialized_partition = serialize(self.df)

        self.execution_data["serialize_time"] = time.time()

        self.storage.put_object(self.bucket,
                                f"{self.timestamp_prefix}/{OUTPUT_PREFIX}_{self.partition_id}",
                                serialized_partition)



def run_reducer(reducer: Reducer):
    return reducer.run()


