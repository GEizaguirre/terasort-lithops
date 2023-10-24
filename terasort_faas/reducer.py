from lithops import Storage
import random
import concurrent
from terasort_faas.check_output import OUTPUT_PREFIX

from terasort_faas.IO import reader
from terasort_faas.df import serialize
from terasort_faas.df.construct import concat_progressive

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


    def run(self):

        self.storage = Storage()

        self.exchange()

        self.construct()

        self.sort()

        self.write()

    
    def exchange(self):

        map_parts = [ (map_partition * self.reduce_partitions) 
                     + self.partition_id for map_partition in range(self.map_partitions) ]
        
        random.shuffle(map_parts)

        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(map_parts)) as executor:

            for subpartition_id in map_parts:

                future = executor.submit(
                     reader, 
                     f"{self.timestamp_prefix}/intermediates/{subpartition_id}", 
                     self.bucket,
                     self.storage)
                
                futures.append(future)

        self.partitions = [ future.result() for future in futures ]
    

    def construct(self):
        
        self.df = concat_progressive(self.partitions)



    def sort(self):
        
        self.df = self.df.sort("0")

    
    def write(self):

        serialized_partition = serialize(self.df)

        self.storage.put_object(self.bucket,
                                f"{self.timestamp_prefix}/{OUTPUT_PREFIX}_{self.partition_id}",
                                serialized_partition)



def run_reducer(reducer: Reducer):
    reducer.run()


