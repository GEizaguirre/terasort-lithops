import gc
from lithops import Storage
from terasort_faas.df.construct import construct_df
from terasort_faas.df.serialize import serialize_partitions
from terasort_faas.read_terasort_data import read_terasort_data
import concurrent
from terasort_faas.IO import get_read_range

class Mapper:

    storage: Storage

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


    def run(self):

        self.storage = Storage()

        self.read()

        self.partition()

        self.exchange()

    
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
        
        


    def partition(self):
        
        keys_list, values_list, partitioning = read_terasort_data(self.data, self.reduce_partitions)

        del self.data
        gc.collect()

        self.df = construct_df(keys_list, values_list)
        print("Read %d row"%(len(self.df)))
        del keys_list
        del values_list
        gc.collect()

        self.partitions = serialize_partitions(
            self.reduce_partitions,
            self.df, 
            partitioning
        )

        del self.df
        gc.collect()

    
    def exchange(self):
        
        base_id = self.reduce_partitions * self.partition_id

        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.partitions)) as executor:

            for reducer_id, data in self.partitions.items():

                subpartition_id = base_id + reducer_id

                future = executor.submit(
                     self.storage.put_object,  
                     self.bucket,
                     f"{self.timestamp_prefix}/intermediates/{subpartition_id}", 
                     b''.join([data]))
                
                futures.append(future)


def run_mapper(mapper: Mapper):
    mapper.run()


