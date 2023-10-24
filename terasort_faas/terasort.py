import click
import os
from lithops import FunctionExecutor
from datetime import datetime
import time
from terasort_faas.IO import get_data_size
from terasort_faas.mapper import Mapper, run_mapper
from terasort_faas.reducer import Reducer, run_reducer
from terasort_faas.config import bcolors


def remove_intermediates(executor, bucket, timestamp_prefix):

    keys = executor.storage.list_objects(bucket, prefix=timestamp_prefix)
    keys = [ k["Key"] for k in keys ]

    executor.storage.delete_objects(bucket, keys)



def run_terasort(
        bucket,
        key,
        map_parallelism,
        reduce_parallelism,
        runtime_name,
        runtime_memory,
):
    
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_prefix = f"{timestamp}"
    
    executor = FunctionExecutor(runtime_memory=runtime_memory)

    click.echo("Sorting dataset: "+bcolors.BOLD+bcolors.OKBLUE+"%s "%(key)+bcolors.ENDC+bcolors.ENDC+"(%dMB)"%(get_data_size(executor.storage, bucket, key) / 1024 / 1024))
    click.echo("\t- "+bcolors.BOLD+"%d"%(map_parallelism)+bcolors.ENDC+" map partitions.")
    click.echo("\t- "+bcolors.BOLD+"%d"%(reduce_parallelism)+bcolors.ENDC+" reduce partitions.")

    mappers = [
                Mapper(
                    partition_id, 
                    map_parallelism, 
                    reduce_parallelism, 
                    timestamp_prefix, 
                    bucket, 
                    key
                )
               for partition_id in range(map_parallelism)
    ]
    
    reducers = [
        Reducer(
            partition_id,
            map_parallelism,
            reduce_parallelism,
            timestamp_prefix,
            bucket,
            key
        )
        for partition_id in range(reduce_parallelism)
    ]

    start_time = time.time()
    # run_mappers
    map_futures = executor.map(run_mapper, mappers)

    executor.wait(map_futures, return_when=20)

    # run_reducers
    reduce_futures = executor.map(run_reducer, reducers)
    executor.wait(reduce_futures)
    end_time = time.time()

    click.echo(bcolors.OKGREEN+bcolors.BOLD+"Sort time: %.2f s"%(end_time-start_time)+bcolors.ENDC+bcolors.ENDC)

    click.echo("\n\nRemoving intermediates...")
    # remove_intermediates(executor, bucket, timestamp_prefix)
    