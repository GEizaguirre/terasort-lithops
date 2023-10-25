import os
import click
from lithops import FunctionExecutor
from datetime import datetime
import time
from terasort_faas.IO import get_data_size
from terasort_faas.aux import remove_intermediates
from terasort_faas.logging.logging import setup_logger
from terasort_faas.logging.results import result_summary
from terasort_faas.mapper import Mapper, run_mapper
from terasort_faas.reducer import Reducer, run_reducer
from terasort_faas.config import bcolors
import logging
from terasort_faas.config import *
import yaml

console_logger = logging.getLogger(CONSOLE_LOGGER)
execution_logger = logging.getLogger(EXECUTION_LOGGER)
lithops_logger = logging.getLogger(__name__)

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
    setup_logger(timestamp_prefix)
    
    executor = FunctionExecutor(runtime_memory=runtime_memory, runtime=runtime_name)

    dataset_size = get_data_size(executor.storage, bucket, key)

    click.echo("Sorting dataset: "+bcolors.BOLD+bcolors.OKBLUE+"%s "%(key)+bcolors.ENDC+bcolors.ENDC+"(%dMB)"%(dataset_size / 1024 / 1024))
    click.echo("\t- "+bcolors.BOLD+"%d"%(map_parallelism)+bcolors.ENDC+" map partitions.")
    click.echo("\t- "+bcolors.BOLD+"%d"%(reduce_parallelism)+bcolors.ENDC+" reduce partitions.")

    execution_logger.info(yaml.dump(
            {"execution_info": {
                "dataset": key,
                "map_parallelism": map_parallelism,
                "reduce_parallelism": reduce_parallelism,
                "dataset_size": dataset_size / 1024 / 1024,
                "timestamp": timestamp_prefix
             }}, 
            default_flow_style=False
        ))

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
    reducer_futures = executor.map(run_reducer, reducers)
    executor.wait(reducer_futures)
    end_time = time.time()

    click.echo(bcolors.OKGREEN+bcolors.BOLD+"Client sort time: %.2f s"%(end_time-start_time)+bcolors.ENDC+bcolors.ENDC)

    function_results = executor.get_result(map_futures+reducer_futures)

    for result in function_results:
        execution_logger.info(yaml.dump(
                result, 
                default_flow_style=False
            ))

    execution_data = {
        "start_time": start_time,
        "end_time": end_time
    }
    execution_logger.info(
        yaml.dump(
            {"sort": execution_data}, 
            default_flow_style=False
        )
    )


    log_file = os.path.join(LOG_PATH, "%s.yaml"%(timestamp_prefix))
    print("Log file: %s"%(log_file))

    click.echo("\n\nRemoving intermediates...")
    remove_intermediates(executor, bucket, timestamp_prefix)

    result_summary(log_file)



    