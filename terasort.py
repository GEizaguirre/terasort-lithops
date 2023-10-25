import click
from terasort_faas.aux import warm_up_functions
from terasort_faas.terasort import run_terasort

@click.command()
@click.option("--bucket", type=str, help="S3 bucket")
@click.option("--key", type=str, help="terasort dataset")
@click.option("--map_parallelism", type=int, help="Number of map partitions")
@click.option("--runtime_name", type=str, help="Docker runtime to use")
@click.option("--reduce_parallelism", type=int, default=None, help="Number of reduce partitions")
@click.option("--runtime_memory", type=int, default=1792, help="Default runtime memory per cloud function")
@click.option("--warm_up", type=bool, default=False, help="Warm up cloud functions before execution (recommended at the very first execution of a set)")
def run(bucket, key, map_parallelism, runtime_name, reduce_parallelism, runtime_memory, warm_up):

    if warm_up:
        warm_up_functions(runtime_name, runtime_memory)

    if reduce_parallelism is None:
        reduce_parallelism = map_parallelism

    run_terasort(bucket, key, map_parallelism, reduce_parallelism, runtime_name, runtime_memory)


if __name__ == "__main__":
    run()