# TeraSort benchmark on Lithops

Implementation of the TeraSort benchmark (a distributed sort), built on Lithops. It is based on [this Spark implementation of the Terasort benchmark](https://github.com/ehiggs/spark-terasort). 

Tasks are executed on cloud functions, object storage is used for reading & writing data (including the exchange of itnermediate files).

To generate the input dataset, we recommend the [Lithops TeraGen implementation](https://github.com/gfinol/teragen-lithops)! Our TeraSort implementation is designed to be used on the Ascii version of the TeraGen.

## Installation

### Lithops installation

You can find Lithops' documentation [here](https://lithops-cloud.github.io/). You can install Lithops simply using pip.

```bash
pip3 install lithops
```

To configure Lithops, check out its [configuration guide](https://lithops-cloud.github.io/docs/source/compute_backends.html). To say, you could set up its AWS Lambda (compute backend) and AWS S3 (storage backend) configuration...

### Terasort installation

Just download the code from this repository, unzip it and use pip to install the package.

```bash
unzip main.zip
cd terasort-lithops-master
pip3 install -r requirements.txt
pip3 install -e .
```

### Runtime set up

You should build and deploy a custom runtime on your FaaS service. [Lithops describes how do it](https://github.com/lithops-cloud/lithops/tree/master/runtime) for most cloud providers. Use the [Dockerfile included in this project](Dockerfile). Having docker installed and configured in your computer, it should be something as simple as:

```bash
lithops runtime build -f Dockerfile -b aws_lambda "terasort/terasort-runtime-1.0"
```

## Execution

You can execute the [terasort](terasort.py) script just after the Lithops TeraGen code. A common workflow could be the following (say that you have both TeraGen-Lithops and TeraSort-Lithops installed in the same root directory). In this case we generate and sort a dataset of 20GB.

```bash
cd teragen-lithops
BUCKET_NAME="benchmark-objects"
KEY="terasort-20"
python3 teragen.py -s 20G -b $BUCKET_NAME -k $KEY -p 200 --ascii --unique-file
python3 terasort.py --bucket $BUCKET_NAME --key $KEY --map_parallelism 50 --runtime_name "terasort/terasort-lithops-1.0"
```

## Parameters

The script takes the following parameters:
- **--bucket**: Bucket name where the dataset is stored. It will also host the intermediate files & final output of the TeraSort.
- **--key**: Name of the dataset in the object store.
- **--map_parallelism**: Number of partitions in the map stage (number of parallel map tasks/workers).
- **--runtime_name**: Name of the custom runtime.
- **--reduce_parallelism (optional)**: Number of partitions in the reduce stage (number of parallel reduce tasks/workers). If not specified, it will be equal to **map_parallelism**.
- **--runtime_memory (optional)**: Provisioned runtime memory (in MB) per cloud function. It is 1792 by default.
- **--warm_up (optional)**: Launch a warm up execution of cloud functions before execution. Recommended at the very first execution of a set.

## Result analysis

A summary of the execution metrics is printed at the end of every execution, but logs are also saved in _~/terasort-lithops/_ (defined by default at LOG_PATH in [terasort_faas.config](terasort_faas/config.py)). We provide some methods at [terasort_faas.logging.results](terasort_faas/logging/results.py) to analyse such logs. For instance, to get the exchange time of an execution you could do the following:

```python
import os
from terasort_faas.config import LOG_PATH
from terasort_faas.logging.results import get_exchange_time

fname = "2023-10-25-15-23-21.yaml"
full_fname = os.path.join(LOG_PATH,fname)
exchange_time = get_exchange_time(full_fname)
print("Exchange time: %.2f s"%(exchange_time))
```

You can also have a full summary of the execution metrics.


```python
import os
from terasort_faas.config import LOG_PATH
from terasort_faas.logging.results import result_summary

fname = "2023-10-25-15-23-21.yaml"
full_fname = os.path.join(LOG_PATH,fname)
result_summary(full_fname)
```

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Dataset name: example-ds                      ┃
┃ Dataset size: 19.07 MB                        ┃ 
┃ Map partitions: 5                             ┃
┃ Reduce partitions: 5                          ┃
┃ Total time: 10.20s                            ┃
┃ Real sort time: 6.34s                         ┃
┃ Exchange time: 4.46s                          ┃
┃ Average mapper time: 1.66 ± 0.28s             ┃
┃     Average scan time: 0.45 ± 0.18s           ┃
┃     Average construct time: 0.16 ± 0.01s      ┃
┃     Average serialization time: 0.68 ± 0.17s  ┃
┃     Average exchange_write time: 0.38 ± 0.13s ┃
┃ Average reducer time: 1.14 ± 0.10s            ┃
┃     Average exchange_read time: 0.16 ± 0.05s  ┃
┃     Average aggregation time: 0.71 ± 0.02s    ┃
┃     Average sort time: 0.05 ± 0.02s           ┃
┃     Average write time: 0.22 ± 0.06s          ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
```




Have fun sorting bytes.





