# TeraSort benchmark on Lithops

Implementation of the TeraSort benchmark (a distributed sort), built on Lithops. It is based on [this Spark implementation of the Terasort benchmark](https://github.com/ehiggs/spark-terasort). 

Tasks are executed on cloud functions and intermediate and output data is stored in object storage.

To generate the input dataset, we recommend the [Lithops TeraGen implementation](https://github.com/gfinol/teragen-lithops).

## Installation

### Lithops installation

You can find Lithops' documentation [here](https://lithops-cloud.github.io/). You can install Lithops simply using pip.

```bash
pip3 install lithops
```

To configure Lithops, check out its [configuration guide](https://lithops-cloud.github.io/docs/source/compute_backends.html). To say, you could set up its AWS Lambda (compute backend) and AWS S3 (storage backend) configuration.

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
- **--runtime_memory (optional)**: Provisioned runtime memory (in MB) per cloud function.




