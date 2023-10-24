# !/bin/bash

echo "Running terasort"

BUCKET_NAME="my-bucket"
KEY="terasort-20m-german"

python3 terasort.py --bucket  $BUCKET_NAME --key $KEY --map_parallelism 5 --runtime_name "terasort/terasort-runtime-1.0"

echo "Finished"