# !/bin/bash

echo "Running terasort"

BUCKET_NAME="benchmark-objects"
KEY="terasort-20m-german"
RUNTIME_NAME="terasort/terasort-lithops-1.6"

python3 terasort.py --bucket  $BUCKET_NAME --key $KEY --map_parallelism 5 --runtime_name $RUNTIME_NAME --warm_up True

echo "Finished"