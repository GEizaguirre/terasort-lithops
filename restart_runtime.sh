# !/bin/bash

if [ $# -eq 0 ]; then
    runtime_name="terasort:0.2"

else
    runtime_name="$1"
fi

lithops runtime delete -b aws_lambda $runtime_name
lithops runtime build -f Dockerfile -b aws_lambda $runtime_name
lithops runtime update -b aws_lambda $runtime_name
