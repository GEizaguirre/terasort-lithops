from terasort_faas.check_output import check_output
import sys

bucket = sys.argv[1]
prefix = sys.argv[2]


check_output(bucket, prefix)