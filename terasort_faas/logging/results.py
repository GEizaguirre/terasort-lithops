import yaml
import numpy as np
from terasort_faas.config import *

bold_code = "\033[1m"
reset_code = "\033[0m"

def get_total_time(fname):
    data = yaml.safe_load(open(fname, "r"))
    sort_key = [ k for k in data.keys() if k == "sort" ][0]
    return data[sort_key]["end_time"] - data[sort_key]["start_time"]

def get_exchange_time(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    exchange_start_times = [ data[k]["exchange_start"] for k in mapper_keys ]
    exchange_start = min(exchange_start_times)

    exchange_end_times = [ data[k]["exchange_end"] for k in reducer_keys ]
    exchange_end = max(exchange_end_times)

    return exchange_end - exchange_start


def get_mapper_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]

    mapper_times = [ data[k]["end_time"] - data[k]["start_time"] for k in mapper_keys ]

    return mapper_times



def get_reducer_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    reducer_times = [ data[k]["end_time"] - data[k]["start_time"] for k in reducer_keys ]

    return reducer_times


def get_scan_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]

    scan_times = [ data[k]["scan_time"] - data[k]["start_time"] for k in mapper_keys ]

    return scan_times


def get_construct_time(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]

    construct_times = [ data[k]["construct_time"] - data[k]["scan_time"] for k in mapper_keys ]

    return construct_times


def get_serialization_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]

    serialization_times = [ data[k]["exchange_start"] - data[k]["construct_time"] for k in mapper_keys ]

    return serialization_times


def get_exchange_write_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    mapper_keys = [ k for k in data.keys() if k.startswith("mapper") ]

    exchange_write_times = [ data[k]["end_time"] - data[k]["exchange_start"] for k in mapper_keys ]

    return exchange_write_times


def get_exchange_read_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    exchange_read_times = [ data[k]["exchange_end"] - data[k]["start_time"] for k in reducer_keys ]

    return exchange_read_times


def get_aggregation_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    aggregation_times = [ data[k]["aggregation_time"] - data[k]["exchange_end"] for k in reducer_keys ]

    return aggregation_times


def get_sort_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    sort_times = [ data[k]["sort_time"] - data[k]["aggregation_time"] for k in reducer_keys ]

    return sort_times

def get_write_times(fname):
    data = yaml.safe_load(open(fname, "r"))
    reducer_keys = [ k for k in data.keys() if k.startswith("reducer") ]

    write_times = [ data[k]["end_time"] - data[k]["sort_time"] for k in reducer_keys ]

    return write_times



def result_summary(fname):

    exchange_time = get_exchange_time(fname)

    mapper_times = get_mapper_times(fname)
    avg_mapper_time = np.mean(mapper_times)
    desv_mapper_time = np.std(mapper_times)

    scan_times = get_scan_times(fname)
    avg_scan_time = np.mean(scan_times)
    desv_scan_time = np.std(scan_times)

    construct_times = get_construct_time(fname)
    avg_construct_time = np.mean(construct_times)
    desv_construct_time = np.std(construct_times)

    serialization_times = get_serialization_times(fname)
    avg_serialization_time = np.mean(serialization_times)
    desv_serialization_time = np.std(serialization_times)

    exchange_write_times = get_exchange_write_times(fname)
    avg_exchange_write_time = np.mean(exchange_write_times)
    desv_exchange_write_time = np.std(exchange_write_times)

    reducer_times = get_reducer_times(fname)
    avg_reducer_time = np.mean(reducer_times)
    desv_reducer_time = np.std(reducer_times)

    exchange_read_times = get_exchange_read_times(fname)
    avg_exchange_read_time = np.mean(exchange_read_times)
    desv_exchange_read_time = np.std(exchange_read_times)

    aggregation_times = get_aggregation_times(fname)
    avg_aggregation_time = np.mean(aggregation_times)
    desv_aggregation_time = np.std(aggregation_times)

    sort_times = get_sort_times(fname)
    avg_sort_time = np.mean(sort_times)
    desv_sort_time = np.std(sort_times)

    write_times = get_write_times(fname)
    avg_write_time = np.mean(write_times)
    desv_write_time = np.std(write_times)

    total_time = get_total_time(fname)

    texts = [
        "Total time: %.2fs"%(total_time),
        "Exchange time: %.2fs"%(exchange_time),
        "Average mapper time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_mapper_time, desv_mapper_time),
        "    Average scan time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_scan_time, desv_scan_time),
        "    Average construct time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_construct_time, desv_construct_time),
        "    Average serialization time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_serialization_time, desv_serialization_time),
        "    Average exchange_write time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_exchange_write_time, desv_exchange_write_time),
        "Average reducer time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_reducer_time, desv_reducer_time),
        "    Average exchange_read time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_exchange_read_time, desv_exchange_read_time),
        "    Average aggregation time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_aggregation_time, desv_aggregation_time),
        "    Average sort time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_sort_time, desv_sort_time),
        "    Average write time: %.2f \N{PLUS-MINUS SIGN} %.2fs"%(avg_write_time, desv_write_time),
    ]
    bold_lines = [1, 2, 7]


    longest_text = max([len(text) for text in texts])


    horizontal_line = "━" * (longest_text + 2)

    fillers: list[str] = [ "".join([ " " for _ in range(longest_text - len(text)) ]) for text in texts ] 


    print("┏" + horizontal_line + "┓")

    for text_i, text in enumerate(texts):

        print_line = "┃ " + text + fillers[text_i] + " ┃"

        if text_i in bold_lines:
            print_line = bold_code + print_line + reset_code

        print(print_line)

    print("┗" + horizontal_line + "┛")


