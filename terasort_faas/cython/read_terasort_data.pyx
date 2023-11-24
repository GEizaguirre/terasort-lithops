# my_module.pyx
cimport cython
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from libc.stdint cimport int64_t, uint64_t
from libc.stdio cimport *
import numpy as np


cdef extern from "stdio.h":
    FILE *fopen(const char *, const char *)
    int fclose(FILE *)
    ssize_t getline(char **, size_t *, FILE *)
    ssize_t fread(void* ptr, size_t size, size_t count, FILE* stream)
    int feof(FILE* stream)


cdef uint64_t max_value = 0x7e7e7e7e7e7e7e7e
cdef uint64_t min_value = 0x2020202020202020


cdef get_partition(char* line, uint64_t range_per_part):

    cdef char * reverse_line = <char *> malloc(sizeof(char)*8)
    reverse_line[0] = line[7]
    reverse_line[1] = line[6]
    reverse_line[2] = line[5]
    reverse_line[3] = line[4]
    reverse_line[4] = line[3]
    reverse_line[5] = line[2]
    reverse_line[6] = line[1]
    reverse_line[7] = line[0]

    cdef uint64_t prefix = 0
    memcpy(&prefix, reverse_line, 8)
    res =  int((prefix - min_value) // range_per_part)
    return res



@cython.boundscheck(False)
@cython.wraparound(False)
def read_terasort_data(data, num_partitions):

    cdef int64_t data_len = len(data)
    cdef uint64_t range_per_part = ((max_value - min_value) // num_partitions)

    cdef list key_list = []
    cdef list value_list = []
    cdef list partition_list = []

    data_byte_string = data
    cdef char * cdata = data_byte_string

    cdef size_t l = 0
    cdef char * line = <char *> malloc(sizeof(char) * 100)

    cdef uint64_t current_pos = 0
    while current_pos < data_len:
        
        memcpy(line, cdata + (current_pos * sizeof(char)), sizeof(char)*100)
        partition = get_partition(line, range_per_part)
        key = line[0:10].decode('utf-8')
        value = line[10:101].decode('utf-8')

        key_list.append(key)
        value_list.append(value)
        partition_list.append(partition)
        
        current_pos += 100


    # fclose(cfile)

    return key_list, value_list, np.array(partition_list, dtype=np.uint16)


# @cython.boundscheck(False)
# @cython.wraparound(False)
# def read_terasort_data(data, num_partitionss):

#     cdef uint64_t num_partitions = 5
#     cdef uint64_t range_per_part = ((max_value - min_value) // num_partitions)

#     print(min_value)
#     print(max_value)
#     print(range_per_part)

#     cdef bytes example_value = b"   -Y{'qbZ@"
#     cdef char * cdata = example_value
#     print(get_partition(cdata, range_per_part))



