import polars as pl
from typing import List
import gc
from terasort_faas.df.serialize import deserialize

def construct_df(key_list, value_list) -> pl.DataFrame:

    data = {
        "0": key_list,
        "1": value_list
    }

    df = pl.DataFrame(
        data
        )
    
    return df

def concat_progressive(
        data: List[bytes]) \
        -> pl.DataFrame:
    df = None

    for r_i, r in enumerate(data):

        if len(data[r_i]) > 0:

            new_chunk = deserialize(data[r_i])

            # Remove data for memory efficiency
            data[r_i] = b""
            gc.collect()

            if df is None:
                df = new_chunk
            else:
                df = pl.concat([df, new_chunk])

    return df