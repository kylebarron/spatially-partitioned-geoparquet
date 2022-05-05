import warnings
from time import time

import dask.dataframe as dd
import dask_geopandas
import pygeos
from distributed import Client


def main():
    path = "data/*.parquet"
    ddf = dd.read_parquet(path, columns=["geometry"], split_row_groups=True)
    ddf["geometry"] = ddf.map_partitions(
        lambda part: pygeos.from_wkb(part.geometry), meta=("geometry", object)
    )
    ddf = dask_geopandas.from_dask_dataframe(ddf)

    start_time = time()
    shuffled = ddf.spatial_shuffle(by="hilbert", npartitions=None)
    end_time = time()
    print(f"Time to shuffle data: {(end_time - start_time) / 60:.2f} minutes")

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message=".*initial implementation of Parquet.*"
        )
        shuffled.to_parquet("shuffled.parquet")


if __name__ == "__main__":
    client = Client()
    print(f"Dashboard link: {client.dashboard_link}")
    main()
    print("done")
