import warnings
from pathlib import Path
from time import time

import click
import dask.dataframe as dd
import dask_geopandas
import pygeos
from distributed import Client


class PathType(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


@click.command()
@click.option(
    "-i",
    "--input-dir",
    type=PathType(exists=True, dir_okay=True, file_okay=False),
    help="input Parquet directory",
)
@click.option("-o", "--output-file", type=PathType(file_okay=True, dir_okay=False))
def main(input_dir: Path, output_file: Path):

    input_files = list(input_dir.glob("*.parquet"))
    ddf = dd.read_parquet(input_files, columns=["geometry"], split_row_groups=True)
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
        shuffled.to_parquet(output_file)


if __name__ == "__main__":
    client = Client()
    print(f"Dashboard link: {client.dashboard_link}")
    main()
    print("done")
