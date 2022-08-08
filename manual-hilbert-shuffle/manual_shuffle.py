from pathlib import Path
from typing import List

import click
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.parquet as pq
from numpy.typing import NDArray


class PathType(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def create_hilbert_quantiles(
    dataset: pa.dataset.Dataset, num_row_groups: int
) -> NDArray[np.uint32]:
    arr = dataset.to_table(columns=["hilbert_distance"])["hilbert_distance"].to_numpy()
    quantile_groups = np.linspace(0, 1, num_row_groups + 1)
    return np.quantile(arr, quantile_groups, method="nearest")


def select(dataset: pa.dataset.Dataset, low: int, high: int) -> pa.Table:
    # NOTE: to avoid duplicating data, use strict comparison on the upper end
    # This might omit one row on the upper end?
    filter_ = (pc.field("hilbert_distance") >= low) & (
        pc.field("hilbert_distance") < high
    )
    table = dataset.to_table(filter=filter_)

    # Double checks on pyarrow's implementation :smile:
    assert (table["hilbert_distance"] >= low).all()
    assert (table["hilbert_distance"] < high).all()

    return table


@click.command()
@click.option(
    "-i",
    "--input",
    type=PathType(readable=True, dir_okay=True, file_okay=False),
    help="Path to input Parquet dataset",
)
@click.option(
    "-o",
    "--output",
    type=PathType(writable=True, dir_okay=True, file_okay=False),
    help="Path to output Parquet dataset",
)
@click.option(
    "-n",
    "--num-row-groups",
    type=int,
    default=2000,
    show_default=True,
    help="Number of row groups for output Parquet dataset",
)
def main(input: Path, output: Path, num_row_groups: int):
    output.mkdir()

    dataset = pa.dataset.parquet_dataset(input / "_metadata")
    quantiles = create_hilbert_quantiles(dataset, num_row_groups)

    metadata_collector: List[pq.FileMetaData] = []
    with click.progressbar(length=num_row_groups, label="Shuffling data") as bar:
        for low, high in zip(quantiles[:-1], quantiles[1:]):
            table = select(dataset, low, high)
            out_path = output / f"{low}.parquet"
            with pq.ParquetWriter(
                out_path,
                schema=table.schema,
                version="2.4",
                compression="zstd",
                write_statistics=["minx", "miny", "maxx", "maxy", "hilbert_distance"],
                metadata_collector=metadata_collector,
            ) as writer:
                writer.write_table(table)

            bar.update(1)

    for low, metadata in zip(quantiles[:-1], metadata_collector):
        filename = f"{low}.parquet"
        metadata.set_file_path(filename)

    full_metadata = metadata_collector[0]
    for _meta in metadata_collector[1:]:
        full_metadata.append_row_groups(_meta)

    full_metadata.write_metadata_file(output / "_metadata")


if __name__ == "__main__":
    main()
