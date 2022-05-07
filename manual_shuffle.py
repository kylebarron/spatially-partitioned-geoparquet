from pathlib import Path
from typing import List

import click
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.parquet as pq
from numpy.typing import NDArray


def create_hilbert_quantiles(
    dataset: pa.dataset.Dataset, n_row_groups: int
) -> NDArray[np.uint32]:
    arr = dataset.to_table(columns=["hilbert_distance"])["hilbert_distance"].to_numpy()
    quantile_groups = np.linspace(0, 1, n_row_groups + 1)
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


def main():
    n_row_groups = 2000
    input_dir = Path("with_hilbert_distance.parquet")
    output_dir = Path("shuffled.parquet")
    output_dir.mkdir()

    dataset = pa.dataset.parquet_dataset(input_dir / "_metadata")
    quantiles = create_hilbert_quantiles(dataset, n_row_groups)

    metadata_collector: List[pq.FileMetaData] = []
    with click.progressbar(length=n_row_groups, label="Shuffling data") as bar:
        for low, high in zip(quantiles[:-1], quantiles[1:]):
            table = select(dataset, low, high)
            out_path = output_dir / f"{low}.parquet"
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

    full_metadata.write_metadata_file(output_dir / "_metadata")


if __name__ == "__main__":
    main()
