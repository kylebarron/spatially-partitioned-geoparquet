from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetDataset

from pyflatbush import Flatbush


def create_index(path: Path, nodeSize: int):
    meta = pq.read_metadata(path / "_metadata")

    index = Flatbush(numItems=meta.num_rows, nodeSize=nodeSize)

    columns = ["minx", "miny", "maxx", "maxy"]

    for i in range(meta.num_row_groups):
        table = pq.read_table(path / f"part.{i}.parquet", columns=columns)
        index.add_vectorized(
            np.copy(table["minx"].to_numpy()),
            np.copy(table["miny"].to_numpy()),
            np.copy(table["maxx"].to_numpy()),
            np.copy(table["maxy"].to_numpy()),
        )

    index.finish()

    return index


def partition_from_index(path: Path, index: Flatbush, level: int):
    level = 2
    begin, end = index._levelBounds[-(level + 2) : -(level)]
    boxes = np.array(index._boxes[begin:end]).reshape(-1, 4)
    boxes.shape

    Visualize([box(*x) for x in boxes])

    from keplergl_cli import Visualize
    from shapely.geometry import box

    index._levelBounds
    # index.
    pass


def main():
    path = Path(
        "/Users/kyle/github/mapping/building-footprints-geoparquet/data/v2/2_global_identifier.parquet"
    )
    index = create_index(path, nodeSize=300)


len(index.data)

help(Flatbush)
Flatbush()


dataset = ParquetDataset(path, use_legacy_dataset=False)
frag = dataset.fragments[0]
# frag.
dataset.schema.metadata
path = "/Users/kyle/github/mapping/building-footprints-geoparquet/data/v2/2_global_identifier.parquet/part.535.parquet"
columns = ["index", "minx", "miny", "maxx", "maxy"]
df = pd.read_parquet(path, columns=columns)

df
