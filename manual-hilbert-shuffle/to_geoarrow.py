from pathlib import Path
from typing import List

import click
import pyarrow as pa
import pyarrow.parquet as pq
import pygeos
from extension_types import construct_geometry_array


class PathType(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


def convert_to_geoarrow_encoding(
    path: Path, geometry_column_name: str = "geometry"
) -> pa.Table:
    # Load parquet
    table = pq.read_table(path)

    # Convert to geoarrow encoding
    arr = pygeos.from_wkb(table[geometry_column_name])
    geom_array = construct_geometry_array(arr)

    # Remove existing column
    geom_column_index = table.column_names.index(geometry_column_name)
    new_table = table.remove_column(geom_column_index).append_column(
        geometry_column_name, geom_array
    )

    # Drop hilbert_distance column
    hilbert_col_index = new_table.column_names.index("hilbert_distance")
    return new_table.remove_column(hilbert_col_index)


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
def main(input: Path, output: Path):
    # Read input meta
    meta = pq.read_metadata(input / "_metadata")

    output.mkdir()

    metadata_collector: List[pq.FileMetaData] = []
    file_names: List[str] = []
    with click.progressbar(
        length=meta.num_row_groups, label="Converting to geoarrow encoding"
    ) as bar:
        for input_path in input.glob("*.parquet"):
            output_path = output / input_path.name
            file_names.append(str(output_path))

            table = convert_to_geoarrow_encoding(input_path)
            with pq.ParquetWriter(
                output_path,
                schema=table.schema,
                version="2.4",
                compression="zstd",
                write_statistics=["minx", "miny", "maxx", "maxy"],
                metadata_collector=metadata_collector,
            ) as writer:
                writer.write_table(table)

            bar.update(1)

    # TODO: figure out how to set custom metadata on the _metadata file
    # It isn't possible to update the dict stored in FileMetaData.metadata
    for file_name, metadata in zip(file_names, metadata_collector):
        metadata.set_file_path(file_name)

    full_metadata = metadata_collector[0]
    for _meta in metadata_collector[1:]:
        full_metadata.append_row_groups(_meta)

    full_metadata.write_metadata_file(output / "_metadata")


if __name__ == "__main__":
    main()
