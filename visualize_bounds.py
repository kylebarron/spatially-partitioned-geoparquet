import json
from pathlib import Path
from typing import List

import geojson
import pyarrow.parquet as pq
import pygeos
from keplergl_cli import Visualize


def create_geo_feature_from_rg(rg: pq.RowGroupMetaData) -> geojson.Feature:
    total_bounds = [None, None, None, None]
    hilbert_min = None
    hilbert_max = None
    for i in range(rg.num_columns):
        column_meta = rg.column(i)

        if column_meta.path_in_schema == "minx":
            total_bounds[0] = column_meta.statistics.min
        if column_meta.path_in_schema == "miny":
            total_bounds[1] = column_meta.statistics.min
        if column_meta.path_in_schema == "maxx":
            total_bounds[2] = column_meta.statistics.max
        if column_meta.path_in_schema == "maxy":
            total_bounds[3] = column_meta.statistics.max

        if column_meta.path_in_schema == "hilbert_distance":
            hilbert_min = column_meta.statistics.min
            hilbert_max = column_meta.statistics.max

    geojson_geom = json.loads(pygeos.to_geojson(pygeos.box(*total_bounds)))
    return geojson.Feature(
        geometry=geojson_geom,
        properties={"hilbert_min": hilbert_min, "hilbert_max": hilbert_max},
    )


def main():
    input_dir = Path("shuffled.parquet")
    meta = pq.read_metadata(input_dir / "_metadata")

    features: List[geojson.Feature] = []
    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        feature = create_geo_feature_from_rg(rg)
        features.append(feature)

    fc = geojson.FeatureCollection(features)
    Visualize(fc)
