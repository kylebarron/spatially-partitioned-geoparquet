## Exploring spatially-partitioned GeoParquet

### Install

Using Poetry:

```
poetry install
```

This will install packages from the lockfile and ensure that you're using the exact same environment of packages as me.

### Spatially partitioning the data

#### Step 1: Download data

Download source files from Microsoft's website:

```bash
> mkdir -p data/orig/
> wget -P data/orig/ -i files.txt
```

#### Step 2: Preprocess data

The data are distributed by Microsoft in zipped GeoJSON, which is not a performant format to load. To make later steps faster, we'll convert all the input files into Parquet. This uses the `osgeo/gdal:latest` image (as of May 7, 2022) for simplicity.

```bash
cd data
mkdir 1_preprocessed_parquet
for file in $(ls orig/*.zip); do
  state=$(basename $file .geojson.zip)
  echo $state
  docker run --rm -it -v $(pwd):/data osgeo/gdal:latest \
    ogr2ogr \
    /data/1_preprocessed_parquet/$state.parquet \
    /vsizip//data/orig/$file \
    -lco COMPRESSION=ZSTD
done
cd ..
```

Takes ~1 hour on my computer.

#### Step 3: Compute hilbert distances of data

`manual_hilbert.py` uses dask-geopandas to compute hilbert distances of each polygon in the data.

Takes ~10 minutes on my computer.

#### Step 4: Shuffle data

`manual_shuffle.py`:

1. Loads the `hilbert_distance` column from the data.
2. Uses numpy and `np.quantile` to create even partitions for the data.
3. For each quantile, selects the rows from the input dataset with hilbert distances between the quantile's minimum and maximum.
4. Saves this filtered row group as a new Parquet file

You might think that step 3 would be very slow, because it needs to search through the input dataset to find all the rows . However it's actually not horrible because it's able to leverage Parquet's _predicate pushdown_ functionality. Because the input data was roughly spatially sorted (as it's distributed by state), rows within a single quantile of `hilbert_distance` aren't evenly spread throughout the entire input dataset. `pyarrow` is able to only read record batches where at least one row intersects with the minimum/maximum hilbert bounds.

#### Step 5: Visualize bounds
