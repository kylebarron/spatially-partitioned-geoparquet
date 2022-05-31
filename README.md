# Exploring spatially-partitioned GeoParquet

## Install

Using Poetry:

```
poetry install
```

This will install packages from the lockfile and ensure that you're using the exact same environment of packages as me.

## Spatially partitioning the data

### Preprocessing

#### Step 1: Download data

Download source files from Microsoft's website:

```bash
> mkdir -p data/source/
# With 8 threads:
> cat files.txt | xargs -n 1 -P 8 wget -q -P data/source/
```

#### Step 2: Preprocess data

The data are distributed by Microsoft in zipped GeoJSON, which is not a performant format to load. To make later steps faster, we'll convert all the input files into Parquet. This uses the `osgeo/gdal:latest` image (as of May 7, 2022) for simplicity.

```bash
cd data
mkdir -p preprocessed
for file in $(ls source/*.zip); do
  state=$(basename $file .geojson.zip)
  echo $state
  docker run --rm -it -v $(pwd):/data osgeo/gdal:latest \
    ogr2ogr \
    /data/preprocessed/$state.parquet \
    /vsizip//data/$file \
    -lco COMPRESSION=ZSTD
done
cd ..
```

Takes ~1 hour on my computer.

#### Step 3: Compute hilbert distances of data

```bash
poetry run python manual_hilbert.py \
    --input data/1_preprocessed_parquet/ \
    --output data/2_with_hilbert_distance.parquet \
    --num-row-groups 2000
```

`manual_hilbert.py` uses dask-geopandas to compute hilbert distances of each polygon in the data.

Takes ~10 minutes on my computer.

#### Step 4: Shuffle data

```bash
poetry run python manual_shuffle.py \
    --input data/2_with_hilbert_distance.parquet \
    --output data/3_shuffled.parquet \
    --num-row-groups 2000
```

`manual_shuffle.py`:

1. Loads the `hilbert_distance` column from the data.
2. Uses numpy and `np.quantile` to create even partitions for the data.
3. For each quantile, selects the rows from the input dataset with hilbert distances between the quantile's minimum and maximum.
4. Saves this filtered row group as a new Parquet file

You might think that step 3 would be very slow, because it needs to search through the input dataset to find all the rows . However it's actually not horrible because it's able to leverage Parquet's _predicate pushdown_ functionality. Because the input data was roughly spatially sorted (as it's distributed by state), rows within a single quantile of `hilbert_distance` aren't evenly spread throughout the entire input dataset. `pyarrow` is able to only read record batches where at least one row intersects with the minimum/maximum hilbert bounds.

#### Step 5: Visualize bounds

```bash
poetry run python visualize_bounds.py \
    --input data/3_shuffled.parquet
```
