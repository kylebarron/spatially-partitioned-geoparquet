# Manual shuffle using hilbert values

#### Compute hilbert distances of data

```bash
mkdir -p
poetry run python manual_hilbert.py \
    --input ../data/preprocessed/ \
    --output ../data/manual-hilbert-shuffle/with_hilbert_distance.parquet
```

`manual_hilbert.py` uses dask-geopandas to compute hilbert distances of each polygon in the data.

Takes ~10 minutes on my computer.

#### Shuffle data

```bash
poetry run python manual_shuffle.py \
    --input ../data/manual-hilbert-shuffle/with_hilbert_distance.parquet \
    --output ../data/manual-hilbert-shuffle/shuffled.parquet \
    --num-row-groups 2000
```

`manual_shuffle.py`:

1. Loads the `hilbert_distance` column from the data.
2. Uses numpy and `np.quantile` to create even partitions for the data.
3. For each quantile, selects the rows from the input dataset with hilbert distances between the quantile's minimum and maximum.
4. Saves this filtered row group as a new Parquet file

You might think that step 3 would be very slow, because it needs to search through the input dataset to find all the rows . However it's actually not horrible because it's able to leverage Parquet's _predicate pushdown_ functionality. Because the input data was roughly spatially sorted (as it's distributed by state), rows within a single quantile of `hilbert_distance` aren't evenly spread throughout the entire input dataset. `pyarrow` is able to only read record batches where at least one row intersects with the minimum/maximum hilbert bounds.
