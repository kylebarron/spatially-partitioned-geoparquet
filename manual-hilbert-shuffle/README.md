# Manual partitioning using hilbert values

Use the hilbert values algorithm from `dask-geopandas`, but handle the
partitioning manually instead of using dask to avoid out of memory errors.

#### Compute hilbert distances of data

The following runs `manual_hilbert.py`, which uses `dask-geopandas` to compute
hilbert distances of each polygon in the data.

```bash
mkdir -p ../data/manual-hilbert-shuffle
poetry run python manual_hilbert.py \
    --input ../data/preprocessed/ \
    --output ../data/manual-hilbert-shuffle/with_hilbert_distance.parquet
```

This takes ~10 minutes on my computer.

#### Sort data according to hilbert values

```bash
poetry run python manual_shuffle.py \
    --input ../data/manual-hilbert-shuffle/with_hilbert_distance.parquet \
    --output ../data/manual-hilbert-shuffle/partitioned.parquet \
    --num-row-groups 2000
```

The file `manual_shuffle.py` does:

1. Loads the `hilbert_distance` column (and only this column) into memory.
2. Uses numpy and `np.quantile` to create equal partitions for the data.
3. For each quantile, selects the rows from the input dataset with hilbert distances between the quantile's minimum and maximum.
4. Saves this filtered row group as a new Parquet file

Step 3 is the bottleneck here. If you have `N` total rows and `M` partitions, then in the worst case this is `O(M * N)`, because for each partition you could need to search through the entire dataset.

In practice, however, it's not _quite_ that bad because the original input data was _roughly_ spatially sorted. Since Microsoft distributed the data by state, there's some amount of correlation between state containment and the numerical hilbert value. Therefore, rows within a single output hilbert distance quantile aren't evenly spread throughout the entire input dataset. Since we initially computed hilbert values and saved them back to Parquet, step 3 here is able to leverage Parquet's _predicate pushdown_ functionality to avoid reading most of the dataset for each row group.

This took around 2 hours on my computer

```
poetry run python manual_shuffle.py  26022.65s user 2808.25s system 381% cpu 2:06:05.57 total
```

### Convert to GeoArrow

```bash
poetry run python to_geoarrow.py \
    --input ../data/manual-hilbert-shuffle/partitioned.parquet \
    --output ../data/manual-hilbert-shuffle/partitioned_geoarrow.parquet
```
