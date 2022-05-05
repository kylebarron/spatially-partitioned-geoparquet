## Exploring spatially-partitioned GeoParquet

Download data

```
wget -P data/ -i files.txt
```

Convert each file to Parquet as a preprocessing step to make the data easier to work with than GeoJSON.

```bash
cd data
for file in $(ls *.zip); do
  state=$(basename $file .geojson.zip)
  echo $state
  docker run --rm -it -v $(pwd):/data osgeo/gdal:latest \
    ogr2ogr \
    /data/$state.parquet \
    /vsizip//data/$file \
    -lco COMPRESSION=ZSTD
done
rm *.zip
cd ..
```
