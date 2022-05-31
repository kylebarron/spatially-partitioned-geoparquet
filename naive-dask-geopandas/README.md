This didn't work on my local machine. Maybe on a larger machine with more memory it would be sufficient :shrug:.

```
poetry install
poetry run pip install --force-reinstall pygeos --no-binary pygeos
poetry run python shuffle.py --help
poetry run python shuffle.py -i ../data/preprocessed -o shuffled.parquet
```
