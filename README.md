# Investeringsparser

To prepare the data:
1. Copy full year CSV files to the `data` directory
2. Of each file, remove all lines until the CSV header

To run:
```sh
sbt "run --csvPath data/*.csv"
```
