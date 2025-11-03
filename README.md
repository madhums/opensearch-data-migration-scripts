## Opensearch Import and Export

Import and export opensearch data from and to desired hosts. Useful for data migration between hosts/servers.

## Requirements

- Source host
- Target host
- Python 3.12 (install [pyenv](https://github.com/pyenv/pyenv) to manage python env)

## Migrate

```sh
# copy and change values
cp .env.example .env

# runs the export and creates .csv files for all the indices mentioned
python opensearch_export.py

# imports data to the target host
python opensearch_import.py
```
