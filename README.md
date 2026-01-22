# DBT Lab with DuckDB

## Quickstart

### Install Dependencies

- Install deps with `pip install -r requirements.txt`
- Install duckdb cli.
- Install sqlfluff and its db plugin. Also install their vscode extension.

```
pip install sqlfluff
pip install sqlfluff-templater-dbt
pip install dbt-duckdb
```

then put this into .sqlfluff
```
[sqlfluff]
templater = dbt
```

### Pre-populated DB

lab db has been pre-configured with the following data:

- raw data pre-seeded in tables under schema

### Note

- duckDB automatically prepend schema with `main_`. i.e `main_silver`.
