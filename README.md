# MDverse data explorer 2

## Setup environment

We use [uv](https://docs.astral.sh/uv/getting-started/installation/)
to manage dependencies and the project software environment.

Clone the GitHub repository:

```sh
git clone https://github.com/MDverse/mdde2.git
cd mdde2
```

Sync dependencies:

```sh
uv sync
```

## Get data and models

```bash
cp xxx/database.db ./database.db
cp xxx/db_schema.py app/db_schema.py
```


## Launch web app

### Developpment mode

To launch the FastAPI web app, run:

```bash
uv run fastapi dev app/main.py
```

The app should be available at the following URL:

[http://127.0.0.1:8000/](http://127.0.0.1:8000/)


## Production mode

```bash
uv run uvicorn app.main:app --reload
```
