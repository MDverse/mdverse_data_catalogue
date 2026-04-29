# MDverse web application and API

## Run the web app in development mode

Run the FastAPI web app in dev mode:

```bash
uv run fastapi dev webapp/app/main.py
```

The app should be available at the following URL:

[http://127.0.0.1:8000/](http://127.0.0.1:8000/)

## Run the web app in production mode

```bash
uv run uvicorn webapp.app.main:app
```

## Run the web app in production with Docker

```bash
docker compose up -d
```
