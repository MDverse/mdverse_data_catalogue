import pathlib
import time

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from .frontend.controller import router as frontend_router
from .frontend.datasets.controller import router as frontend_datasets_router
from .frontend.file_types.controller import router as frontend_file_types_router

# ============================================================================
# FastAPI app
# ============================================================================
print(f"Running FastAPI app from: {pathlib.Path().absolute()}")

# Create FastAPI app
app = FastAPI(title="MDverse")
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")


# Middleware to measure endpoint response time.
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    end_time = time.perf_counter()
    execution_time_ms = (end_time - start_time) * 1000
    # Add the execution time to the response headers for clients to see
    response.headers["X-Process-Time-Ms"] = str(execution_time_ms)
    # Print the execution time.
    print(f"Request to '{request.url.path}' took {execution_time_ms:.4f} ms")
    return response


# Frontend endpoints
app.include_router(frontend_router)
app.include_router(frontend_datasets_router)
app.include_router(frontend_file_types_router)
