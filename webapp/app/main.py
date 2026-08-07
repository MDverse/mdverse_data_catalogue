import pathlib
import time

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from .api.v1.endpoints import router as api_v1_router
from .frontend.ai_models import router as frontend_ai_models_router
from .frontend.datasets import router as frontend_datasets_router
from .frontend.explore import router as frontend_explore_router
from .frontend.file_types import router as file_types_router
from .frontend.navigation import router as frontend_navigation_router
from .frontend.publications import router as frontend_publications_router
from .frontend.tools import router as frontend_tools_router

# ============================================================================
# FastAPI app
# ============================================================================
print(f"Running FastAPI app from: {pathlib.Path().absolute()}")

# Create FastAPI app
app = FastAPI(title="MDverse API")
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
app.include_router(frontend_navigation_router)
app.include_router(frontend_explore_router)
app.include_router(frontend_datasets_router)
app.include_router(frontend_publications_router)
app.include_router(frontend_ai_models_router)
app.include_router(frontend_tools_router)
app.include_router(file_types_router)
app.include_router(api_v1_router)
