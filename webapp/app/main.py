import pathlib

from fastapi import FastAPI
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
app.mount("/static", StaticFiles(directory="static"), name="static")

# Frontend endpoints
app.include_router(frontend_router)
app.include_router(frontend_datasets_router)
app.include_router(frontend_file_types_router)
