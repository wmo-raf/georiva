import os
import re

from fastapi import FastAPI, Query, HTTPException
from starlette.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory

MINIO_HOST = os.getenv("MINIO_HOST", "http://georiva-minio:9000")
MINIO_BUCKET_PREFIX = os.getenv("MINIO_BUCKET_PREFIX", "georiva/processed")


def GeoRivaPathParams(
        dataset_path: str = Query(
            ...,
            description="Path relative to bucket prefix, e.g. ecmwf-ais/ecmwf-ais-temperature/temperature/2026/01/31/temperature_060000.tif"
        ),
) -> str:
    if not re.match(r"^[\w/.-]+\.tif$", dataset_path):
        raise HTTPException(status_code=400, detail=f"Invalid path: {dataset_path}")
    return f"{MINIO_HOST}/{MINIO_BUCKET_PREFIX}/{dataset_path}"


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cog = TilerFactory(path_dependency=GeoRivaPathParams)
app.include_router(cog.router, tags=["Cloud Optimized GeoTIFF"])
