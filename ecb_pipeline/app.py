import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from starlette import status
from starlette.responses import JSONResponse


import ecb_pipeline
from ecb_pipeline.v1.ecb_api import v1

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.info(
        "Application started. You can check the documentation \
        in https://localhost:8000/docs/"
    )
    yield
    # Shut Down
    logger.warning("Application shutdown")


app = FastAPI(
    title=ecb_pipeline.__name__,
    version=ecb_pipeline.__version__,
)


app.include_router(v1)


@app.get("/health", status_code=200)
async def get_health() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content="ok",
    )


@app.get("/version", status_code=200)
async def get_version() -> dict:
    return {"version": ecb_pipeline.__version__}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000, access_log=False)


if __name__ == "__main__":
    main()
