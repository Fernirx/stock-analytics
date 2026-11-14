import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from app.api.router import api_router
from app.core.config import settings

app = FastAPI(
    title="Stock Analytics API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/")
def root():
    if os.path.exists("web/index.html"):
        return FileResponse(
            "web/index.html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            }
        )
    return {"message": "Stock Analytics API is running"}


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "app_port": settings.APP_PORT,
    }
