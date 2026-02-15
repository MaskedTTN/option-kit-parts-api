from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Keep DB provider accessible so tests can monkeypatch `main.get_db`
from services.db import get_db
from routers.v1 import router as v1_router


app = FastAPI(
    title="BMW Parts API",
    description="RESTful API for BMW Parts Catalog (Normalized Schema)",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# include the v1 router which contains the endpoints
app.include_router(v1_router)


@app.get("/")
def root():
    return {
        "message": "BMW Parts API (Normalized Schema)",
        "version": "2.0.0",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
