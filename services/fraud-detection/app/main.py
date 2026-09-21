from fastapi import FastAPI
from .config import settings

app = FastAPI(title=settings.service_name)

@app.get("/")
def read_root():
    return {"status": "ok", "service": settings.service_name}
