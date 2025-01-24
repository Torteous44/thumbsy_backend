# services/auth/app/main.py
from fastapi import FastAPI
from shared.config.db import engine
from shared.models.base import Base
from .routes.auth import router as auth_router

app = FastAPI()

# Create tables on startup (for dev only; in production, use migrations)
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)  # Creates users table if not already present

app.include_router(auth_router, prefix="/auth")
