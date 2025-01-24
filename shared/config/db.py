# shared/config/db.py
import os
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
from urllib.parse import quote_plus

# Database connection settings
DB_USER = os.getenv("DB_USER", "thumbsy_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Matt.4483")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "thumbsy_db")

# Include options in the connection URLs
DATABASE_URL = (
    f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print(f"Attempting to connect to database: {DATABASE_URL}")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    echo=True,
)

# Create session factory
SessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False
)

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
