# services/ingestion/app/main.py

from fastapi import FastAPI
from sqlalchemy import text
from shared.config.db import engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# Import your ingestion route
from .routes.ingest import router as ingest_router

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    try:
        # First connect as postgres user to grant privileges
        conn_string = f"dbname=thumbsy_db user=postgres host=localhost port=5432"
        conn = psycopg2.connect(conn_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            # Grant superuser privileges
            cur.execute("ALTER ROLE thumbsy_user WITH SUPERUSER;")
        conn.close()

        # Now use SQLAlchemy connection as thumbsy_user
        with engine.begin() as conn:
            # Verify connection info
            result = conn.execute(text("""
                SELECT 
                    current_user, 
                    session_user, 
                    current_database(),
                    current_schema(),
                    r.rolsuper,
                    r.rolcreatedb,
                    r.rolcreaterole,
                    has_schema_privilege('public', 'CREATE'),
                    has_schema_privilege('public', 'USAGE')
                FROM pg_roles r 
                WHERE r.rolname = current_user
            """)).fetchone()
            
            print("\nConnection Info:")
            print(f"Current user: {result[0]}")
            print(f"Session user: {result[1]}")
            print(f"Current database: {result[2]}")
            print(f"Current schema: {result[3]}")
            print(f"Is superuser: {result[4]}")
            print(f"Schema CREATE: {result[7]}")
            print(f"Schema USAGE: {result[8]}")
            
            # Create products table if it doesn't exist
            print("\nChecking table existence...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    description VARCHAR,
                    price FLOAT,
                    category VARCHAR
                );
            """))
            print("Table check/creation completed successfully")
                
    except Exception as e:
        print(f"Error during startup: {str(e)}")
        raise e

@app.get("/")
def read_root():
    return {"Hello": "World"}

# Include your router under a prefix "/ingest"
app.include_router(ingest_router, prefix="/ingest")
