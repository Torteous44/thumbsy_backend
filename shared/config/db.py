import os
from databases import Database
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Fetch the DATABASE_URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# Initialize the database connection
database = Database(DATABASE_URL)

# Function to connect to the database
async def connect_db():
    await database.connect()

# Function to disconnect from the database
async def disconnect_db():
    await database.disconnect()

