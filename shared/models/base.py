# shared/models/base.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData

# Define custom MetaData with schema
metadata = MetaData(schema='public')

# Create base class with schema-aware metadata
Base = declarative_base(metadata=metadata)
