# shared/models/product.py
from sqlalchemy import Column, Integer, String, Float
from .base import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    description = Column(String, nullable=True)
    price = Column(Float, nullable=True)
    category = Column(String, nullable=True)
