# services/ingestion/app/scheduler/tasks.py

from celery import Celery
from celery.utils.log import get_task_logger
from services.ingestion.app.etl.web_scraper import WebScraper
from services.ingestion.app.etl.transformer import Transformer
from shared.config.db import engine
from sqlalchemy.orm import sessionmaker
from shared.models.product import Product
from shared.config.elasticsearch import es_client

# Initialize Celery (ensure this matches your celery.py configuration)
celery_app = Celery(
    "ingestion",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

# Configure Celery (ensure consistency with celery.py)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

logger = get_task_logger(__name__)

# Set up SQLAlchemy session
SessionLocal = sessionmaker(bind=engine)

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def ingest_amazon_search(self, query: str, pages: int = 1):
    """
    Celery task to:
    1. Scrape ASINs from Amazon search results.
    2. Scrape detailed product data for each ASIN.
    3. Transform the data.
    4. Load into PostgreSQL and Elasticsearch.
    """
    logger.info(f"Starting ingest_amazon_search task for query='{query}', pages={pages}")

    # Step 1: Scrape ASINs
    try:
        asins = WebScraper.scrape_amazon_search(query=query, pages=pages)
        if not asins:
            logger.warning(f"No ASINs found for query='{query}'")
            return {"status": "No ASINs found", "asins_scraped": 0}
    except Exception as e:
        logger.error(f"Error scraping ASINs for query='{query}': {e}")
        self.retry(exc=e)

    # Step 2: Scrape product details
    all_product_data = []
    for asin in asins:
        try:
            product_data_list = WebScraper.scrape_amazon_by_asin(asin)
            if product_data_list:
                all_product_data.extend(product_data_list)
        except Exception as e:
            logger.error(f"Error scraping product for ASIN='{asin}': {e}")
            continue  # Skip to next ASIN

    if not all_product_data:
        logger.warning("No product data scraped after ASIN scraping")
        return {"status": "No product data scraped", "products_scraped": 0}

    # Step 3: Transform data
    try:
        cleaned_products = Transformer.clean_product_data(all_product_data)
        if not cleaned_products:
            logger.warning("No valid product data after transformation")
            return {"status": "No valid product data", "products_cleaned": 0}
    except Exception as e:
        logger.error(f"Error transforming product data: {e}")
        self.retry(exc=e)

    # Step 4: Load into PostgreSQL and Elasticsearch
    try:
        db = SessionLocal()
        inserted_count = 0
        for prod in cleaned_products:
            try:
                new_product = Product(
                    name=prod["title"],
                    description=f"Scraped from Amazon: rating={prod['rating']}",
                    price=prod["price"],
                    category=prod["category"],
                    rating=prod["rating"],
                    total_reviews=prod["total_reviews"],
                )
                db.add(new_product)
                db.commit()
                db.refresh(new_product)

                # Index in Elasticsearch
                doc_body = {
                    "name": new_product.name,
                    "description": new_product.description,
                    "price": new_product.price,
                    "category": new_product.category,
                    "rating": new_product.rating,
                    "total_reviews": new_product.total_reviews,
                }
                es_client.index(index="products", id=new_product.id, body=doc_body)
                inserted_count += 1

            except Exception as e:
                db.rollback()
                logger.error(f"Error inserting/indexing product '{prod['title']}': {e}")
                continue  # Skip to next product

        db.close()
        logger.info(f"Successfully ingested {inserted_count} products for query='{query}'")
        return {"status": "Success", "products_inserted": inserted_count}

    except Exception as e:
        logger.error(f"Error loading data into DB/Elasticsearch: {e}")
        self.retry(exc=e)
