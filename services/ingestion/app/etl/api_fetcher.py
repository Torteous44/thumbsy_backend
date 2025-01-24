# services/ingestion/app/etl/api_fetcher.py

import requests

class APIFetcher:
    @staticmethod
    def fetch_products(url: str, params: dict | None = None) -> list[dict]:
        """
        Fetch product data from a given API endpoint.
        Returns a list of product dictionaries.
        """
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            # Ensure it's a list of dicts
            if isinstance(data, dict):
                # Some APIs return a dict with a "products" key, for example
                data = data.get("products", [])
            return data
        except Exception as e:
            print(f"Error fetching products from {url}: {e}")
            return []
