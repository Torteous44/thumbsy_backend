# services/ingestion/app/etl/transformer.py

class Transformer:
    @staticmethod
    def clean_product_data(products: list[dict]) -> list[dict]:
        """
        Clean or standardize the scraped products.
        Converts price and rating to appropriate types, handles missing values.
        """
        cleaned = []
        for p in products:
            # Skip products without a valid title
            if not p.get("title") or p["title"] == "Not Found":
                continue

            # Clean and convert price
            price_str = p.get("price", "Not Available")
            if price_str != "Not Available":
                # Remove currency symbols and commas
                price_str = price_str.replace("$", "").replace(",", "").strip()
                try:
                    price_float = float(price_str)
                except ValueError:
                    price_float = 0.0
            else:
                price_float = 0.0
            p["price"] = price_float

            # Clean and convert rating
            rating_str = p.get("rating", "Not Available")
            try:
                rating_float = float(rating_str)
            except ValueError:
                rating_float = 0.0
            p["rating"] = rating_float

            # Clean and convert total_reviews
            reviews_str = p.get("total_reviews", "Not Available")
            if reviews_str != "Not Available":
                # Extract digits and commas
                reviews_str = ''.join(c for c in reviews_str if c.isdigit() or c == ',')
                reviews_str = reviews_str.replace(",", "")
                try:
                    total_reviews = int(reviews_str)
                except ValueError:
                    total_reviews = 0
            else:
                total_reviews = 0
            p["total_reviews"] = total_reviews

            # Handle category
            category = p.get("category", "Unknown").strip() if p.get("category") else "Unknown"
            p["category"] = category

            cleaned.append(p)
        return cleaned
