# services/ingestion/app/etl/web_scraper.py

import requests
from bs4 import BeautifulSoup
import urllib.parse

class WebScraper:
    @staticmethod
    def scrape_amazon_search(query: str, pages: int = 1) -> list[str]:
        """
        1) Perform a search on Amazon for 'query' (e.g. "best wireless headphones").
        2) Parse out ASINs from up to 'pages' pages of results.
        Returns a list of unique ASINs.
        """
        found_asins = set()
        base_url = "https://www.amazon.com/s"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.5",
        }

        query_encoded = urllib.parse.quote_plus(query)

        for page in range(1, pages + 1):
            params = {"k": query_encoded, "page": page}
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                # Typical: <div data-asin="XYZ" data-component-type="s-search-result">
                search_divs = soup.find_all("div", attrs={"data-component-type": "s-search-result"})
                for div in search_divs:
                    asin = div.get("data-asin")
                    if asin and asin.strip():
                        found_asins.add(asin.strip())

            except Exception as e:
                print(f"Error scraping Amazon search (page={page}, query='{query}'): {e}")

        return list(found_asins)

    @staticmethod
    def scrape_amazon_product(url: str) -> list[dict]:
        """
        Scrape product data from an Amazon product page by direct URL.
        Returns a list with a single dict, or empty on error.

        Example usage:
            product_data = WebScraper.scrape_amazon_product("https://www.amazon.com/dp/B08JWMPVDM")
        """
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.5",
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # ---------------------
            # Extract Title
            # ---------------------
            title_elem = soup.select_one("#productTitle")
            title = title_elem.get_text(strip=True) if title_elem else "Not Found"

            # ---------------------
            # Extract Price
            # ---------------------
            possible_price_selectors = [
                "#priceblock_ourprice",
                "#priceblock_dealprice",
                "span.a-price span.a-offscreen",
            ]
            price = None
            for sel in possible_price_selectors:
                price_elem = soup.select_one(sel)
                if price_elem and price_elem.get_text(strip=True):
                    price = price_elem.get_text(strip=True)
                    break
            if not price:
                price = "Not Available"

            # ---------------------
            # Extract Category
            # ---------------------
            category = "Not Available"
            breadcrumb_elem = soup.select("#wayfinding-breadcrumbs_feature_div ul li span a")
            if breadcrumb_elem:
                categories = [b.get_text(strip=True) for b in breadcrumb_elem if b.get_text(strip=True)]
                if categories:
                    category = " > ".join(categories)

            # ---------------------
            # Extract Rating
            # ---------------------
            rating_elem = soup.select_one(".a-icon-star span.a-icon-alt, #averageCustomerReviews .a-icon-alt")
            rating = rating_elem.get_text(strip=True).split()[0] if rating_elem else "Not Available"

            # ---------------------
            # Extract Total Reviews
            # ---------------------
            reviews_elem = soup.select_one("#acrCustomerReviewText, #acrCustomerReviewLink span")
            total_reviews = reviews_elem.get_text(strip=True) if reviews_elem else "Not Available"

            product_dict = {
                "title": title,
                "price": price,
                "category": category,
                "rating": rating,
                "total_reviews": total_reviews,
            }
            return [product_dict]

        except Exception as e:
            print(f"Error scraping product data from {url}: {e}")
            return []

    @staticmethod
    def scrape_amazon_by_asin(asin: str) -> list[dict]:
        """
        Helper function to build an Amazon product URL from an ASIN,
        then call scrape_amazon_product().
        """
        product_url = f"https://www.amazon.com/dp/{asin}"
        return WebScraper.scrape_amazon_product(product_url)
