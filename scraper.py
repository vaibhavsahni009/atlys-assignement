# # BE engineer testing assigment

# You are tasked with developing a scraping tool using Python FastAPI framework to automate the information scraping process from the target [website](https://dentalstall.com/shop/). Your tool should be able to:

# 1. Scrape the product name, price, and image from each page of the catalogue (it’s not necessary to open each product card).
# Different settings can be provided as input, so your tool should be able to recognize them and work accordingly. For the current task, you can implement only two optional settings:
#     1. The first one will limit the number of pages from which we need to scrape the information (for example, `5` means that we want to scrape only products from the first 5 pages).
#     2. The secod one will provide a proxy string that tool can use for scraping
# 2. Store the scraped information in a database. For simplicity, you can store it on your PC's local storage as a JSON file in the following format:

# ```jsx
# [
# {
# "product_title":"",
# "product_price":0,
# "path_to_image":"", # path to image at your PC
# }
# ]
# ```

# However, be aware that there should be an easy way to use another storage strategy.

# 1. At the end of the scraping cycle, you need to notify designated recipients about the scraping status - send a simple message that will state how many products were scraped and updated in DB during the current session. For simplicity, you can just print this info in the console. However, be aware that there should be an easy way to use another notification strategy.

# To implement the functionality outlined in points 2 and 3, please use an object-oriented approach.

# Your task is to design and implement this tool, keeping in mind the following guidelines:

# - Ensure type validation and data integrity using appropriate methods. Remember, accurate typing is crucial for data validation and processing efficiency.
# - Consider adding a simple retry mechanism for the scraping part. For example, if a page cannot be reached because of a destination site server error, we would like to retry it in N seconds.
# - Add simple authentication to the endpoint using a static token.
# - Add a scraping results caching mechanism using your favourite in-memory DB. If the scraped product price has not changed, we don’t want to update the data of such a product in the DB.

# You are encouraged to make design decisions based on your understanding of the problem and the requirements provided, but usage of object oriented approach with abstractions will be considered as an advantage.

import os
import bs4
import requests
from bs4 import BeautifulSoup
import json
import time
from fastapi import FastAPI
from typing import Optional, Union
from fastapi.params import Query, Header



class Scraper:
    def __init__(self, proxy: Union[str, None] = None):
        """
        proxy: str or None - URL of the proxy server, or None for no proxy
        """
        self.proxy = proxy
        self.retry_delay = 5  # Retry delay in seconds
    
    def fetch_page(self, url: str, max_retries: int = 3) -> Union[str, None]:
        """
        Retrieve the HTML content of a web page
        url: str - URL of the page to fetch
        max_retries: int - Maximum number of retries to attempt
        Returns str or None if the page could not be fetched
        """
        retries = 0
        while retries < max_retries:
            try:
                if self.proxy:
                    response = requests.get(url, proxies={"http": self.proxy, "https": self.proxy})
                else:
                    response = requests.get(url)
                
                if response.status_code == 200:
                    return str(response.content)
                else:
                    print(f"Failed to fetch page: {url}. Status code: {response.status_code}.\nRetrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
            except Exception as e:
                print(f"Error fetching page: {e}")
            
            retries += 1
        
        print(f"Failed to fetch page after {max_retries} retries: {url}")
        return None
    
    def scrape_product_info(self, html_content: str) -> list[dict[str, Union[str, float]]]:
        """
        Scrape product information from a page
        html_content: str - HTML content of the page
        Returns list of products, where each product is a dict with fields:
            "product_title" (str), "image_src" (str), "product_price" (float)

        # TODO(in future scope):
        # We can do multithreading here to speed up the scraping process,
        # keep _scrape_product_info logic in a seperate function for that
        """
        # Parse the HTML content
        soup = BeautifulSoup(html_content, "html.parser")

        # Find the product listings
        products_ul = soup.find("ul", class_="products columns-4")
        if products_ul:
            # Extract information for each product
            products = []
            for li_element in products_ul.find_all("li"):
                product = self._scrape_product_info(li_element)
                products.append(product)

            return products
        return []

    def _scrape_product_info(self, li_element: bs4.element.Tag) -> dict[str, Union[str, float]]:
        """
        Extract product information from a single product listing
        li_element: bs4.element.Tag - Product listing HTML element
        Returns dict with fields:
            "product_title" (str), "image_src" (str), "product_price" (float)
        """
        product: dict[str, Union[str, float]] = {}
        
        # Extract title
        title_element = li_element.find(lambda tag: tag.name == "a" and "button" in tag.get("class", []))
        product["product_title"] = title_element.get("data-title", "").strip() if title_element else ""
        assert isinstance(product["product_title"], str)

        # Extract image source
        image_element = li_element.find("img", class_="attachment-woocommerce_thumbnail")
        product["image_src"] = image_element["src"] if image_element else ""
        assert isinstance(product["image_src"], str)

        # Extract price
        price_element = li_element.find("span", class_="price")
        if price_element:
            price_text = price_element.text.strip()
            # Check if price contains "Starting at:"
            if "Starting at:" in price_text:
                # Remove "Starting at:" prefix with rupee symbol
                price_text = price_text.replace("Starting at:", "").strip()

            # Extract only the first price (assuming it's the current price) 
            price_text = price_text.split("\u20b9")[1].strip()
            product["product_price"] = float(price_text) if price_text else 0.0
            assert isinstance(product["product_price"], float)
        else:
            product["product_price"] = 0.0
            assert isinstance(product["product_price"], float)

        return product





class LocalStorage:
    """
    Local storage class for storing scraped data as JSON
    In future scope, we might use a database instead of Locally storing JSON files.
    """
    def __init__(self, file_path: str):
        """
        Initialize LocalStorage object

        file_path: str - path to JSON file where data will be stored
        """
        self.file_path = file_path
    
    def save_to_json(self, data: list):
        """
        Save data to JSON file at self.file_path

        data: list - list of dicts with product information
        """
        assert isinstance(data, list)
        for item in data:
            assert isinstance(item, dict)
            assert {"product_title", "product_price", "path_to_image"} <= set(item.keys())
            assert isinstance(item["product_title"], str)
            assert isinstance(item["product_price"], float)
            assert isinstance(item["path_to_image"], str)
        with open(self.file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Data saved to {self.file_path}")
    
    def load_from_json(self) -> list:
        """
        Load data from JSON file at self.file_path

        Returns list of dicts with product information
        """
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
            assert isinstance(data, list)
            for item in data:
                assert isinstance(item, dict)
                assert {"product_title", "product_price", "path_to_image"} <= set(item.keys())
                assert isinstance(item["product_title"], str)
                assert isinstance(item["product_price"], float)
                assert isinstance(item["path_to_image"], str)
            return data
        except FileNotFoundError:
            print(f"No data found at {self.file_path}")
            return []
        except json.JSONDecodeError:
            print(f"Error decoding JSON data in {self.file_path}")
            return []
        except AssertionError:
            print(f"Data integrity error in {self.file_path}")
            return []

class Notifier:
    """
    Class for sending notifications. In future scope, we might use different forms of notifier
    like email, Slack, etc.
    """
    def notify(self, message: str):
        """
        Send notification

        message: str - notification message
        """
        assert isinstance(message, str)
        print(message)



class ScrapingManager:
    def __init__(self, scraper: "Scraper", storage: "LocalStorage", notifier: "Notifier"):
        """
        Initialize scraping manager

        scraper: Scraper - instance of scraper class
        storage: LocalStorage - instance of local storage class
        notifier: Notifier - instance of notifier class
        """
        assert isinstance(scraper, Scraper)
        assert isinstance(storage, LocalStorage)
        assert isinstance(notifier, Notifier)
        self.scraper = scraper
        self.storage = storage
        self.notifier = notifier
        self.data_cache: dict[str, list[float, str]] = {}
        

    
    def scrape_and_store(self, url: str, pages: int = 1):
        """
        Scrape data from target website and store it in local storage

        url: str - target website URL
        pages: int - number of pages to scrape from
        """
        self.db_cache_fetch()
        for page_num in range(1, pages + 1):
            page_url = f"{url}?page={page_num}"
            html = self.scraper.fetch_page(page_url)
            if html:
                products_info = self.scraper.scrape_product_info(html)
                self.db_cache_extend(products_info)
                self.notifier.notify(f"{len(products_info)} products scraped from page {page_num}.")
                time.sleep(1)  # Add a delay between requests
        data = self.db_cache_to_dict()
        self.storage.save_to_json(data)

    def db_cache_fetch(self):
        """
        Load data from local storage
        """
        data = self.storage.load_from_json()
        assert isinstance(data, list)
        for item in data:
            assert isinstance(item, dict)
            assert {"product_title", "product_price", "path_to_image"} <= set(item.keys())
            assert isinstance(item["product_title"], str)
            assert isinstance(item["product_price"], float)
            assert isinstance(item["path_to_image"], str)
        self.data_cache = {p["product_title"]: [p["product_price"], p["path_to_image"]] for p in data}

    def db_cache_extend(self, products: list[dict[str, any]]):
        """
        Add new products to local storage cache

        products: List[Dict[str, Any]] - list of dicts with product information
        """
        assert isinstance(products, list)
        for product in products:
            assert isinstance(product, dict)
            assert {"product_title", "product_price", "image_src"} <= set(product.keys())
            assert isinstance(product["product_title"], str)
            assert isinstance(product["product_price"], float)
            assert isinstance(product["image_src"], str)
        for product in products:
            product_title = product["product_title"]
            product_price = product["product_price"]
            product_img = product["image_src"]
            if product_title not in self.data_cache:
                product_img_path = self.download_image(product_img, product_title)
                self.data_cache[product_title] = [product_price, product_img_path]
            self.data_cache[product_title][0] = product_price

    def db_cache_to_dict(self) -> list[dict[str, any]]:
        """
        Return local storage cache in list of dicts format

        Returns list of dicts with product information
        """
        return [{"product_title": k, "product_price": v[0], "path_to_image": v[1]} for k, v in self.data_cache.items()]

            
def download_image(self, url: str, title: str) -> str:
    """
    Download image from url and save it in local storage
    Return the path of image relative to this file

    url: str - URL of image
    title: str - title of image
    """
    # Ensure that the relative directory exists
    relative_path = "product_images"
    directory = os.path.join(os.getcwd(), relative_path)
    ext = url.split(".")[-1]
    os.makedirs(directory, exist_ok=True)
    image_path = f"{relative_path}/{title}.{ext}"

    # Downloading image synchronously, could be done asynchronously for better performance
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(image_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    # Todo: Future scope: 
    # - Asynchronous downloading: Consider using asynchronous libraries like aiohttp
    #   to download images concurrently, improving performance.
    # - Offloading to another service: Instead of saving locally, consider offloading 
    #   image storage to a cloud service like AWS S3 and downloading could be offloaded to a
    #   separate service or atleast a lambda function that is benfecial if we don't need the images immedietely.
    #   This would provide scalability, durability, and easier management of images.
    # - Error handling and retries: Implement robust error handling and retries
    #   for network failures or other issues during image download.


    return image_path





app = FastAPI()

@app.get("/")
async def hello():
    return {"message": "Hello World"}


@app.get("/scrape/")
async def read_items(
    pages: int = Query(default=1, ge=1, lt=120, description="Number of pages to scrape", alias="pages"),
    proxy: Optional[str] = Query(default=None, description="Proxy server address", alias="proxy"),
    x_token: str = Header(...),
):
    """
    Scrape Dentalstall website
    """
    assert x_token == "my_static_token", "Invalid token"
    assert isinstance(pages, int), f"pages must be an integer, got {type(pages).__name__}"
    if proxy is not None:
        assert isinstance(proxy, str), f"proxy must be a string, got {type(proxy).__name__}"

    url = "https://dentalstall.com/shop/"
    scraper = Scraper(proxy=proxy)
    storage = LocalStorage("scraped_data.json")
    notifier = Notifier()
    
    scraping_manager = ScrapingManager(scraper, storage, notifier)
    scraping_manager.scrape_and_store(url, pages=pages)
    return {"message": "Scraping completed"}



