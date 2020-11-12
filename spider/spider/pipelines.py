# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from datetime import datetime, timedelta

from decouple import config
from itemadapter import ItemAdapter
from scrapingtool import db_manager, cache
from scrapy.exceptions import DropItem


class SpiderPipeline:

    def __init__(self, credentials, db_name):
        self.credentials = credentials
        self.db_name = db_name

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            credentials=db_manager.get_credentials(),
            db_name=config('DB_NAME'),
        )

    def open_spider(self, spider):
        self.engine, self.SessionFactory = db_manager.connect_to_db(self.db_name, self.credentials)

    def close_spider(self, spider):
        db_manager.close_all_db_connections(self.engine, self.SessionFactory)
        try:
            _cache = cache.Cache()
            _cache.connect('master', use_redis=True)
            _cache.set(f"SCRAPING_COMPLETED", 1, timeout=6 * 24 * 24)
        except Exception as ex:
            print(f"Error when setting cache: {ex}")

    def process_item(self, item, spider):
        if spider.name == 'archive_details_spider':
            # Archive Spider
            return self.process_archive_details(item, spider)
        return item
        
    def process_archive_details(self, item, spider):
        details = ItemAdapter(item).asdict()
        if 'product_id' not in details:
            raise DropItem(f"Missing product_id in {item}")

        product_id = details['product_id']
        
        with db_manager.session_scope(self.SessionFactory) as session:
            instance = session.query(db_manager.ProductListing).filter(db_manager.ProductListing.product_id == product_id).first()
            if instance is None:
                raise DropItem(f"For PID {product_id}, no such instance in ProductListing")

            instance.date_completed = datetime.now()
            
            required_details = ["num_reviews", "curr_price", "avg_rating"]

            for field in required_details:
                if field == "num_reviews" and details.get('num_reviews') is not None:
                    num_reviews = int(details[field].split()[0].replace(',', '').replace('.', ''))
                    if hasattr(instance, "total_ratings"):
                        setattr(instance, "total_ratings", num_reviews)
                elif field == "curr_price" and details.get('curr_price') is not None:
                    price = float(details[field].replace(',', ''))
                    if hasattr(instance, "price"):
                        setattr(instance, "price", price)
                elif field == "avg_rating" and details.get('avg_rating') is not None and isinstance(details.get('avg_rating'), float):
                    avg_rating = details['avg_rating']
                    if hasattr(instance, "avg_rating"):
                        setattr(instance, "avg_rating", avg_rating)
            
            session.add(instance)
        return item
