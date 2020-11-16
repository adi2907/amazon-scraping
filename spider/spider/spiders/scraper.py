import logging
from collections import OrderedDict
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from decouple import config
from scrapingtool import db_manager, parse_data
from scrapingtool.utils import create_logger, category_to_domain, domain_map
from scrapy import Request, Spider
from sqlalchemy import asc, desc
from sqlitedict import SqliteDict


class ArchiveScraper(Spider):
    name = u'archive_details_spider'
    logger = create_logger('archive_spider')

    def start_requests(self, category="all", instance_id=0, start_idx=0, end_idx=100, *args, **kwargs):
        """This is our first request to grab all the urls.
        """

        super(ArchiveScraper, self).__init__(*args, **kwargs)

        credentials = db_manager.get_credentials()
        _, SessionFactory = db_manager.connect_to_db(config('DB_NAME'), credentials)

        _info = OrderedDict()
        info = OrderedDict()

        if self.category == 'all':
            _domain_map = domain_map
        else:
            _domain_map = {'amazon.in': {self.category: ''}}
        
        for domain in domain_map:
            for category in domain_map[domain]:
                with db_manager.session_scope(SessionFactory) as session:
                    queryset = session.query(db_manager.ProductListing).filter(db_manager.ProductListing.is_active == False, db_manager.ProductListing.category == category, (db_manager.ProductListing.date_completed == None) | (db_manager.ProductListing.date_completed <= datetime.today().date() - timedelta(days=1))).order_by(asc('category')).order_by(desc('total_ratings'))
                    self.logger.info(f"Found {queryset.count()} inactive products totally")
                    for instance in queryset:
                        _info[instance.product_id] = instance.product_url
                
                for idx, pid in enumerate(_info):
                    if idx >= int(self.start_idx) and idx < int(self.end_idx):
                        pass
                    else:
                        if idx < int(self.start_idx):
                            continue
                        else:
                            break
                    info[pid] = _info[pid]
                
                for pid in info:
                    self.logger.info(f"Scraping PID {pid} at url {info[pid]}")
                    request = Request(
                        url=u'https://' + domain + info[pid],
                        callback=self.parse_response,
                    )
                    request.meta['product_id'] = pid
                    yield request

    def parse_response(self, response):
        html = response.body
        soup = BeautifulSoup(html, 'lxml')
        details = parse_data.get_product_data(soup, html=html)
        details['product_id'] = response.meta['product_id']
        yield details
