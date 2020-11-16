import logging
from collections import OrderedDict
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from decouple import config
from scrapingtool import db_manager, parse_data, cache
from scrapingtool.utils import create_logger
from scrapy import Request, Spider
from sqlalchemy import asc, desc
from sqlitedict import SqliteDict


def process_product_detail(category, base_url, num_pages, change=False, server_url='https://amazon.in', no_listing=False, detail=False, jump_page=0, subcategories=None, no_refer=False, threshold_date=None, listing_pids=None, qanda_pages=50, review_pages=500, override=False):
    global cache
    global headers, cookies
    global last_product_detail
    global cache
    global speedup
    global use_multithreading
    global cache_file, use_cache
    global USE_DB
    global pids

    cache_file = 'cache.sqlite3'

    logger = create_logger('fetch_category')
    
    for idx, product_id in enumerate(listing_pids):
        curr_page = 1
        curr_url = base_url

        factor = 0
        cooldown = False

        rescrape = 0

        try:
            if (product_id not in pids) and (cache.sismember(f"{category}_PIDS", product_id) == False):
                logger.info(f"PID {product_id} not in set")
                pids.add(product_id)
                cache.atomic_set_add(f"{category}_PIDS", product_id)
            else:
                logger.info(f"PID {product_id} in set. Skipping this product")
                obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                if obj is not None:
                    continue

            if product_id is not None:
                _date = threshold_date
                obj = db_manager.query_table(db_session, 'ProductDetails', 'one', filter_cond=({'product_id': f'{product_id}'}))
                
                recent_date = None

                if obj is not None:
                    if obj.completed == True:
                        a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                        if a is None:
                            error_logger.info(f"{idx}: Product with ID {product_id} not in ProductListing. Skipping this, as this will give an integrityerror")
                            continue
                        else:
                            recent_obj = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.duplicate_set == a.duplicate_set).order_by(desc(text('detail_completed'))).first()
                            if recent_obj is None:
                                error_logger.info(f"{idx}: Product with ID {product_id} not in duplicate set filter")
                                continue
                                                        
                            if cache.sismember("DUPLICATE_SETS", str(recent_obj.duplicate_set)):
                                error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                                continue

                            if recent_obj.duplicate_set is not None:
                                cache.sadd(f"DUPLICATE_SETS", str(recent_obj.duplicate_set))
                            
                            product_url = recent_obj.product_url
                            
                            recent_date = recent_obj.detail_completed

                        if override == False and recent_date is not None:
                            _date = recent_date
                            logger.info(f"Set date as {_date}")
                            delta = datetime.now() - _date
                            if delta.days < 6:
                                logger.info(f"Skipping this product. within the last week")
                                continue
                            
                        elif override == False and hasattr(recent_obj, 'date_completed') and recent_obj.detail_completed is not None:
                            # Go until this point only
                            _date = recent_obj.detail_completed
                            logger.info(f"Set date as {_date}")
                            delta = datetime.now() - _date
                            if delta.days < 6:
                                logger.info(f"Skipping this product. within the last week")
                                continue
                        else:
                            _date = threshold_date

                    if hasattr(obj, 'product_details') and obj.product_details in (None, {}, '{}'):
                        rescrape = 1
                        error_logger.info(f"Product ID {product_id} has NULL product_details. Scraping it again...")
                        #product_url = obj.product_url
                        if hasattr(obj, 'completed') and obj.completed is None:
                            rescrape = 2
                    else:
                        #product_url = obj.product_url
                        if _date != threshold_date:
                            rescrape = 0
                        else:
                            rescrape = 2
                            if override == True:
                                pass
                            else:
                                logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                error_logger.info(f"Product with ID {product_id} already in ProductDetails. Skipping this product")
                                continue
                else:
                    error_logger.info(f"{idx}: Product with ID {product_id} not in DB. Scraping this from scratch")
                    rescrape = 0
                    a = db_manager.query_table(db_session, 'ProductListing', 'one', filter_cond=({'product_id': f'{product_id}'}))
                    if a is None:
                        error_logger.info(f"{idx}: Product with ID {product_id} not in ProductListing. Skipping this, as this will give an integrityerror")
                        continue
                    else:
                        recent_obj = db_session.query(db_manager.ProductListing).filter(db_manager.ProductListing.duplicate_set == a.duplicate_set).order_by(desc(text('date_completed'))).first()
                        if recent_obj is None:
                            error_logger.info(f"{idx}: Product with ID {product_id} not in duplicate set filter")
                            continue
                        
                        instances = db_manager.query_table(db_session, 'ProductListing', 'all', filter_cond=({'duplicate_set': f'{a.duplicate_set}'}))
                        
                        if cache.sismember("DUPLICATE_SETS", str(recent_obj.duplicate_set)):
                            error_logger.info(f"{idx}: Product with ID {product_id} is a duplicate. Skipping this...")
                            continue

                        if recent_obj.duplicate_set is not None:
                            cache.sadd(f"DUPLICATE_SETS", str(recent_obj.duplicate_set))
                        
                        product_url = recent_obj.product_url
                        
                        recent_date = recent_obj.detail_completed

                    if override == False and recent_date is not None:
                        _date = recent_date
                        logger.info(f"Set date as {_date}")
                        delta = datetime.now() - _date
                        if delta.days < 6:
                            logger.info(f"Skipping this product. within the last week")
                            continue
                        
                    elif override == False and hasattr(recent_obj, 'detail_completed') and recent_obj.detail_completed is not None:
                        # Go until this point only
                        _date = recent_obj.detail_completed
                        logger.info(f"Set date as {_date}")
                        delta = datetime.now() - _date
                        if delta.days < 6:
                            logger.info(f"Skipping this product. within the last week")
                            continue
                    else:
                        _date = threshold_date
            
            if override == True:
                _date = threshold_date
                rescrape = 2
                logger.info(f"Overriding: Scraping FULL Details for {product_id}")

            if rescrape == 0:
                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=0)
            elif rescrape == 1:
                # Only details
                _ = scrape_product_detail(category, product_url, review_pages=0, qanda_pages=0, threshold_date=_date, listing_url=curr_url, total_ratings=0, incomplete=True)
            elif rescrape == 2:
                # Whole thing again
                _ = scrape_product_detail(category, product_url, review_pages=review_pages, qanda_pages=qanda_pages, threshold_date=_date, listing_url=curr_url, total_ratings=0, incomplete=True, jump_page=jump_page)

            idx += 1
        except Exception as ex:
            traceback.print_exc()
            if product_id is not None:
                logger.critical(f"During scraping product details for ID {product_id}, got exception: {ex}")
            else:
                logger.critical(f"Product ID is None, got exception: {ex}")

        if last_product_detail == True:
            logger.info("Completed pending products. Exiting...")
            return
        
        cooldown = False

        time.sleep(4)

        change = True

