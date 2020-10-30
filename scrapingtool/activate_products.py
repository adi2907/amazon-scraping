import glob
import os

from bs4 import BeautifulSoup

from scrapingtool import parse_data
from scrapingtool.cache import Cache
from scrapingtool.utils import create_logger, listing_categories

logger = create_logger('activate_products')


def main():

    DUMP_DIR = os.path.join(os.getcwd(), 'dumps')

    if not os.path.exists(DUMP_DIR):
        raise ValueError("Dump Directory doesn't exist")

    cache = Cache()
    cache.connect('master', use_redis=True)

    categories = listing_categories
    for category in categories:
        old_pids = cache.smembers(f"LISTING_{category}_PIDS")

        cache.delete(f"LISTING_{category}_PIDS_BACKUP")
        
        # Take a backup
        for pid in old_pids:
            cache.sadd(f"LISTING_{category}_PIDS_BACKUP", pid)
        
        # Delete old ones
        cache.delete(f"LISTING_{category}_PIDS")

        # Add the new ones from the dump
        files = sorted(glob.glob(os.path.join(DUMP_DIR, f"listing_{category}_*")), key=lambda x: int(x.split('.')[0].split('_')[-1]))

        for _, filename in enumerate(files):
            with open(filename, 'rb') as f:
                html = f.read()

            soup = BeautifulSoup(html, 'lxml')
            product_info, _ = parse_data.get_product_info(soup)

            for title in product_info:
                product_id = product_info[title].get('product_id')
                if product_id is None:
                    continue
                cache.sadd(f"LISTING_{category}_PIDS", product_id)
        
        num_pids = len(cache.smembers(f"LISTING_{category}_PIDS"))
    
        print(f"Inserted {num_pids} PIDS for category: {category}")


if __name__ == '__main__':
    main()
