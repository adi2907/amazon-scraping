import sys
import random
import time
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db.models.base import ObjectDoesNotExist

from apps.dashboard.models import Productdetails, Reviews, ProductAggregate, Productlisting, Sentimentbreakdown

from datetime import datetime, timedelta
import string
import json

def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

class Command(BaseCommand):
    help = 'Populate table with info from scraped DB.'

    def handle(self, *args, **options): 
        # Filter on only non NULL completed fields
        queryset = Productdetails.objects.using('scraped').filter(date_completed__isnull=False).values('brand', 'model', 'product_id', 'num_reviews', 'subcategories', 'product_title', 'featurewise_reviews').order_by('-date_completed')

        models = dict()
        results = []

        dup_sets = set()

        for idx, item in enumerate(queryset):
            if idx % 10 == 0:
                print(f"IDX: {idx}")
            
            if idx % 1000 == 0:
                print("Sleeping...")
                time.sleep(3)
            
            if item['brand'] is not None:
                brand = item['product_title'].split(' ')[0].lower()
            else:
                brand = None
            
            model = item['model']
            product_title = item['product_title']

            product_id = item['product_id']
            listing_reviews = item['num_reviews']
            featurewise_reviews = (item['featurewise_reviews'])

            try:
                instance = Productlisting.objects.using('scraped').get(product_id=product_id)
                short_title = instance.short_title
                category = instance.category
                duplicate_set = instance.duplicate_set
            except Exception as ex:
                print(ex)
                category = None
                continue

            # TODO: Remove this in the future
            duplicate_product_ids = Productlisting.objects.using('scraped').filter(duplicate_set=duplicate_set).values_list('product_id', flat=True)

            try:
                obj = Sentimentbreakdown.objects.using('scraped').get(product_id=product_id)
                sentiments = json.loads(obj.sentiments)
            except Exception as ex:
                print(ex)
                sentiments = {}

            if duplicate_set not in dup_sets:
                dup_sets.add(duplicate_set)
            else:
                continue
            
            try:
                instance = ProductAggregate.objects.get(product_id=item['product_id'])
                review_info = json.loads(instance.review_info)
                #if 8 not in models[model]['review_info']:
                for period in range(1, 12+1):
                    if period not in review_info:
                        review_info[period] = 0
            except ProductAggregate.DoesNotExist:
               review_info = {period: 0 for period in range(1, 12+1)}
            subcategories = item['subcategories']
            
            for period in range(1, 12+1):
                # This is relatively inexpensive, so the below two lines can be commented out
                # However, if you're going to do the aggregation only once a month, then
                # You may want to un-comment the below two lines of code
                #if period in review_info or str(period) in review_info:
                #    continue

                # Get Num reviews
                last_date = datetime.now() - timedelta(days=7)
                first_date = last_date - timedelta(weeks=4*period)

                next_period = period + 1
                if next_period > 12:
                    next_period = 1

                first_date = datetime(year=2020, month=period, day=1)
                last_date = datetime(year=(2020 + (period + 1)//12), month=(next_period), day=1)
                try:
                    num_reviews = Reviews.objects.using('scraped').filter(product_id__in=duplicate_product_ids, review_date__range=[first_date, last_date], is_duplicate=False, duplicate_set=duplicate_set).count()
                except Exception as ex:
                    print(ex)
                    num_reviews = 0

                review_info[period] += num_reviews
                            
        
            qs = ProductAggregate.objects.filter(product_id=product_id)
            if qs:
                qs.update(is_duplicate=False, brand=brand, model=model, review_info=json.dumps(review_info), subcategories=subcategories, category=category, short_title=short_title, num_reviews=num_reviews, product_title=product_title, listing_reviews=listing_reviews, duplicate_set=duplicate_set, featurewise_reviews=featurewise_reviews, sentiments=json.dumps(sentiments))
            else:
                _ = ProductAggregate.objects.create(is_duplicate=False, product_id=product_id, brand=brand, model=model, review_info=json.dumps(review_info), subcategories=subcategories, category=category, short_title=short_title, num_reviews=num_reviews, product_title=product_title, listing_reviews=listing_reviews, duplicate_set=duplicate_set, featurewise_reviews=featurewise_reviews, sentiments=json.dumps(sentiments))
