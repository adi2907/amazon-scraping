import sys
import random
import time
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db.models.base import ObjectDoesNotExist
from django.db.models import Avg, F, Count

from apps.dashboard.models import Productdetails, Reviews, ReviewAggregate, Productlisting

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
        dup_sets = set()

        queryset = Productdetails.objects.using('scraped').filter(date_completed__isnull=False).values('brand', 'model', 'product_id', 'num_reviews', 'subcategories').order_by('-date_completed')

        models = dict()
        results = []

        delta = (datetime.now() - datetime(year=2020, month=8, day=1)).days

        end_date = datetime.now()
        start_date = datetime(year=2020, month=8, day=1)

        print(f"Delta = {delta}")

        print("Sleeping....")
        time.sleep(5)

        for idx, item in enumerate(queryset):
            if idx % 10 == 0:
                print(f"IDX: {idx}")
            
            if idx % 100 == 0:
                print("Reviews Sleeping...")
                time.sleep(3)
            
            brand = item['brand']
            model = item['model']

            product_id = item['product_id']

            try:
                instance = Productlisting.objects.using('scraped').get(product_id=product_id)
                category = instance.category
                duplicate_set = instance.duplicate_set
            except Exception as ex:
                print(ex)
                category = None
                time.sleep(5)
                continue

            if duplicate_set not in dup_sets:
                dup_sets.add(duplicate_set)
            else:
                continue
            
            # TODO: Remove this kind of query in the future. Is expensive
            # I'm currently needing to query all instances where duplicate_set matches
            # and aggregate the reviews
            duplicate_product_ids = Productlisting.objects.using('scraped').filter(duplicate_set=duplicate_set).values_list('product_id', flat=True)

            row = dict()
            row['product_id'] = product_id
            row['brand'] = brand
            row['model'] = model
            subcategories = item['subcategories']

            # Condense the review statistics
            instance = ReviewAggregate.objects.filter(product_id=product_id).first()
            if instance:
                review_info = json.loads(instance.review_info)
            else:
                review_info = {}
            curr_date = start_date

            while curr_date <= end_date:
                prev_date = curr_date - timedelta(days=1)
                avg_reviews_none = 0
                
                # TODO: Change this later to index only based on the base product_id
                item_not_none = Reviews.objects.using('scraped').filter(product_id__in=duplicate_product_ids, review_date__range=[prev_date, curr_date], is_duplicate=False, duplicate_set=duplicate_set).aggregate(avg_rating=Avg('rating'), num_reviews=Count('id'))

                avg_reviews_not_none = item_not_none['avg_rating']
                if avg_reviews_not_none is None:
                    avg_rating = "NaN"
                else:
                    avg_rating = max(avg_reviews_not_none, avg_reviews_none)
                num_reviews = item_not_none['num_reviews']
                review_info[prev_date.strftime("%d/%m/%Y")] = {"rating": avg_rating, "num_reviews": num_reviews}

                curr_date += timedelta(days=1)

            row['review_info'] = review_info
            row['subcategories'] = item['subcategories']
            row['category'] = category

            qs = ReviewAggregate.objects.filter(product_id=product_id)
            if qs:
                qs.update(brand=brand, model=model, review_info=json.dumps(review_info), subcategories=subcategories, category=category, duplicate_set=duplicate_set)
            else:
                _ = ReviewAggregate.objects.create(product_id=product_id, brand=brand, model=model, review_info=json.dumps(review_info), subcategories=subcategories, category=category, duplicate_set=duplicate_set)
