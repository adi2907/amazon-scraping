import json
import random
import string
import sys
import time
from datetime import date, datetime, timedelta

from amazonscraper.subcategories import subcategory_dict
from apps.dashboard.models import (ProductAggregate, Productdetails, Reviews,
                                   SubcategoryMap)
from django.core.management.base import BaseCommand
from django.db.models.base import ObjectDoesNotExist


class Command(BaseCommand):
    help = 'Populate table with info from scraped DB.'

    def handle(self, *args, **options):
        # This is just to make the developers understand how the category_map is structured
        category_map = {
            "headphones": {
                "Wired vs Wireless": ["wired", "wireless"],
                "True Wireless": ["tws"],
                "Price": ["<500", "500-1000", "1000-2000", "2000-3000", "3000-5000", ">5000"],
            },
            "smartphones": {
            },
            "ceiling fan": {
                "Price": ["luxury", "economy", "premium", "standard"]
            }
        }

        category_map = {}
        for category in subcategory_dict:
            category_map[category] = {}
            for field in subcategory_dict[category]:
                category_map[category][field] = []
                for subcategory in subcategory_dict[category][field]:
                    category_map[category][field].append(subcategory)
        
        for category in category_map:
            q = SubcategoryMap.objects.filter(category=category)
            if q.count() == 0:
                SubcategoryMap.objects.create(category=category, subcategory_map=json.dumps(category_map[category]))
            else:
                q.update(subcategory_map=json.dumps(category_map[category]))
        
        print("Finished assigning subcategories")
        pass
