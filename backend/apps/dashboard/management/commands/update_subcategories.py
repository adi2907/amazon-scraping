import sys
import random
import time
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db.models.base import ObjectDoesNotExist

from apps.dashboard.models import Productdetails, Reviews, ProductAggregate, SubcategoryMap

from datetime import datetime, timedelta
import string
import json

def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

subcategory_dict = {
    'headphones': {
        'Type': {
            'wired': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564046031&dc&fst=as%3Aoff&qid=1599294897&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_1',
            'wireless': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_n_feature_six_browse-bin%3A15564047031%7C15564048031&dc&fst=as%3Aoff&qid=1599295118&rnid=15564019031&ref=sr_nr_p_n_feature_six_browse-bin_1',
            'tws': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and any(token in product_title.lower() for token in ['tws', 'true wireless', 'true-wireless', 'truly wireless', 'truly-wireless'])) else False},
        },
        'Price': {
            "<500": 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A-50000&dc&fst=as%3Aoff&qid=1599295515&ref=sr_ex_p_n_feature_six_brow_0',
            '500-1000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A50000-100000&dc&fst=as%3Aoff&qid=1599295566&rnid=1318502031&ref=sr_nr_p_36_1',
            '1000-2000':'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A100000-200000&dc&fst=as%3Aoff&qid=1599295865&rnid=1318502031&ref=sr_nr_p_36_5',
            '2000-3000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A200000-300000&dc&fst=as%3Aoff&qid=1599295874&rnid=1318502031&ref=sr_nr_p_36_1',
            '3000-5000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A300000-500000&dc&fst=as%3Aoff&qid=1599295929&rnid=1318502031&ref=sr_nr_p_36_5',
            '>5000': 'https://www.amazon.in/s?i=electronics&bbn=1388921031&rh=n%3A976419031%2Cn%3A976420031%2Cn%3A1388921031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_72%3A1318478031%2Cp_36%3A500000-5000000&dc&fst=as%3Aoff&qid=1599296034&rnid=1318502031&ref=sr_nr_p_36_5',
        },
    },
    'smartphones': {
        'Price': {
            'budget (<10000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_36%3A1318505031&dc&qid=1604122900&rnid=1318502031&ref=sr_nr_p_36_1',
            'economy (10000-20000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_36%3A1000000-2000000&dc&qid=1604122922&rnid=1318502031&ref=sr_nr_p_36_1',
            'mid premium (20000-30000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_36%3A2000000-3000000&dc&qid=1604122997&rnid=1318502031&ref=sr_nr_p_36_1',
            'premium (>30000)': 'https://www.amazon.in/s?k=smartphone&i=electronics&rh=n%3A1805560031%2Cp_72%3A1318478031%2Cp_6%3AA14CZOWI0VEHLG%2Cp_n_availability%3A1318485031%2Cp_36%3A3000000-9000000&dc&qid=1604123115&rnid=1318502031&ref=sr_pg_1',
        },
    },
    'ceiling fan': {
        'Price': {
            'economy (<1500)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A-150000&dc&crid=1TGIH58I2LW9I&qid=1604125306&rnid=3444809031&sprefix=ceili%2Caps%2C380&ref=sr_nr_p_36_3',
            'standard (1500-2500)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A150000-250000&dc&crid=1TGIH58I2LW9I&qid=1604125324&rnid=3444809031&sprefix=ceili%2Caps%2C380&ref=sr_nr_p_36_1',
            'premium (2500-4000)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A250000-400000&dc&crid=1TGIH58I2LW9I&qid=1604125340&rnid=3444809031&sprefix=ceili%2Caps%2C380&ref=sr_nr_p_36_2',
            'luxury (>4000)': 'https://www.amazon.in/s?k=ceiling+fan&i=kitchen&rh=n%3A2083427031%2Cn%3A4369221031%2Cp_6%3AAT95IG9ONZD7S%2Cp_72%3A1318478031%2Cp_n_availability%3A1318485031%2Cp_36%3A400000-2000000&dc&crid=1TGIH58I2LW9I&qid=1604125356&rnid=3444809031&sprefix=ceili%2Caps%2C380&ref=sr_nr_p_36_2',
        },
        'Features': {
            'bldc': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and 'bldc' in product_title.lower()) else False},
            'smart': {'field': 'features', 'predicate': lambda features: True if (features is not None and any(token in str(features).lower() for token in ['remote', 'bldc', 'smart', 'iot'])) else False},
            'lights': {'field': 'product_title', 'predicate': lambda product_title: True if (product_title is not None and any(token in product_title.lower() for token in ['light', 'lights', 'decorative'])) else False},
        }
    },
    'refrigerator': {
        'Capacity': {
            '<120': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480496031&dc&qid=1599326565&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_1',
            '120-200': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480497031&dc&qid=1599327453&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '200-230': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480498031&dc&qid=1599327541&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_3',
            '230-300': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480499031&dc&qid=1599327668&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_4',
            '300-400': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480500031&dc&qid=1604128869&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_6',
            '>400': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480501031&dc&qid=1604128890&rnid=1480495031&ref=sr_nr_p_n_feature_seven_browse-bin_5',
        },
        'Door': {
            'multi door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753039031%7C2753045031&dc&qid=1599327851&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2',
            'double door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753043031&dc&qid=1599327930&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_2',
            'single door': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_thirteen_browse-bin%3A2753044031&dc&qid=1604128967&rnid=2753038031&ref=sr_nr_p_n_feature_thirteen_browse-bin_4',
        },
        'Defrost': {
            'direct cool': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_eleven_browse-bin%3A2753030031&dc&qid=1599329198&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1',
            'frost free': 'https://www.amazon.in/s?k=refrigerator&i=kitchen&rh=n%3A1380365031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_eleven_browse-bin%3A2753031031&dc&qid=1604129131&rnid=2753029031&ref=sr_nr_p_n_feature_eleven_browse-bin_1',
        },
    },
    'washing machine': {
        'Automatic': {
            'semi automatic': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_sixteen_browse-bin%3A2753056031&dc&qid=1599329355&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_2',
            'fully automatic': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_sixteen_browse-bin%3A2753055031&dc&qid=1599329326&rnid=2753054031&ref=sr_nr_p_n_feature_sixteen_browse-bin_1',
        },
        'Loading': {
            'front load': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_fifteen_browse-bin%3A2753053031&dc&qid=1599329490&rnid=2753051031&ref=sr_nr_p_n_feature_fifteen_browse-bin_1',
            'top load': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_fifteen_browse-bin%3A2753052031&dc&qid=1599329495&rnid=2753051031&ref=sr_nr_p_n_feature_fifteen_browse-bin_2',
        },
        'Capacity': {
            '<7kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480508031%7C1480509031&dc&qid=1599329576&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_2',
            '7-8kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480510031&dc&qid=1599329628&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_3',
            '>=8kg': 'https://www.amazon.in/s?k=washing+machine&i=kitchen&rh=n%3A1380369031%2Cp_72%3A1318478031%2Cp_6%3AAT95IG9ONZD7S%2Cp_n_availability%3A1318485031%2Cp_n_feature_seven_browse-bin%3A1480511031&dc&qid=1599329662&rnid=1480507031&ref=sr_nr_p_n_feature_seven_browse-bin_4',
        },
        'Price': {
            '<10000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and curr_price < 10000) else False},
            '10000-15000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 10000) and (curr_price < 15000))) else False},
            '15000-20000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 15000) and (curr_price < 20000))) else False},
            '20000-30000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and ((curr_price >= 20000) and (curr_price < 30000))) else False},
            '>30000': {'field': 'curr_price', 'predicate': lambda curr_price: True if (curr_price is not None and curr_price >= 30000) else False},
        }
    }
}

try:
    from scrapingtool.subcategories import subcategory_dict
except:
    # Not in the same package, etc. For future developers to look at. Currently, this won't override the subcategory_dict at the top of this file.
    # However, you'll probably want to make sure that this import succeeds
    pass


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
