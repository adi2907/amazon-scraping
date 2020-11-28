import copy
import csv
import datetime
import io
import itertools
import json
import os
import traceback
import uuid

from decouple import config
from django.apps import apps
from django.conf import settings
from django.contrib.auth import authenticate, login
from django.core.mail import EmailMessage, get_connection
from django.db.models import Avg, Count, F
from django.http import Http404
from django.shortcuts import get_object_or_404
from django.template.loader import render_to_string
from django.utils.encoding import force_bytes, force_text
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from rest_framework import generics, permissions, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.accounts.api import AdminAuthenticationPermission

from .models import (Dailyproductlisting, ProductAggregate, Productdetails,
                     Productlisting, Qanda, ReviewAggregate, Reviews,
                     SubcategoryMap, Sentimentanalysis, Sentimentbreakdown)
from .serializers import (DailyProductListingSerializer,
                          ProductDetailSerializer, ProductListingSerializer,
                          QandASerializer, ReviewSerializer)


class DashboardListing(APIView):
    

    def get(self, request):
        """Returns the Dashboard Listing Page
        """
        return Response("Dashboard Listing Page", status=status.HTTP_200_OK)


class DashboardProductListing(APIView):


    def get(self, request, page_no=None):
        """Lists the Product Listing in the Dashboard
        """
        queryset = Productlisting.objects.using('scraped').all()
        if page_no is None:
            page_no = 1
        if page_no <= 0:
            return Response("Page Number must be >= 1", status=status.HTTP_400_BAD_REQUEST)
        
        ITEMS_PER_PAGE = 10
        queryset = queryset[(page_no - 1) * ITEMS_PER_PAGE : (page_no) * ITEMS_PER_PAGE]
        
        serializer = ProductListingSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class DashboardDailyProductListing(APIView):


    def get(self, request, page_no=None):
        """Lists the Daily Product Listing in the Dashboard
        """
        query_params = request.query_params
        
        if query_params in ({}, None):
            queryset = Dailyproductlisting.objects.using('scraped').all()
        else:
            if 'category' in query_params:
                queryset = Dailyproductlisting.objects.using('scraped').filter(category=query_params['category'])
            elif 'product_id' in query_params:
                queryset = Dailyproductlisting.objects.using('scraped').filter(product_id=query_params['product_id'])
        
        if page_no is None:
            page_no = 1
        if page_no <= 0:
            return Response("Page Number must be >= 1", status=status.HTTP_400_BAD_REQUEST)
        
        ITEMS_PER_PAGE = 10
        queryset = queryset[(page_no - 1) * ITEMS_PER_PAGE : (page_no) * ITEMS_PER_PAGE]
        
        serializer = DailyProductListingSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class DashboardReviews(APIView):


    def get(self, request, product_id=None, page_no=None):
        if request.query_params == {}:
            return Response("Need to specify query params", status=status.HTTP_400_BAD_REQUEST)
        
        if 'type' not in request.query_params or request.query_params['type'] not in ('positive', 'negative',):
            return Response('Need to specify "type" = ("positive"/"negative") query parameter', status=status.HTTP_400_BAD_REQUEST)

        if product_id is None:
            return Response("product_id cannot be null", status=status.HTTP_400_BAD_REQUEST)
        
        review_type = request.query_params['type']

        if page_no is None:
            page_no = 1
        if page_no <= 0:
            return Response("Page Number must be >= 1", status=status.HTTP_400_BAD_REQUEST)

        if review_type == 'positive':
            # Positive Reviews
            threshold = 3.0
            if product_id == 'all':
                queryset = Reviews.objects.using('scraped').filter(rating__isnull=False, rating__gte=threshold)
            else:
                queryset = Reviews.objects.using('scraped').filter(product_id=product_id, rating__isnull=False, rating__gte=threshold)
            
            ITEMS_PER_PAGE = 10
            queryset = queryset[(page_no - 1) * ITEMS_PER_PAGE : (page_no) * ITEMS_PER_PAGE]
            
            serializer = ReviewSerializer(queryset, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        elif review_type == 'negative':
            # Negative Reviews
            threshold = 3.0
            if product_id == 'all':
                queryset = Reviews.objects.using('scraped').filter(rating__isnull=False, rating__lt=threshold)
            else:
                queryset = Reviews.objects.using('scraped').filter(product_id=product_id, rating__isnull=False, rating__lt=threshold)

            ITEMS_PER_PAGE = 10
            queryset = queryset[(page_no - 1) * ITEMS_PER_PAGE : (page_no) * ITEMS_PER_PAGE]
            
            serializer = ReviewSerializer(queryset, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response("Invalid Request", status=status.HTTP_400_BAD_REQUEST)


class DashboardQandA(APIView):


    def get(self, request, product_id=None, page_no=None):
        """Lists the QandAs in the Dashboard
        """
        if product_id is None:
            return Response("product_id cannot be null", status=status.HTTP_400_BAD_REQUEST)
        
        if page_no is None:
            page_no = 1
        if page_no <= 0:
            return Response("Page Number must be >= 1", status=status.HTTP_400_BAD_REQUEST)

        queryset = Qanda.objects.using('scraped').filter(product_id=product_id)
        if queryset.count() == 0:
            return Response(f"No QandA exists for this product - {product_id}", status=status.HTTP_404_NOT_FOUND)
        
        ITEMS_PER_PAGE = 10
        queryset = queryset[(page_no - 1) * ITEMS_PER_PAGE : (page_no) * ITEMS_PER_PAGE]
        
        serializer = QandASerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class BrandListAPI(APIView):

    #permission_classes = [IsAuthenticated, AdminAuthenticationPermission]

    def get(self, request, category):
        query_params = request.query_params
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
            agg = agg.values_list('brand', flat=True).order_by('pk').distinct().order_by()
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False).order_by('pk').values_list('brand', flat=True).distinct().order_by()

        return Response(agg, status=status.HTTP_200_OK)


class ModelListAPI(APIView):

    def get(self, request, category, brand):
        query_params = request.query_params
        
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(category=category, brand__iexact=brand, model__isnull=False, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__iexact=brand, category=category, model__isnull=False, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
            agg = agg.values_list('short_title', flat=True).distinct().order_by()
            #agg = agg.values_list('model', flat=True).distinct().order_by()
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__iexact=brand, model__isnull=False).values_list('short_title', flat=True).distinct().order_by()
            #agg = ProductAggregate.objects.filter(category=category, brand__iexact=brand, model__isnull=False).values_list('model', flat=True).distinct().order_by()

        return Response(agg, status=status.HTTP_200_OK)


class BrandandModelListAPI(APIView):

    def get(self, request, category):
        query_params = request.query_params
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        subcategories = None
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
            agg = agg.values('brand', 'model', 'num_reviews', 'short_title').order_by('pk').distinct().order_by()
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False).order_by('pk').values('brand', 'model', 'short_title', 'num_reviews').distinct().order_by('-num_reviews')

        results = {}
        for item in agg:
            model = item['short_title']
            brand = item['brand']
            if brand not in results:
                results[brand] = set()            
            results[brand].add(model)
        
        for brand in results:
            results[brand] = list(results[brand])
        
        return Response(results, status=status.HTTP_200_OK)


class ReviewBreakDownAPI(APIView):

    def get(self, request, category):

        query_params = request.query_params

        if 'model' not in query_params:
            model = None
        else:
            model = query_params['model']
        
        if 'product_id' not in query_params:
            product_id = None
        else:
            product_id = query_params['product_id']

        if product_id is None and model is None:
            return Response(f"Atleast one of `model`/`product_id` needs to be sent in query params", status=status.HTTP_400_BAD_REQUEST)
        
        if product_id is not None:
            instance = ProductAggregate.objects.filter(category=category, product_id=f'{product_id}').order_by('-num_reviews').first()
            if instance is None:
                return Response(f"No such product_id - {product_id}", status=status.HTTP_404_NOT_FOUND)
        else:
            instance = ProductAggregate.objects.filter(category=category, short_title=f'{model}').order_by('-num_reviews').first()
            if instance is None:
                return Response(f"No such model - {model}", status=status.HTTP_404_NOT_FOUND)
        
        return Response({'product_title': instance.product_title, 'model': model, 'brand': instance.brand, 'sentiments': json.loads(instance.sentiments)}, status=status.HTTP_200_OK)


class GetFeaturesAPI(APIView):

    def get(self, request, category):
        
        PARAMETERS_FILE = os.path.join(os.getcwd(), f"parameters.csv")

        if not os.path.exists(PARAMETERS_FILE):
            return Response("No parameters file found.", status=status.HTTP_404_NOT_FOUND)

        # Read parameters file
        with open(PARAMETERS_FILE) as f:
            reader=csv.reader(f)
            data = list(reader)
        
        features = []
        
        for row in data:
            flag = False
            if row != [] and row[0] == category:
                flag = True
            if flag:
                feature = row[1]
                aspects = {feature: []}
                for keyword in row[2:]:
                    if keyword not in ['', None]:
                        aspects[feature].append(keyword)
                features.append(aspects)
        
        return Response(features, status=status.HTTP_200_OK)


class SentimentReviewsAPI(APIView):

    @staticmethod
    def get_range_dataframe(indexed_df, product_id, feature, sentiment_type='pos'):
        if sentiment_type == 'pos':
            return indexed_df[((indexed_df['product_id'] == product_id) & (indexed_df[feature] > 0))]['id']
        elif sentiment_type == 'neg':
            return indexed_df[((indexed_df['product_id'] == product_id) & (indexed_df[feature] < 0))]['id']
        else:
            return None
    

    @staticmethod
    def get_review_ids(product_id, feature, sentiment_type='pos'):
        queryset = Sentimentanalysis.objects.using('scraped').filter(product_id=product_id)
        if sentiment_type == 'pos':
            queryset = queryset.filter(positive_sentiments__iregex=f".*'{feature}'.*")
            return queryset.values_list('id', flat=True)
        elif sentiment_type == 'neg':
            queryset = queryset.filter(negative_sentiments__iregex=f".*'{feature}'.*")
            return queryset.values_list('id', flat=True)
        else:
            return None


    def get(self, request, category):

        query_params = request.query_params

        if 'model' not in query_params:
            model = None
        else:
            model = query_params['model']
        
        if 'product_id' not in query_params:
            product_id = None
        else:
            product_id = query_params['product_id']
        
        if product_id is None and model is None:
            return Response(f"Atleast one of `model`/`product_id` needs to be sent in query params", status=status.HTTP_400_BAD_REQUEST)
        
        if 'feature' not in query_params:
            return Response(f"`feature` needs to be sent in query params", status=status.HTTP_400_BAD_REQUEST)
        
        feature = query_params['feature']

        if 'sentiment_type' not in query_params:
            sentiment_type = 'pos'
        else:
            sentiment_type = query_params['sentiment_type']
        
        if sentiment_type not in ('pos', 'neg',):
            return Response(f"Sentiment Type can only be `pos` or `neg`", status=status.HTTP_400_BAD_REQUEST)
        
        if product_id is not None:
            instance = ProductAggregate.objects.filter(category=category, product_id=f'{product_id}').order_by('-num_reviews').first()
            if instance is None:
                return Response(f"No such Product ID - {product_id}", status=status.HTTP_404_NOT_FOUND)
        else:
            instance = ProductAggregate.objects.filter(category=category, short_title=f'{model}').order_by('-num_reviews').first()
            if instance is None:
                return Response(f"No such model - {model}", status=status.HTTP_404_NOT_FOUND)
        
        review_ids = SentimentReviewsAPI.get_review_ids(instance.product_id, feature, sentiment_type)
        #review_ids = SentimentReviewsAPI.get_range_dataframe(REVIEW_DATAFRAME, instance.product_id, feature, sentiment_type)
        
        if review_ids is None:
            return Response([], status=status.HTTP_200_OK)
        
        # Fetch reviews from Scraped DB
        queryset = Reviews.objects.using('scraped').filter(pk__in=list(review_ids)).values('title', 'body', 'review_date', 'rating',)

        return Response(queryset, status=status.HTTP_200_OK)


class RatingsoverTimeAPI(APIView):
    

    def get(self, request, category):
        # For last 3 months
        query_params = request.query_params
        
        if 'brand' not in query_params:
            return Response("Need to specify a brand", status=status.HTTP_400_BAD_REQUEST)
        
        brands = request.GET.getlist('brand')

        NUM_WEEKS = 8

        if 'weeks' not in query_params:
            pass
        else:
            NUM_WEEKS = int(query_params['weeks'])

        final_results = {}

        subcategories = None

        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        for brand in brands:
            if brand not in final_results:
                final_results[brand] = []
                agg = []
                idx = 0
                if subcategories is not None:
                    for subcategory in subcategories:
                        if idx == 0:
                            agg = ProductAggregate.objects.filter(brand__iexact=brand, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                        else:
                            queryset = ProductAggregate.objects.filter(brand__iexact=brand, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                            agg = agg.union(queryset)
                        idx += 1
                else:
                    agg = ProductAggregate.objects.filter(brand__iexact=brand, category=category)
            
            queryset = agg.values('product_title', 'product_id', 'model', 'short_title', 'duplicate_set')

            results = []
            
            duplicate_sets = set()
            short_titles = set()

            last_date = datetime.datetime.today()
            # first_date = last_date - datetime.timedelta(weeks=4*NUM_MONTHS)

            # { "brand":"apple", "model":"airpod" [ { "date":"07-28-2020", "rating":3.4 } ] }

            for item in queryset:
                product_id = item['product_id']
                model = item['model']
                product_title = item['product_title']
                short_title = item['short_title']
                duplicate_set = item['duplicate_set']
                if duplicate_set in duplicate_sets:
                    continue
                else:
                    duplicate_sets.add(duplicate_set)
                    short_titles.add(short_title)
                    result = []
                try:
                    _queryset = ReviewAggregate.objects.filter(duplicate_set=item['duplicate_set']).order_by('num_reviews')

                    review_info = {}

                    for instance in _queryset:
                        temp = json.loads(instance.review_info) if instance.review_info else {}
                        for val in temp:
                            if "rating" not in temp[val] or temp[val]['rating'] in (None, "NaN", 0,):
                                if val not in review_info:
                                    review_info[val] = temp[val]
                                continue
                            else:
                                review_info[val] = temp[val]
                        break
                                
                    #review_info = json.loads(instance.review_info) if instance.review_info else {}

                    flag = False

                    for week in range(NUM_WEEKS+1):
                        weekly_reviews = 0
                        weekly_total = 0
                        start_date = last_date - datetime.timedelta(days=week*7)
                        end_date = last_date - datetime.timedelta(days=week*7 + 7 - 1)

                        for day in range(week*7, week*7 + 7):
                            # Last 1 week
                            curr_day = last_date - datetime.timedelta(days=day)
                            prev_day = curr_day - datetime.timedelta(days=1)
                            _date = prev_day.strftime("%d/%m/%Y")

                            if _date in review_info:
                                value = review_info[_date]
                                if 'num_reviews' not in value:
                                    print(f"KeyError: {value} has no num_reviews")
                                weekly_reviews += value["num_reviews"]
                                try:
                                    if value["rating"] == 'NaN':
                                        continue
                                    weekly_total += int(round(float(value["rating"]) * float(value["num_reviews"])))
                                except Exception as ex:
                                    print(f"Error during counting reviews: {ex}")
                                #result.append({"date": _date, "rating": value["rating"], "num_reviews": value["num_reviews"]})
                        try:
                            avg_reviews = weekly_total / weekly_reviews
                        except ZeroDivisionError:
                            avg_reviews = "NaN"
                         
                        result.append({"start_date": start_date.strftime("%d/%m/%Y"), "end_date": end_date.strftime("%d/%m/%Y"), "rating": avg_reviews, "num_reviews": weekly_reviews})
                
                    results.append({"product_title": product_title, "model": short_title, "ratings": result, "duplicate_set": duplicate_set})
                except Exception as ex:
                    traceback.print_exc()
                    print(ex)
                    results.append({"product_title": product_title, "model": short_title, "ratings": result, "duplicate_set": duplicate_set})
                    continue
            
            final_results[brand] = results
        
        return Response(final_results, status=status.HTTP_200_OK)


class AspectBasedRatingAPI(APIView):


    # { { "brand":"apple", "model":"airpod", "aspect_rating": { "build":3.4, "sound quality":4.2, "noise cancellation":3.7 } },  { "brand":"samsung", "model":"triple m", "aspect_rating": { "build":3.9, "sound quality":4.8, "noise cancellation":4.5 } }, }

    def get(self, request, category):
        query_params = request.query_params
        
        if 'brand' not in query_params:
            return Response("Need to specify a brand", status=status.HTTP_400_BAD_REQUEST)
        
        brands = request.GET.getlist('brand')

        final_results = {}

        subcategories = None

        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        for brand in brands:
            agg = []
            idx = 0
            queryset = ProductAggregate.objects.filter(category=category, brand__iexact=brand).values('product_title', 'model', 'featurewise_reviews', 'short_title', 'duplicate_set')
            results = []
            duplicate_sets = set()
            short_titles = set()

            if subcategories is not None:
                for subcategory in subcategories:
                    if idx == 0:
                        agg = ProductAggregate.objects.filter(brand__iexact=brand, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    else:
                        queryset = ProductAggregate.objects.filter(brand__iexact=brand, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                        agg = agg.union(queryset)
                    idx += 1
            else:
                agg = ProductAggregate.objects.filter(brand__iexact=brand, category=category)
            
            queryset = agg.values('product_title', 'featurewise_reviews', 'model', 'short_title', 'duplicate_set')

            for item in queryset:
                if item['duplicate_set'] in duplicate_sets:
                    continue
                if item['featurewise_reviews'] is None:
                    item['featurewise_reviews'] = json.dumps({})
                duplicate_sets.add(item['duplicate_set'])
                short_titles.add(item['short_title'])
                #models.add(item['short_title'])
                _item = {"product_title": item['product_title'], "model": item['short_title'], "aspect_rating": {**(json.loads(item['featurewise_reviews']))}}
                results.append(_item)
            final_results[brand] = results
        
        return Response(final_results, status=status.HTTP_200_OK)


class BrandMarketShare(APIView):

    def get(self, request, category, max_products=10, period=None):
        # Get the brand Market Share
        # Output: brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        if max_products <= 0:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)

        # Count number of reviews until the last N months
        if period is None:
            period = 1
        else:
            if period not in range(1, 12+1):
                return Response("Period must be one of 1 to 12", status=status.HTTP_400_BAD_REQUEST)
        
        query_params = request.query_params
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        subcategories = None

        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(brand__isnull=False, model__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__isnull=False, model__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False)
        
        queryset = agg.values('product_title', 'brand', 'model', 'product_id', 'review_info', 'short_title', 'duplicate_set').order_by('-review_info').distinct()


        brands = dict()
        results = []
        duplicate_sets = set()
        short_titles = set()

        curr = 0

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            #result['model'] = item['model']
            result['model'] = item['short_title']
            result['product_title'] = item['product_title']

            if item['duplicate_set'] in duplicate_sets:
                continue
            else:
                if item['duplicate_set'] not in duplicate_sets:
                    duplicate_sets.add(item['duplicate_set'])
                if item['short_title'] not in short_titles:
                    short_titles.add(item['short_title'])

            if result['brand'] not in brands:
                brands[result['brand']] = dict()
                brands[result['brand']]['product_title'] = result['product_title']
                brands[result['brand']]['model'] = result['model']
                brands[result['brand']]['reviews'] = 0
                curr += 1
            # Get Num reviews
            num_reviews = json.loads(item['review_info'])[str(period)]
            
            if brands[result['brand']]['reviews'] != num_reviews:
                brands[result['brand']]['reviews'] += num_reviews
            
        
        for brand in brands:
            results.append({'brand': brand, 'num_reviews': brands[brand]['reviews']})

        return Response(sorted(results, key=lambda x: -x['num_reviews'])[:max_products], status=status.HTTP_200_OK)


class CummulativeModelMarketShare(APIView):


    def get(self, request, category, max_products=10, period=None):
        # Get the category Market Share
        # Output: brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        if max_products <= 0:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)

        # Count number of reviews until the last N months
        if period is None:
            period = 1
        else:
            if period not in range(1, 12+1):
                return Response("Period must be one of 1 to 12", status=status.HTTP_400_BAD_REQUEST)
        
        query_params = request.query_params
        subcategories = None
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        if 'brand' not in query_params:
            _brand = None
        else:
            _brand = query_params['brand']
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            print("Flag is false")
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        print("Exc cat mar share")
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(brand__isnull=False, model__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__isnull=False, model__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False)
        
        if _brand is not None:
            agg = agg.filter(brand=_brand)

        queryset = agg.values('product_title', 'brand', 'model', 'product_id', 'review_info', 'short_title', 'duplicate_set').order_by('-review_info').distinct()

        models = dict()
        dict_results = dict()
        results = []
        duplicate_sets = set()
        short_titles = set()

        curr = 0

        for item in queryset:
            duplicate_set = item['duplicate_set']
            short_title = item['short_title']
            if duplicate_set in duplicate_sets or short_title in short_titles:
                continue
            else:
                duplicate_sets.add(duplicate_set)

            result = {}
            
            result['brand'] = item['brand']
            result['model'] = item['short_title']
            result['product_title'] = item['product_title']
            result['num_reviews'] = 0

            # Get Num reviews
            num_reviews = json.loads(item['review_info'])[str(period)]

            result['num_reviews'] = num_reviews

            dict_results[item['product_id']] = result
        
        for pid in dict_results:
            results.append({'product_title': dict_results[pid]['product_title'], 'model': dict_results[pid]['model'], 'brand': dict_results[pid]['brand'], 'num_reviews': dict_results[pid]['num_reviews']})

        return Response(sorted(results, key=lambda x: -x['num_reviews'])[:max_products], status=status.HTTP_200_OK)


class FetchSubcategories(APIView):

    def get(self, request, category):
        result = {}

        if 'subcategory' in request.query_params:
            subcategory = request.query_params['subcategory']
        else:
            subcategory = None
        
        qs = SubcategoryMap.objects.filter(category=category)
        if qs.exists():
            instance = qs.first()
            if subcategory is not None:
                result[subcategory] = []
                subcategory_list = json.loads(instance.subcategory_map)                
                for _subcategory in subcategory_list:
                    if _subcategory == subcategory:
                        for item in subcategory_list[_subcategory]:
                            result[subcategory].append(item)
            else:
                subcategory_map = json.loads(instance.subcategory_map)
                for subcategory in subcategory_map:
                    if subcategory not in result:
                        result[subcategory] = []
                    for item in subcategory_map[subcategory]:
                        result[subcategory].append(item)
        
        return Response(result, status=status.HTTP_200_OK)

    
    def post(self, request, category):
        # Get the subcategory Market Share
        # Output: brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        try:
            if 'max_products' in request.data:
                max_products = int(request.data['max_products'])
                assert max_products > 0
            else:
                try:
                    temp = request.POST.get('max_products')
                    max_products = int(temp)
                except:
                    max_products = 10
        except:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)
        
        if 'period' in request.data:
            period = request.data['period']
            try:
                period = int(period)
            except:
                period = 1
        else:
            try:
                temp = int(request.POST.get('period'))
                period = temp
            except:
                period = 1

        if period not in range(1, 12+1):
            return Response("Period must be one of 1 to 12", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        if ('subcategories' not in request.data):
            subcategories = request.POST.get('subcategories')
            #subcategories = request.POST.getlist('subcategories[]')
            if subcategories in (None, []):
                return Response("subcategories must be in request data", status=status.HTTP_400_BAD_REQUEST)
        else:
            subcategories = (request.data['subcategories'])
        
        #subcategories = request.POST.getlist('subcategories[]')

        print(subcategories)

        print(f"Max products = {max_products}, period = {period}")

        _subcategories = SubcategoryMap.objects.filter(category=category)

        if _subcategories:
            try:
                if subcategories == "all":
                    _subcategories = list(itertools.chain(*(json.loads(_subcategories.first().subcategory_map).values())))
                else:
                    _subcategories = json.loads(_subcategories.first().subcategory_map)[subcategories]
                subcategories = _subcategories
            except:
                try:
                    _map = json.loads(_subcategories.first().subcategory_map)
                    flag = False
                    for name in _map:
                        for subc in _map[name]:
                            if subc == subcategories:
                                subcategories = [subcategories]
                                flag = True
                    if flag == False:
                        return Response(f"subcategory {subcategories} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                except:
                    return Response(f"subcategory {subcategories} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response({}, status=status.HTTP_200_OK)
        
        print(subcategories)

        #if not isinstance(subcategories, list):
        #    return Response("subcategories must be a list", status=status.HTTP_400_BAD_REQUEST)

        results = {}
        duplicate_sets = set()
        short_titles = set()

        for subcategory in subcategories:
            queryset = ProductAggregate.objects.filter(category=category, model__isnull=False, brand__isnull=False, subcategories__iregex=f'.*"{subcategory}".*').values('product_title', 'brand', 'model', 'product_id', 'review_info', 'subcategories', 'short_title', 'duplicate_set').order_by('-model').distinct()
            subcategory_results = []
            temp = {}
            brands = {}

            curr = 0

            for item in queryset:
                duplicate_set = item['duplicate_set']
                short_title = item['short_title']
                if duplicate_set in duplicate_sets:
                    continue
                else:
                    duplicate_sets.add(duplicate_set)
                result = {}
                result['brand'] = item['brand']
                result['model'] = item['short_title']
                #result['model'] = item['model']
                result['product_title'] = item['product_title']
                subcategories = [subcategory]
                #subcategories = item['subcategories']
                
                #subcategories = json.loads(subcategories) if subcategories is not None else ["all"]

                if result['brand'] not in brands:
                    brands[result['brand']] = dict()
                    brands[result['brand']]['product_title'] = result['product_title']
                    brands[result['brand']]['model'] = result['model']
                    brands[result['brand']]['reviews'] = json.loads(item['review_info'])[str(period)]
                    brands[result['brand']]['subcategories'] = subcategories
                    curr += 1
                else:
                    reviews = brands[result['brand']]['reviews']
                    new_reviews = json.loads(item['review_info'])[str(period)]
                    if (new_reviews != reviews):
                        brands[result['brand']]['reviews'] += new_reviews
            
            for brand in brands:
                for subcategory in brands[brand]['subcategories']:
                    if subcategory in results:
                        results[subcategory].append({'product_title': brands[brand]['product_title'], 'brand': brand, 'model': brands[brand]['model'], 'num_reviews': brands[brand]['reviews']})
                    else:
                        results[subcategory] = [{'product_title': brands[brand]['product_title'], 'brand': brand, 'model': brands[brand]['model'], 'num_reviews': brands[brand]['reviews']}]

                # Sort by descending order of num_reviews
                results[subcategory] = sorted(results[subcategory], key=lambda x: -x['num_reviews'])[:max_products]
        
        return Response(results, status=status.HTTP_200_OK)


class SubCategoryMarketShare(APIView):


    def get(self, request, category, subcategory, max_products=10, period=None):
        # Get the subcategory Market Share
        # Output: brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        if max_products <= 0:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)

        # Count number of reviews until the last N months
        if period is None:
            period = 1
        else:
            if period not in range(1, 12+1):
                return Response("Period must be one of 1 to 12", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        if subcategory == 'all':
            queryset = ProductAggregate.objects.filter(category=category, model__isnull=False, brand__isnull=False).values('product_title', 'brand', 'model', 'product_id', 'review_info', 'subcategories', 'short_title', 'duplicate_set').order_by('-model').distinct()
        else:
            queryset = ProductAggregate.objects.filter(category=category, model__isnull=False, brand__isnull=False, subcategories__iregex=f'.*"{subcategory}".*').values('product_title', 'brand', 'model', 'product_id', 'review_info', 'subcategories', 'short_title', 'duplicate_set').order_by('-model').distinct()

        results = {}
        subcategory_results = []
        temp = {}
        models = {}
        duplicate_sets = set()

        curr = 0

        for item in queryset:
            if item['duplicate_set'] not in duplicate_sets:
                duplicate_sets.add(item['duplicate_set'])
            else:
                continue
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['short_title']
            #result['model'] = item['model']
            result['product_title'] = item['product_title']
            result['product_id'] = item['product_id']
            subcategories = [subcategory]
            #subcategories = item['subcategories']
            
            #subcategories = json.loads(subcategories) if subcategories is not None else ["all"]

            models[result['product_id']] = dict()
            models[result['product_id']] = result['model']
            models[result['product_id']]['product_title'] = result['product_title']
            models[result['product_id']]['brand'] = result['brand']
            models[result['product_id']]['reviews'] = json.loads(item['review_info'])[str(period)]
            models[result['product_id']]['subcategories'] = subcategories
            curr += 1          
        
        for product_id in models:
            for subcategory in models[product_id]['subcategories']:
                if subcategory in results:
                    results[subcategory].append({'product_title': models[product_id]['product_title'], 'model': models[product_id]['model'], 'brand': models[product_id]['brand'], 'num_reviews': models[product_id]['reviews']})
                else:
                    results[subcategory] = [{'product_title': models[product_id]['product_title'], 'model': models[product_id]['model'], 'brand': models[product_id]['brand'], 'num_reviews': models[product_id]['reviews']}]

            # Sort by descending order of num_reviews
            results[subcategory] = sorted(results[subcategory], key=lambda x: -x['num_reviews'])[:max_products]
        
        return Response(results, status=status.HTTP_200_OK)

class IndividualModelMarketShare(APIView):


    def post(self, request, category):
        # Get the individual model Market Share
        # Output: subcategory, brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        if 'subcategory' not in request.data:
            subcategory = None
        
        if  'model' not in request.data:
            return Response("Need to send model", status=status.HTTP_400_BAD_REQUEST)
        
        model = request.data['model']
        
        if 'max_products' in request.data:
            max_products = request.data['max_products']
        else:
            max_products = 10
        
        if 'period' not in request.data:
            period = 6
        else:
            period = request.data['period']

        if max_products <= 0:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)

        # Count number of reviews until the last N months
        if period not in range(1, 12+1):
            return Response("Period must be one of 1 to 12", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)
        
        results = []
        num_products = 0
        models = set()
        duplicate_sets = set()
        short_titles = set()
        curr = 0

        if subcategory is not None:
            queryset = ProductAggregate.objects.filter(category=category, model=model, subcategories__iregex=f'.*"{subcategory}".*').values('product_title', 'brand', 'model', 'product_id', 'review_info', 'short_title', 'duplicate_set').order_by('-product_id').distinct()
        else:
            queryset = ProductAggregate.objects.filter(model=model, category=category).values('product_title', 'brand', 'model', 'product_id', 'review_info', 'short_title', 'duplicate_set').order_by('-product_id').distinct()

        for item in queryset:
            if item['duplicate_set'] in duplicate_sets:
                continue
            else:
                duplicate_sets.add(item['duplicate_set'])
                short_titles.add(item['short_title'])
            
            num_reviews = json.loads(item['review_info'])[str(period)]
            
            if item['short_title'] not in models:
                models.add(item['short_title'])
                #models.add(item['model'])
                curr += 1
            
            results.append({"subcategory": subcategory, "product_title": item['product_title'], "brand": item['brand'], "model": item['short_title'], "num_reviews": num_reviews})
            if curr == max_products:
                break
        
        return Response(results, status=status.HTTP_200_OK)


class ReviewCount(APIView):

    def get(self, request, category):
        query_params = request.query_params
        if 'subcategory' not in query_params:
            subcategory = None
        else:
            subcategory = query_params['subcategory']
        
        subcategories = None
        
        if 'period' not in query_params:
            period = None
        else:
            try:
                period = int(query_params['period'])
                assert period > 0 and period <= 12
            except:
                return Response("period query param must be an integer", status=status.HTTP_400_BAD_REQUEST)
        
        if subcategory is None:
            subcategories = None
        else:
            q = SubcategoryMap.objects.filter(category=category)
            if q:
                try:
                    subcategories = json.loads(q.first().subcategory_map)[subcategory]
                except:
                    try:
                        _map = json.loads(q.first().subcategory_map)
                        flag = False
                        for name in _map:
                            for subc in _map[name]:
                                if subc == subcategory:
                                    subcategories = [subcategory]
                                    flag = True
                        if flag == False:
                            return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)
                    except:
                        return Response(f"subcategory {subcategory} not found for category {category}", status=status.HTTP_400_BAD_REQUEST)

        agg = []
        idx = 0
        if subcategories is not None:
            for subcategory in subcategories:
                if idx == 0:
                    agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False, subcategories__iregex=f'.*"{subcategory}".*')
                else:
                    queryset = ProductAggregate.objects.filter(brand__isnull=False, category=category, subcategories__iregex=f'.*"{subcategory}".*')
                    agg = agg.union(queryset)
                idx += 1
            agg = agg.values('brand', 'model', 'num_reviews', 'short_title', 'duplicate_set').order_by('pk').distinct().order_by()
        else:
            agg = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False).order_by('pk').values('brand', 'model', 'short_title', 'num_reviews', 'duplicate_set').distinct().order_by('-num_reviews')

        duplicate_sets = set()
        short_titles = set()
        
        total_reviews = 0
        
        for item in agg:
            if item['duplicate_set'] in duplicate_sets:
                continue
            # Get from August also
            try:
                instance = ProductAggregate.objects.filter(duplicate_set=item['duplicate_set']).order_by('-num_reviews').distinct().first()
                if not instance:
                    continue
                
                info = instance.total_reviews
                if instance.total_reviews is None:
                    info = None
                else:
                    info = json.loads(instance.total_reviews)
                
                if period is None:
                    if info is None:
                        total_reviews += 0
                    else:
                        for _period in info:
                            total_reviews += info[_period]
                else:
                    if info is None:
                        total_reviews += 0
                    else:
                        if str(period) in info:
                            total_reviews += info[str(period)]
                        else:
                            total_reviews += 0
                
            except Exception as ex:
                print(ex)
                total_reviews += 0
            
            duplicate_sets.add(item['duplicate_set'])
            short_titles.add(item['short_title'])
                
        return Response({'total_reviews': total_reviews}, status=status.HTTP_200_OK)



class SendEmailAPI(APIView):

    permission_classes = [IsAuthenticated, AdminAuthenticationPermission]

    def get(self, request, category=None):
        # First get the csv data
        fields = [field.get_attname_column()[1] for field in Productlisting._meta.fields]
        file_name = f"ProductListing"
        file_name = file_name.replace('"', r'\"')
        csvfile = io.StringIO()
        writer = csv.writer(csvfile)

        # Write the headers first
        headers = fields
        
        writer.writerow(headers)

        if category is not None:
            queryset = Productlisting.objects.using('scraped').filter(category=category)
        else:
            queryset = Productlisting.objects.using('scraped').all()
        
        # Now write the data
        for obj in queryset:
            row = [getattr(obj, field) for field in fields]
            writer.writerow(row)
        
        # Send an Email
        mail_subject = 'Your Exported Product Data'
        
        if not hasattr(request.user, 'first_name'):
            setattr(request.user, 'first_name', 'User')
        if not hasattr(request.user, 'last_name'):
            setattr(request.user, 'last_name', '')
        
        message = render_to_string('send_email.html', {
            'user': request.user,
        })
        to_email = request.user.email
        
        email = EmailMessage(mail_subject, message, to=[to_email])
        email.attach(f'{file_name}.csv', csvfile.getvalue(), 'text/csv')

        email.send()
        return Response(f"An Email has been sent to your account - {request.user.email}. Please check the attachment for details", status=status.HTTP_200_OK)
