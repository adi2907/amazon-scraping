import copy
import csv
import datetime
import io
import json
import os
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
                     Productlisting, Qanda, ReviewAggregate, Reviews)
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


class CategoryMarketShareAPI(APIView):


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
            if period not in (1, 3, 6):
                return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)
        

        # Filter on only non NULL completed fields
        #queryset = Productdetails.objects.using('scraped').filter(category=category, completed__isnull=False).order_by("-num_reviews")
        #queryset = Productdetails.objects.using('scraped').filter(completed__isnull=False, model__isnull=False, brand__isnull=False).values('brand', 'model').annotate(num_reviews=F('num_reviews')).order_by('-num_reviews')
        queryset = Productdetails.objects.using('scraped').filter(completed__isnull=False, model__isnull=False, brand__isnull=False).values('product_title', 'brand', 'model', 'product_id', 'num_reviews').order_by('-num_reviews').distinct()[:2*max_products]

        models = dict()
        results = []

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['model']
            result['product_title'] = item['product_title']
            if result['model'] not in models:
                models[result['model']] = dict()
                models[result['model']]['brand'] = result['brand']
                models[result['model']]['reviews'] = 0
                models[result['model']]['product_title'] = result['product_title']
            # Get Num reviews
            try:
                num_reviews_none = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date], page_num__isnull=False).count()
                num_reviews_not_none = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date], page_num__isnull=True).count()
                num_reviews = max(num_reviews_none, num_reviews_not_none)
                #num_reviews = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date]).count()
            except Exception as ex:
                print(ex)
                num_reviews = 0
            
            if models[result['model']]['reviews'] != num_reviews:
                models[result['model']]['reviews'] += num_reviews
        
        for model in models:
            results.append({'product_title': models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']})

        return Response(results, status=status.HTTP_200_OK)
        #return Response(queryset[:max_products], status=status.HTTP_200_OK)


class SubCategoryMarketShareAPI(APIView):


    def get(self, request, subcategory, max_products=10, period=None):
        # Get the subcategory Market Share
        # Output: brand, model, num_reviews
        # Period can be '1M', '3M', '6M'

        if max_products <= 0:
            return Response("max_products must be a positive integer", status=status.HTTP_400_BAD_REQUEST)

        # Count number of reviews until the last N months
        if period is None:
            period = 1
        else:
            if period not in (1, 3, 6):
                return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        # Filter on only non NULL completed fields
        # NOTE: Here, subcategory is assumed to be a ManyToMany field
        #queryset = Productdetails.objects.using('scraped').filter(subcategory__in=[subcategory], completed__isnull=False)
        #queryset = Productdetails.objects.using('scraped').filter(model__isnull=False, brand__isnull=False).values('brand').annotate(num_reviews=F('num_reviews')).order_by('-num_reviews')

        if subcategory == 'all':
            queryset = Productdetails.objects.using('scraped').filter(completed__isnull=False, model__isnull=False, brand__isnull=False).values('product_title', 'brand', 'model', 'product_id', 'num_reviews', 'subcategories').order_by('-num_reviews').distinct()[:max_products]
        else:
            queryset = Productdetails.objects.using('scraped').filter(completed__isnull=False, model__isnull=False, brand__isnull=False, subcategories__in=[subcategory]).values('product_title', 'brand', 'model', 'product_id', 'num_reviews', 'subcategories').order_by('-num_reviews').distinct()[:max_products]

        results = {}
        subcategory_results = []
        temp = {}
        models = {}

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['model']
            result['product_title'] = item['product_title']
            subcategories = item['subcategories']
            
            subcategories = json.loads(subcategories) if subcategories is not None else ["all"]

            if result['model'] not in models:
                models[result['model']] = dict()
                models[result['model']]['product_title'] = result['product_title']
                models[result['model']]['brand'] = result['brand']
                models[result['model']]['reviews'] = 0
                models[result['model']]['subcategories'] = subcategories
            # Get Num reviews
            try:
                num_reviews_none = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date], page_num__isnull=False).count()
                num_reviews_not_none = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date], page_num__isnull=True).count()
                num_reviews = max(num_reviews_none, num_reviews_not_none)
                #num_reviews = Reviews.objects.using('scraped').filter(product_id=item['product_id'], review_date__range=[first_date, last_date]).count()
            except Exception as ex:
                print(ex)
                num_reviews = 0
            
            if models[result['model']]['reviews'] != num_reviews:
                models[result['model']]['reviews'] += num_reviews
        
        for model in models:
            for subcategory in models[model]['subcategories']:
                if subcategory in results:
                    results[subcategory].append({"product_title": models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']})
                else:
                    results[subcategory] = [{"product_title": models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']}]

        return Response(results, status=status.HTTP_200_OK)


class IndividualModelMarketShareAPI(APIView):


    def post(self, request):
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
        if period not in (1, 3, 6):
            return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)
        
        results = []
        num_products = 0

        # Filter on only non NULL completed fields
        # NOTE: Here, subcategory is assumed to be a ManyToMany field
        if subcategory is None:
            queryset = ProductAggregate.objects.filter(model=model, brand__isnull=False).values('product_title', 'brand', 'product_id', 'review_info', 'subcategories', 'review_info').order_by().distinct()
        else:
            queryset = ProductAggregate.objects.filter(model=model, brand__isnull=False, subcategories__icontains=f'"{subcategory}"').values('product_title', 'brand', 'product_id', 'review_info', 'subcategories', 'review_info').order_by().distinct()

        total_reviews = 0

        curr = 0
        
        for item in queryset[:max_products]:
            product_id = item['product_id']
            model = item['model']
            product_title = item['product_title']
            if item['subcategories'] is None:
                subcategories = ""
            else:
                subcategories = json.loads(item['subcategories'])
            
            try:
                num_reviews = json.loads(item['review_info'])[str(period)]
                if num_reviews == total_reviews and total_reviews > 0:
                    continue
                total_reviews += num_reviews
            except:
                if curr > 0:
                    continue
                num_reviews = 0
            
            curr += 1

            results.append({"subcategory": subcategories, "product_title": item['product_title'], "brand": item['brand'], "model": model, "num_reviews": total_reviews})
        
        return Response(results, status=status.HTTP_200_OK)


class BrandListAPI(APIView):

    #permission_classes = [IsAuthenticated, AdminAuthenticationPermission]

    def get(self, request, category):
        response = ProductAggregate.objects.filter(category=category, brand__isnull=False, model__isnull=False).order_by('pk').values_list('brand', flat=True).distinct().order_by()
        return Response(response, status=status.HTTP_200_OK)


class ModelListAPI(APIView):

    def get(self, request, category, brand):
        response = ProductAggregate.objects.filter(category=category, brand=brand, model__isnull=False).values_list('model', flat=True).distinct().order_by()
        return Response(response, status=status.HTTP_200_OK)


class RatingsoverTimeAPI(APIView):
    

    def get(self, request, category):
        # For last 3 months
        query_params = request.query_params
        
        if 'brand' not in query_params:
            return Response("Need to specify a brand", status=status.HTTP_400_BAD_REQUEST)
        
        brands = request.GET.getlist('brand')

        NUM_DAYS = 7

        if 'days' not in query_params:
            pass
        else:
            NUM_DAYS = int(query_params['days'])

        final_results = {}

        for brand in brands:
            if brand not in final_results:
                final_results[brand] = []
            
            queryset = ProductAggregate.objects.filter(brand=brand, category=category).values('product_title', 'product_id', 'model')
            
            results = []
            
            models = set()

            last_date = datetime.datetime.now()
            # first_date = last_date - datetime.timedelta(weeks=4*NUM_MONTHS)

            # { "brand":"apple", "model":"airpod" [ { "date":"07-28-2020", "rating":3.4 } ] }

            for item in queryset:
                product_id = item['product_id']
                model = item['model']
                product_title = item['product_title']
                if model in models:
                    continue
                else:
                    models.add(model)
                    result = []
                try:
                    instance = ReviewAggregate.objects.get(product_id=item['product_id'])
                    review_info = json.loads(instance.review_info) if instance.review_info else {}

                    for day in range(NUM_DAYS):
                        # Last 1 week
                        curr_day = last_date - datetime.timedelta(days=day)
                        prev_day = curr_day - datetime.timedelta(days=1)
                        _date = prev_day.strftime("%d/%m/%Y")
                        if _date in review_info:
                            value = review_info[_date]
                            result.append({"date": _date, "rating": value["rating"], "num_reviews": value["num_reviews"]})
                
                    results.append({"product_title": product_title, "model": model, "ratings": result})
                except Exception as ex:
                    print(ex)
                    results.append({"product_title": product_title, "model": model, "ratings": result})
                    continue
            
            final_results[brand] = results
        
        return Response(final_results, status=status.HTTP_200_OK)


class AspectBasedRatingAPI(APIView):


    # { { "brand":"apple", "model":"airpod", "aspect-rating": { "build":3.4, "sound quality":4.2, "noise cancellation":3.7 } },  { "brand":"samsung", "model":"triple m", "aspect-rating": { "build":3.9, "sound quality":4.8, "noise cancellation":4.5 } }, }

    def get(self, request, category):
        query_params = request.query_params
        
        if 'brand' not in query_params:
            return Response("Need to specify a brand", status=status.HTTP_400_BAD_REQUEST)
        
        brands = request.GET.getlist('brand')

        final_results = {}

        for brand in brands:
            queryset = Productdetails.objects.using('scraped').filter(completed__isnull=False, brand=brand, category=category).values('product_title', 'model', 'featurewise_reviews')
            results = []
            models = set()
            for item in queryset:
                if item['model'] in models or item['featurewise_reviews'] is None:
                    continue
                models.add(item['model'])
                _item = {"product_title": item['product_title'], "model": item['model'], "aspect-rating": {**(json.loads(item['featurewise_reviews']))}}
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
            if period not in (1, 3, 6):
                return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        # Filter on only non NULL completed fields
        queryset = ProductAggregate.objects.filter(model__isnull=False, brand__isnull=False, category=category).values('brand', 'model', 'product_id', 'review_info', 'product_title').order_by('-review_info').distinct()

        brands = dict()
        results = []

        curr = 0

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['model']
            result['product_title'] = item['product_title']

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
            
            if curr == max_products:
                break
        
        for brand in brands:
            results.append({'product_title': brands[brand]['product_title'], 'model': brands[brand]['model'], 'brand': brand, 'num_reviews': brands[brand]['reviews']})

        return Response(results, status=status.HTTP_200_OK)


class CategoryMarketShare(APIView):


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
            if period not in (1, 3, 6):
                return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        # Filter on only non NULL completed fields
        queryset = ProductAggregate.objects.filter(model__isnull=False, brand__isnull=False, category=category).values('brand', 'model', 'product_id', 'review_info', 'product_title').order_by('-review_info').distinct()

        models = dict()
        results = []

        curr = 0

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['model']
            result['product_title'] = item['product_title']

            if result['model'] not in models:
                models[result['model']] = dict()
                models[result['model']]['product_title'] = result['product_title']
                models[result['model']]['brand'] = result['brand']
                models[result['model']]['reviews'] = 0
                curr += 1
            # Get Num reviews
            num_reviews = json.loads(item['review_info'])[str(period)]
            
            if models[result['model']]['reviews'] != num_reviews:
                models[result['model']]['reviews'] += num_reviews
            
            if curr == max_products:
                break
        
        for model in models:
            results.append({'product_title': models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']})

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
            if period not in (1, 3, 6):
                return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)

        if subcategory == 'all':
            queryset = ProductAggregate.objects.filter(category=category, model__isnull=False, brand__isnull=False).values('product_title', 'brand', 'model', 'product_id', 'review_info', 'subcategories').order_by('-model').distinct()
        else:
            queryset = ProductAggregate.objects.filter(category=category, model__isnull=False, brand__isnull=False, subcategories__icontains=f'"{subcategory}"').values('product_title', 'brand', 'model', 'product_id', 'review_info', 'subcategories').order_by('-model').distinct()

        results = {}
        subcategory_results = []
        temp = {}
        models = {}

        curr = 0

        for item in queryset:
            result = {}
            result['brand'] = item['brand']
            result['model'] = item['model']
            result['product_title'] = item['product_title']
            subcategories = item['subcategories']
            
            subcategories = json.loads(subcategories) if subcategories is not None else ["all"]

            if result['model'] not in models:
                models[result['model']] = dict()
                models[result['model']]['product_title'] = result['product_title']
                models[result['model']]['brand'] = result['brand']
                models[result['model']]['reviews'] = json.loads(item['review_info'])[str(period)]
                models[result['model']]['subcategories'] = subcategories
                curr += 1

                if curr == max_products:
                    break            
        
        for model in models:
            for subcategory in models[model]['subcategories']:
                if subcategory in results:
                    results[subcategory].append({'product_title': models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']})
                else:
                    results[subcategory] = [{'product_title': models[model]['product_title'], 'model': model, 'brand': models[model]['brand'], 'num_reviews': models[model]['reviews']}]

        return Response(results, status=status.HTTP_200_OK)

class IndividualModelMarketShare(APIView):


    def post(self, request):
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
        if period not in (1, 3, 6):
            return Response("Period must be one of 1, 3 or 6", status=status.HTTP_400_BAD_REQUEST)
        
        last_date = datetime.datetime.now() - datetime.timedelta(days=7)
        first_date = last_date - datetime.timedelta(weeks=4*period)
        
        results = []
        num_products = 0
        models = set()
        curr = 0

        # Filter on only non NULL completed fields
        # NOTE: Here, subcategory is assumed to be a ManyToMany field
        if subcategory is not None:
            queryset = ProductAggregate.objects.filter(subcategory__in=[subcategory], model=model).values('product_title', 'brand', 'model', 'product_id', 'review_info').order_by('-product_id').distinct()
        else:
            queryset = ProductAggregate.objects.filter(model=model).values('product_title', 'brand', 'model', 'product_id', 'review_info').order_by('-product_id').distinct()

        for item in queryset:
            num_reviews = json.loads(item['review_info'])[str(period)]
            if item['model'] not in models:
                models.add(item['model'])
                curr += 1
            results.append({"subcategory": "", "product_title": item['product_title'], "brand": item['brand'], "model": item['model'], "num_reviews": num_reviews})
            if curr == max_products:
                break
        
        return Response(results, status=status.HTTP_200_OK)


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
