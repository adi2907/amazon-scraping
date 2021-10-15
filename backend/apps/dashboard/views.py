import base64
import io
import json

import matplotlib
import numpy as np
import pandas as pd
from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import render

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

from .models import (Dailyproductlisting, Productdetails, Productlisting,
                     Qanda, Reviews)

plt.ioff()

# Create your views here.
def dashboard(request, category=None):
    if not request.user.is_superuser:
        return HttpResponse("Unauthorized. Only Admins can access this", status=401)
    
    if category is None:
        return HttpResponse("Please Enter the category", status=400)
    
    #queryset = Productlisting.objects.using('scraped').filter(category=category).distinct()
    #queryset = queryset.values_list('product_id', flat=True)
    
    #if queryset.count() == 0:
    #    return HttpResponse(f"No products found for category - {category}", status=204)

    pos = np.arange(10) + 2 

    fig = plt.figure(figsize=(8, 3))
    ax = fig.add_subplot(111)

    ax.barh(pos, np.arange(1, 11), align='center')
    ax.set_yticks(pos)
    ax.set_yticklabels(('#hcsm',
        '#ukmedlibs',
        '#ImmunoChat',
        '#HCLDR',
        '#ICTD2015',
        '#hpmglobal',
        '#BRCA',
        '#BCSM',
        '#BTSM',
        '#OTalk',), 
        fontsize=15)
    ax.set_xticks([])
    ax.invert_yaxis()

    ax.set_xlabel('Popularity')
    ax.set_ylabel('Hashtags')
    ax.set_title('Hashtags')

    plt.tight_layout()

    with io.BytesIO() as buffer:
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()

    graphic = base64.b64encode(image_png)
    graphic = graphic.decode('utf-8')

    plt.close(fig)

    return render(request, 'bar_chart.html', {'graphic': graphic})


def categorypage(request, category):
    return render(request, 'chart.html')
    

def pie_chart(request, category=None, top_n=None):
    if not hasattr(request.user, 'is_superuser') or not request.user.is_superuser:
        return HttpResponse("Unauthorized. Only Admins can access this", status=401)
    
    if category is None:
        return HttpResponse("Please Enter the category", status=400)
    
    queryset = Productlisting.objects.using('scraped').filter(category=category).distinct()
    queryset = queryset.values_list('product_id', flat=True)

    if queryset.count() == 0:
        return HttpResponse(f"No products found for category - {category}", status=204)
    
    detail_queryset = Productdetails.objects.using('scraped').filter(product_id__in=queryset)
    results = [{'num_reviews': instance.num_reviews, 'brand': json.loads(instance.byline_info)['info'].replace('Brand:', '').strip(), 'product_id': instance.product_id} for instance in detail_queryset if instance.num_reviews is not None]

    # Now annotate the review count
    #review_queryset = Reviews.objects.using('scraped').values("product_id").annotate(Count("product_id"))

    #cache = dict()

    #for review_dict in review_queryset:
    #    if review_dict['product_id'] is not None:
    #        cache[review_dict['product_id']] = review_dict['product_id__count']
    
    #for result in results:
    #    if result['product_id'] in cache:
    #        result['num_reviews'] = cache[result['product_id']]
    
    # Add up only unique brands now
    unique_results = dict()
    for result in results:
        if result['brand'] is None:
            continue
        if result['brand'] not in unique_results:
            unique_results[result['brand']] = result['num_reviews']
        else:
            unique_results[result['brand']] += result['num_reviews']

    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    #labels = [brand for brand in unique_results]
    #sizes = [num_reviews for _, num_reviews in unique_results.items()]
    #explode = [0 for _ in unique_results]

    df = pd.DataFrame(data={'brand': list(unique_results.keys()), 'reviews': list(unique_results.values())}).sort_values('reviews', ascending=False)

    if top_n is None or top_n <= 0:
        TOP_N = 4
    else:
        TOP_N = top_n

    new_df = df[:TOP_N].copy() # Get the top N
    others = pd.DataFrame(data={'brand': ['others'], 'reviews': [df['reviews'][TOP_N:].sum()]})

    new_df = pd.concat([new_df, others])

    fig, ax = plt.subplots(figsize = (12, 5))
    new_df.plot(kind = 'pie', y = 'reviews', labels = new_df['brand'], autopct='%1.1f%%', shadow=True, startangle=90, ax=ax)
    #ax.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
    #        shadow=True, startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    with io.BytesIO() as buffer:
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()

    graphic = base64.b64encode(image_png)
    graphic = graphic.decode('utf-8')

    plt.close(fig)

    return render(request, 'pie_chart.html', {'graphic': graphic})
