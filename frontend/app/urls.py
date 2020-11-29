# -*- encoding: utf-8 -*-
"""
License: MIT
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path
from app import views
from .views import *

urlpatterns = [
    # Matches any html file - to be used for gentella
    # Avoid using your .html in your resources.
    # Or create a separate django app.
    re_path(r'^.*\.html', views.pages, name='pages'),

    # The home page
    path('', views.index, name='home'),
    path('brand-skus', BrandSKUs.as_view()),
    path('brand-share', BrandShare.as_view()),
    path('wired-skus', WiredSKUs.as_view()),
    path('wired-share', WiredShare.as_view()),
    path('wireless-skus', WirelessSKUs.as_view()),
    path('wireless-share', WirelessShare.as_view()),
    path('tws-skus', TwsSKUs.as_view()),
    path('tws-share', TwsShare.as_view()),
    path('brandrating-time',BrandRating.as_view())
    
]
