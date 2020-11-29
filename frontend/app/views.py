# -*- encoding: utf-8 -*-
"""
License: MIT
Copyright (c) 2019 - present AppSeed.us
"""

from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.template import loader
from django.http import HttpResponse
from django import template
from django.views.generic import View
from django.http import JsonResponse
import requests

from rest_framework.views import APIView
from rest_framework.response import Response
from django.contrib.auth import get_user_model

@login_required(login_url="/login/")
def index(request):
    return render(request, "index.html")

@login_required(login_url="/login/")
def pages(request):
    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:
        
        load_template = request.path.split('/')[-1]
        html_template = loader.get_template( load_template )
        return HttpResponse(html_template.render(context, request))
        
    except template.TemplateDoesNotExist:

        html_template = loader.get_template( 'error-404.html' )
        return HttpResponse(html_template.render(context, request))

    except:
    
        html_template = loader.get_template( 'error-500.html' )
        return HttpResponse(html_template.render(context, request))

class BrandSKUs(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/modelmarketshare/headphones/8/6').json()
        return Response(data)

class BrandShare(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/brandmarketshare/headphones/8/6').json()
        return Response(data)

class WiredSKUs(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/modelmarketshare/headphones/8/6?subcategory=wired').json()
        return Response(data)

class WiredShare(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/brandmarketshare/headphones/8/6?subcategory=wired').json()
        return Response(data)

class WirelessSKUs(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/modelmarketshare/headphones/8/6?subcategory=wireless').json()
        return Response(data)

class WirelessShare(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/brandmarketshare/headphones/8/6?subcategory=wireless').json()
        return Response(data)

class TwsSKUs(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/modelmarketshare/headphones/8/6?subcategory=True%20Wireless').json()
        return Response(data)

class TwsShare(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/brandmarketshare/headphones/8/6?subcategory=True%20Wireless').json()
        return Response(data)

class BrandRating(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):
        url = 'http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/rating/headphones'
        data = requests.get('http://ec2-3-7-69-109.ap-south-1.compute.amazonaws.com:8000/api/dashboard/rating/headphones?brand=ptron').json()
        return Response(data)