from django.apps import apps
from rest_framework import serializers

from .models import (Dailyproductlisting, Productdetails, Productlisting,
                     Qanda, Reviews)


class ProductListingSerializer(serializers.ModelSerializer):
    class Meta:
        model = Productlisting
        fields = '__all__'


class ProductDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Productdetails
        fields = '__all__'


class DailyProductListingSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dailyproductlisting
        fields = '__all__'


class QandASerializer(serializers.ModelSerializer):
    class Meta:
        model = Qanda
        fields = '__all__'


class ReviewSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reviews
        fields = '__all__'
