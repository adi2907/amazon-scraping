# Create your models here
from django.db import models


class Productlisting(models.Model):
    product_id = models.CharField(primary_key=True, max_length=16)
    category = models.CharField(blank=True, null=True, max_length=100)
    title = models.TextField(blank=True, null=True)
    product_url = models.TextField(blank=True, null=True)
    avg_rating = models.FloatField(blank=True, null=True)
    total_ratings = models.IntegerField(blank=True, null=True)
    price = models.IntegerField(blank=True, null=True)
    old_price = models.IntegerField(blank=True, null=True)
    secondary_information = models.TextField(blank=True, null=True)
    image = models.CharField(blank=True, null=True, max_length=1000)
    short_title = models.CharField(blank=True, null=True, max_length=100)
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)
    brand = models.CharField(blank=True, null=True, max_length=100)
    model = models.CharField(blank=True, null=True, max_length=100)
    date_completed = models.DateTimeField(blank=True, null=True)
    detail_completed = models.DateTimeField(blank=True, null=True)
    class Meta:
        managed = False
        db_table = 'ProductListing'


class Dailyproductlisting(models.Model):
    id = models.AutoField(primary_key=True)
    product_id = models.CharField(blank=True, null=True, max_length=16)
    category = models.CharField(blank=True, null=True, max_length=100)
    avg_rating = models.FloatField(blank=True, null=True)
    total_ratings = models.IntegerField(blank=True, null=True)
    price = models.IntegerField(blank=True, null=True)
    old_price = models.IntegerField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    serial_no = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'DailyProductListing'


class Productdetails(models.Model):
    product_id = models.CharField(primary_key=True, max_length=16)
    product_title = models.TextField(blank=True, null=True)
    byline_info = models.TextField(blank=True, null=True)
    num_reviews = models.IntegerField(blank=True, null=True)
    answered_questions = models.CharField(blank=True, null=True, max_length=100)
    curr_price = models.FloatField(null=True, blank=True)
    features = models.TextField(blank=True, null=True)
    offers = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    product_details = models.TextField(blank=True, null=True)
    featurewise_reviews = models.TextField(blank=True, null=True)
    customer_qa = models.TextField(blank=True, null=True)
    histogram = models.TextField(blank=True, null=True)
    reviews_url = models.TextField(blank=True, null=True)
    created_on = models.DateTimeField(blank=True, null=True)
    subcategories = models.TextField(blank=True, null=True)
    brand = models.CharField(blank=True, null=True, max_length=100)
    model = models.CharField(blank=True, null=True, max_length=100)
    date_completed = models.DateTimeField(blank=True, null=True)
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)
    product_overview = models.TextField(blank=True, null=True)
    related_products = models.TextField(blank=True, null=True)
    
    class Meta:
        managed = False
        db_table = 'ProductDetails'


class Qanda(models.Model):
    id = models.AutoField(primary_key=True)
    product = models.ForeignKey(Productlisting, models.DO_NOTHING, blank=True, null=True)
    question = models.TextField(blank=True, null=True)
    answer = models.TextField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    page_num = models.IntegerField(blank=True, null=True)
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)

    class Meta:
        managed = False
        db_table = 'QandA'


class Reviews(models.Model):
    id = models.AutoField(primary_key=True)
    product = models.ForeignKey(Productlisting, models.DO_NOTHING, blank=True, null=True)
    rating = models.FloatField(blank=True, null=True)
    review_date = models.DateTimeField(blank=True, null=True)
    country = models.CharField(blank=True, null=True, max_length=40)
    title = models.CharField(blank=True, null=True, max_length=1000)
    body = models.TextField(blank=True, null=True)
    product_info = models.TextField(blank=True, null=True)
    verified_purchase = models.IntegerField(blank=True, null=True)
    helpful_votes = models.IntegerField(blank=True, null=True)
    page_num = models.IntegerField(blank=True, null=True)
    is_duplicate = models.BooleanField(blank=True, null=True)
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)

    class Meta:
        managed = False
        db_table = 'Reviews'


class Sponsoredproductdetails(models.Model):
    product_id = models.CharField(primary_key=True, max_length=16)
    product_title = models.TextField(blank=True, null=True)
    byline_info = models.TextField(blank=True, null=True)
    num_reviews = models.IntegerField(blank=True, null=True)
    answered_questions = models.CharField(blank=True, null=True, max_length=100)
    curr_price = models.FloatField(blank=True, null=True)
    features = models.TextField(blank=True, null=True)
    offers = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    product_details = models.TextField(blank=True, null=True)
    featurewise_reviews = models.TextField(blank=True, null=True)
    customer_qa = models.TextField(blank=True, null=True)
    customer_lazy = models.IntegerField(blank=True, null=True)
    histogram = models.TextField(blank=True, null=True)
    reviews_url = models.TextField(blank=True, null=True)
    created_on = models.DateTimeField(blank=True, null=True)
    subcategories = models.TextField(blank=True, null=True)
    is_sponsored = models.BooleanField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'SponsoredProductDetails'


class Sentimentanalysis(models.Model):
    id = models.AutoField(primary_key=True)
    product_id = models.CharField(max_length=16, blank=True, null=True)
    positive_sentiments = models.TextField(blank=True, null=True)
    negative_sentiments = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'SentimentAnalysis'


class Sentimentbreakdown(models.Model):
    product_id = models.CharField(max_length=16, primary_key=True)
    sentiments = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'SentimentBreakdown'


class ProductAggregate(models.Model):
    product_id = models.CharField(max_length=16, primary_key=True, db_column="product_id")
    brand = models.CharField(blank=True, null=True, max_length=100, db_column="brand")
    model = models.CharField(blank=True, null=True, max_length=100, db_column="model")
    subcategories = models.TextField(blank=True, null=True, db_column="subcategories")
    review_info = models.TextField(blank=True, null=True, db_column="review_info")
    category = models.CharField(blank=True, null=True, max_length=100, db_column="category")
    product_title = models.TextField(blank=True, null=True)
    featurewise_reviews = models.TextField(blank=True, null=True)
    num_reviews = models.IntegerField(blank=True, null=True)
    curr_price = models.FloatField(null=True, blank=True)
    is_duplicate = models.BooleanField(blank=True, null=True)
    short_title = models.TextField(blank=True, null=True)
    listing_reviews = models.IntegerField(blank=True, null=True)
    sentiments = models.TextField(blank=True, null=True, db_column="sentiments")
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)
    total_reviews = models.TextField(blank=True, null=True)


class ReviewAggregate(models.Model):
    product_id = models.CharField(max_length=16, primary_key=True, db_column="product_id")
    brand = models.CharField(blank=True, null=True, max_length=100, db_column="brand")
    model = models.CharField(blank=True, null=True, max_length=100, db_column="model")
    subcategories = models.TextField(blank=True, null=True, db_column="subcategories")
    review_info = models.TextField(blank=True, null=True, db_column="review_info")
    category = models.CharField(blank=True, null=True, max_length=100, db_column="category")
    product_title = models.TextField(blank=True, null=True)
    num_reviews = models.IntegerField(blank=True, null=True)
    curr_price = models.FloatField(null=True, blank=True)
    short_title = models.TextField(blank=True, null=True)
    duplicate_set = models.CharField(blank=True, null=True, max_length=100)
    total_reviews = models.TextField(blank=True, null=True)


class SubcategoryMap(models.Model):
    category = models.CharField(primary_key=True, max_length=100, db_column="category")
    subcategory_map = models.TextField(blank=True, null=True, db_column="subcategory_map")
