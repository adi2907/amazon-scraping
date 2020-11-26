from django.urls import path

from . import api, views

urlpatterns = [
    path('home', api.DashboardListing.as_view()),
    path('home/<str:category>/barchart', views.dashboard),
    path('home/<str:category>/piechart', views.pie_chart),
    path('home/<str:category>/chart', views.categorypage),
    path('home/<str:category>/piechart/<int:top_n>', views.pie_chart),
    
    path('productlisting', api.DashboardProductListing.as_view()),
    path('productlisting/<int:page_no>', api.DashboardProductListing.as_view()),
    path('dailyproductlisting', api.DashboardDailyProductListing.as_view()),
    path('dailyproductlisting/<int:page_no>', api.DashboardDailyProductListing.as_view()),
    path('reviews/<str:product_id>', api.DashboardReviews.as_view()),
    path('reviews/<str:product_id>/<int:page_no>', api.DashboardReviews.as_view()),
    path('qanda/<str:product_id>', api.DashboardQandA.as_view()),
    path('qanda/<str:product_id>/<int:page_no>', api.DashboardQandA.as_view()),
    path('email/<str:category>', api.SendEmailAPI.as_view()),

    path('brandlist/<str:category>', api.BrandListAPI.as_view()),
    path('modellist/<str:category>/<str:brand>', api.ModelListAPI.as_view()),

    path('brand-model/<str:category>', api.BrandandModelListAPI.as_view()),

    path('fetchsubcategories/<str:category>', api.FetchSubcategories.as_view()),

    path('modelmarketshare/<str:category>/<int:period>/<int:max_products>', api.CummulativeModelMarketShare.as_view()),
    path('subcategorymarketshare/<str:category>/<str:subcategory>/<int:period>/<int:max_products>', api.SubCategoryMarketShare.as_view()),

    path('brandmarketshare/<str:category>/<int:period>/<int:max_products>', api.BrandMarketShare.as_view()),
    
    path('individualmarketshare', api.IndividualModelMarketShare.as_view()),
    path('rating/<str:category>', api.RatingsoverTimeAPI.as_view()),
    path('review-count/<str:category>', api.ReviewCount.as_view()),
    path('aspect-rating/<str:category>', api.AspectBasedRatingAPI.as_view()),

    path('review-breakdown/<str:category>', api.ReviewBreakDownAPI.as_view()),
    path('fetch-reviews/<str:category>', api.SentimentReviewsAPI.as_view()),

    path('featurelist/<str:category>', api.GetFeaturesAPI.as_view()),
]
