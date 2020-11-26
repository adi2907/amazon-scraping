from django.urls import path, include
from .api import RegisterAPI, LoginAPI, LogoutAPI, UserAPI, UserList, DeleteUserAPI
from knox import views as knox_views
from . import views

urlpatterns = [
    path('api/auth', include('knox.urls')),
    path('api/auth/register', RegisterAPI.as_view()),
    path('api/auth/login', LoginAPI.as_view()),
    path('api/auth/user', UserAPI.as_view()),
    path('api/auth/delete/<int:pk>', DeleteUserAPI.as_view()),
    path('api/list', UserList.as_view()),
    path('api/auth/logout', LogoutAPI.as_view()),
    path('api/camera', views.camera),
    path('login', views.login),
    path('register', views.register),
    path('home', views.RoomAPI.as_view()),
    path('', views.lobby),
]
