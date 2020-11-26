from rest_framework import generics, permissions, status
from rest_framework.response import Response
from knox.models import AuthToken
from .serializers import UserSerializer, RegisterSerializer, LoginSerializer, UserListSerializer
from django.contrib.auth import authenticate, login, logout
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import BasicAuthentication, SessionAuthentication, TokenAuthentication

#from knox.auth import TokenAuthentication

from .models import User

from datetime import datetime
from pytz import timezone

class AdminAuthenticationPermission(permissions.BasePermission):
    ADMIN_ONLY_AUTH_CLASSES = [BasicAuthentication, SessionAuthentication]

    def has_permission(self, request, view):
        user = request.user
        if user and user.is_authenticated:
            return user.is_superuser or \
                not any(isinstance(request._authenticator, x) for x in self.ADMIN_ONLY_AUTH_CLASSES) 
        return False

# Register API
class RegisterAPI(generics.GenericAPIView):
    serializer_class = RegisterSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        return Response({
            "user": UserSerializer(user, context=self.get_serializer_context()).data,
            "token": AuthToken.objects.create(user)[1]
        })

# Login API
class LoginAPI(generics.GenericAPIView):
    authentication_classes = [BasicAuthentication, SessionAuthentication]
    serializer_class = LoginSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data
        login(request, user)
        queryset = User.objects.filter(pk=request.user.pk)
        zone = queryset.first().timezone
        token = AuthToken.objects.create(user)[1]
        
        response = Response({
            "user": UserSerializer(user, context=self.get_serializer_context()).data,
            "token": token
        })

        response.set_cookie(
                    'token',
                    token,
                    httponly=True,
                    samesite='strict'
        )

        return response

# Logout API
class LogoutAPI(APIView):
    permission_classes = [permissions.IsAuthenticated,]
    def get(self, request, format=None):
        queryset = User.objects.filter(pk=request.user.pk)
        queryset.update(is_active=False)
        zone = queryset.first().timezone
        logout(request)
        return Response({"status": "Log Out"})

# Get User API
class UserAPI(generics.GenericAPIView):
    # This route needs to be protected using a valid token
    authentication_classes = [BasicAuthentication, SessionAuthentication]
    permission_classes = [permissions.IsAuthenticated,]
    serializer_class = UserSerializer
    def get_object(self, request):
        # Looks at the token passed through and returns the
        # user associated with the token
        return request.user

    def get(self, request, format=None):
        user = self.get_object(request)
        serializer = self.get_serializer(user)
        return Response(serializer.data)

class DeleteUserAPI(APIView):
    authentication_classes = [BasicAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated, AdminAuthenticationPermission]

    def delete(self, request, pk, format=None):
        if pk == request.user.pk:
            return Response("Cannot delete current user", status=status.HTTP_400_BAD_REQUEST)
        User.objects.filter(pk=pk).delete()
        return Response(status=status.HTTP_200_OK)  

class UserList(generics.GenericAPIView):
    authentication_classes = [BasicAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated, AdminAuthenticationPermission]
    serializer_class = UserListSerializer

    def get_queryset(self):
        return User.objects.all()

    def get(self, request, format=None):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save(owner=self.request.user)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
