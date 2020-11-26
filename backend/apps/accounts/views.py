from django.shortcuts import render
from .models import User
from .serializers import UserSerializer, UserListSerializer

from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import BasicAuthentication, SessionAuthentication

from django.http import HttpResponseRedirect

from knox.auth import TokenAuthentication

from rest_framework import generics, permissions

from rest_framework.views import APIView

class RoomAPI(generics.GenericAPIView):
    # This route needs to be protected using a valid token
    authentication_classes = [SessionAuthentication]
    permission_classes = [permissions.IsAuthenticated,]
    serializer_class = UserSerializer
    def get_object(self, request):
        # Looks at the token passed through and returns the
        # user associated with the token
        return request.user

    def get(self, request, format=None):
        user = self.get_object(request)
        if user.is_superuser:
        	serializer = UserListSerializer(User.objects.all(), many=True)
        	user_serializer = self.get_serializer(user)
        	return render(request, 'accounts/user.html', {'user_id': user_serializer.data['id'], 'real_name': user_serializer.data['real_name'], 'data': serializer.data, 'is_admin': True})
        serializer = self.get_serializer(user)
        return render(request, 'accounts/user.html', {'user_id': serializer.data['id'], 'real_name': serializer.data['real_name'], 'data': serializer.data, 'is_admin': False})
        #return render(request, 'accounts/user.html', serializer.data)

# Create your views here.
def login(request):
    return render(request, 'accounts/login.html', {})

def register(request):
	return render(request, 'accounts/register.html', {})

def lobby(request):
    if request.user:
        queryset = User.objects.filter(id=request.user.pk)
        if queryset.count() > 0:
                return HttpResponseRedirect("/home")
        else:
                return HttpResponseRedirect("/login")


def room(request, user_id):
	if request.user:
		queryset = User.objects.filter(id=user_id)
		if queryset.count() > 0:
			user = queryset.first()
			if user.is_superuser:
				return render(request, 'accounts/user.html', {'user_id': user.id, 'real_name': user.real_name, 'email': user.email, 'is_admin': True})
			else:
				return render(request, 'accounts/user.html', {'user_id': user.id, 'real_name': user.real_name, 'email': user.email, 'is_admin': False})
		else:
			return render(request, 'accounts/404_user.html', {'user_id': user_id})
	else:
		return render(request, 'accounts/404_user.html', {'user_id': user_id})


def camera(request):
    if request.user:
        queryset = User.objects.filter(id=request.user.pk)
        if queryset.count() > 0:
            user = queryset.first()
            email = user.email
            return render(request, 'accounts/camera.html', {'email': email})
        else:
            return render(request, 'accounts/404_user.html', {'user_id': request.user.pk})
    else:
        return render(request, 'accounts/404_user.html', {'user_id': 0})
