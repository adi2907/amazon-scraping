from rest_framework import serializers
from itertools import chain
from django.contrib.auth import authenticate

from .models import User

# User Serializer
class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = '__all__'

# Register Serializer
class RegisterSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'real_name', 'email', 'password', 'timezone')
        extra_kwargs = {'password': {'write_only': True}}

    def create(self, validated_data):
        user = User.objects.create_user(
            *(chain(validated_data[field] for field in ['real_name', 'email', 'password', 'timezone']))
            )
        return user

# Login Serializer
class LoginSerializer(serializers.Serializer):
    email = serializers.CharField()
    password = serializers.CharField()

    def validate(self, data):
        queryset = User.objects.filter(email=data['email'])
        if queryset.count() > 0:
            instance = queryset.first()
            real_name = instance.real_name
            instance.is_active = True
            instance.save()
            user = authenticate(**data)
            if user:
                return user
            else:
                raise serializers.ValidationError("Incorrect Credentials") 
        raise serializers.ValidationError("Incorrect Credentials")

class UserListSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'real_name', 'email', 'is_active', )