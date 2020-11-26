from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from .managers import UserManager
import uuid
import pytz

# Create your models here.
class User(AbstractBaseUser, PermissionsMixin):
    email = models.EmailField(unique=True, db_column='email')
    real_name = models.CharField(max_length=100, blank=True)
    password = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, blank=True)
    created_on = models.DateTimeField(auto_now_add=True)

    # Timezone Field
    TIMEZONES = tuple(zip(pytz.all_timezones, pytz.all_timezones))
    timezone = models.CharField(max_length=32, choices=TIMEZONES, 
    default='UTC')

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['real_name']
