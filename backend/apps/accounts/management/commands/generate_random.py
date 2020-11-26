import sys
import random
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db.models.base import ObjectDoesNotExist
from django.contrib.auth.models import User

from ...models import User
from apps.activitymanager.models import ActivityPeriod

from pytz import common_timezones

from datetime import datetime, timedelta
import string
from pytz import timezone
import names

def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

class Command(BaseCommand):
    args = 'email'
    help = 'Populate table with random dummy data.'

    def add_arguments(self, parser):
        parser.add_argument('email')

    def handle(self, *args, **options):
        if 'email' not in options:
            sys.stdout.write('You must specify the admin email.\n')
            sys.exit(1)

        email = options['email']
        try:
            user = User.objects.get(email=email)
            if not user.is_superuser:
                sys.stdout.write(f'Non admin user {user.email} already exists. Please create an admin')
                sys.exit(1)
        except ObjectDoesNotExist:
            user = User.objects.create(email=email)
            user.real_name = 'John Smith'
            user.is_superuser = True
            user.is_active = False
            user.set_password('demo')
            user.save()


        for _ in range(10):
            random_name = names.get_full_name()
            random_email = random_name.split(' ')[0].lower() + '@example.com'
            random_password = random_name
            zone = random.choice(common_timezones)
            user, created = User.objects.get_or_create(real_name=random_name, password=random_password, email=random_email, timezone=zone)
            if created:
                user.is_active = False
                user.save()
            start = datetime.now(timezone(zone))
            end = start + timedelta(days=1)
            random_date = start + (end - start) * random.random()
            activity = ActivityPeriod.objects.create(user=user, start_time=start, end_time=random_date)
