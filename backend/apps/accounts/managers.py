from django.contrib.auth.models import UserManager

class UserManager(UserManager):
    use_in_migrations = True

    def _create_user(self, real_name, email, password, timezone, **extra_fields):
        """
        Creates and saves a User with the given email and password.
        """
        if not email:
            raise ValueError('The given email must be set')
        if timezone is None:
            timezone = 'UTC'
        email = self.normalize_email(email)
        user = self.model(real_name=real_name, email=email, timezone=timezone, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_user(self, real_name, email, password=None, timezone=None, **extra_fields):
        extra_fields.setdefault('is_superuser', False)
        return self._create_user(real_name, email, password, timezone, **extra_fields)

    def create_superuser(self, real_name, email, password, timezone=None, **extra_fields):
        extra_fields.setdefault('is_superuser', True)

        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')

        return self._create_user(real_name, email, password, timezone, **extra_fields) 