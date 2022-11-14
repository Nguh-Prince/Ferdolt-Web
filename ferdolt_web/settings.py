import logging
import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured
import environ

env = environ.Env(
    DEBUG=(bool, True),
    SECRET_KEY=(str, 'django-insecure-=*8sy^+&sru&vcexj*l720sg#8bq%v&2ms(8ew3!xao9t(o64!'),
    SERVER_ID=(str, 'W2X91')
)

environ.Env.read_env()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = env("SECRET_KEY")

DEBUG = env("DEBUG")

SERVER_ID = env('SERVER_ID')

ALLOWED_HOSTS = []

EMAIL_HOST=env('EMAIL_HOST')
EMAIL_PORT=env('EMAIL_PORT')
EMAIL_HOST_USER=env('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD=env('EMAIL_HOST_PASSWORD')
EMAIL_USE_TLS=True

# Application definition

INSTALLED_APPS = [
    # 'admin_interface',
    # 'colorfield',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'ferdolt',
    'groups',
    'rest_framework',
    "frontend",
    'flux',
    'channels',
    'communication',
    'users',
    'common',
    "rest_framework.authtoken",
    'simple_history',
    'huey.contrib.djhuey',
]

ASGI_APPLICATION = 'ferdolt_web.asgi.application'

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer"
    }
}

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'simple_history.middleware.HistoryRequestMiddleware',
]

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.TokenAuthentication',  # <-- And here
        "rest_framework.authentication.SessionAuthentication",
    ],
}

ROOT_URLCONF = 'ferdolt_web.urls'

FERNET_KEY = 'kXTmzsvlLlvhiTTGWWn4yhGZi143jZUisNC_X8BINO0='

APPEND_SLASH = False

try: 
    FERNET_KEY = env("FERNET_KEY")
except KeyError as e:
    logging.warning(f"In mysite.settings, KeyError: {e}")
except ImproperlyConfigured as e:
    logging.warning(f"In mysite.settings, ImproperlyConfigured: {e}")

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'ferdolt_web.wsgi.application'

DATABASES = {
    # 'default': {
    #     'ENGINE': 'django.db.backends.sqlite3',
    #     'NAME': BASE_DIR / 'old_db.sqlite3',
    #     "ATOMIC_REQUESTS": True,
    # }
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': env("DATABASE_NAME"),
        'USER': env('DATABASE_USERNAME'),
        'PASSWORD': env('DATABASE_PASSWORD'),
        'HOST': env('DATABASE_HOST'),
        'PORT': env('DATABASE_PORT')
    }
}

# HUEY = {
#     'huey_class': 'huey.RedisHuey',  # Huey implementation to use.
#     'name': DATABASES['default']['NAME'],  # Use db name for huey.
#     'results': True,  # Store return values of tasks.
#     'store_none': False,  # If a task returns None, do not save to results.
#     'immediate': False,  # If DEBUG=True, run synchronously.
#     'utc': True,  # Use UTC for all times internally.
#     'blocking': True,  # Perform blocking pop rather than poll Redis.
#     'connection': {
#         'host': 'localhost',
#         'port': 6379,
#         'db': 0,
#         'connection_pool': None,  # Definitely you should use pooling!
#         # ... tons of other options, see redis-py for details.

#         # huey-specific connection parameters.
#         'read_timeout': 1,  # If not polling (blocking pop), use timeout.
#         'url': None,  # Allow Redis config via a DSN.
#     },
#     'consumer': {
#         'workers': 1,
#         'worker_type': 'thread',
#         'initial_delay': 0.1,  # Smallest polling interval, same as -d.
#         'backoff': 1.15,  # Exponential backoff using this rate, -b.
#         'max_delay': 10.0,  # Max possible polling interval, -m.
#         'scheduler_interval': 1,  # Check schedule every second, -s.
#         'periodic': True,  # Enable crontab feature.
#         'check_worker_health': True,  # Enable worker health checks.
#         'health_check_interval': 1,  # Check worker health every second.
#     },
# }

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


STATIC_URL = 'static/'

LOGIN_URL = '/login/'

MEDIA_ROOT = "files"

MEDIA_URL = "media/"

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} ({asctime}): {message}',
            'style': '{',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
        'info_file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, "logs", "info.log"),
            'formatter': 'simple',
        },
        'debug_file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, "logs", "debug.log"),
            'formatter': 'simple',
        },
        'warning_file': {
            'level': 'WARNING',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, "logs", "warning.log"),
            'formatter': 'simple',
        },
        'error_file': {
            'level': 'ERROR',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, "logs", "error.log"),
            'formatter': 'simple',
        },
        'critical_file': {
            'level': 'CRITICAL',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, "logs", "critical.log"),
            'formatter': 'simple',
        }
    },
    'root': {
        'handlers': ['console'],
        'level': 'WARNING',
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.request': {
            'handlers': ['console', 'error_file', 'debug_file', 'info_file', 'critical_file', 'warning_file'],
            'level': 'DEBUG'
        },
        '': {
            'handlers': ['console', 'error_file', 'debug_file', 'info_file', 'critical_file', 'warning_file'],
            'level': 'DEBUG'
        }
    }
}
