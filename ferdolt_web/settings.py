import logging
import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured
import environ

env = environ.Env(
    DEBUG=(bool, True),
    SECRET_KEY=(str, 'django-insecure-=*8sy^+&sru&vcexj*l720sg#8bq%v&2ms(8ew3!xao9t(o64!'),
)

environ.Env.read_env()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = env("SECRET_KEY")

DEBUG = env("DEBUG")

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
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
    'communication'
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
]

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.BasicAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ]
}

ROOT_URLCONF = 'ferdolt_web.urls'

FERNET_KEY = 'kXTmzsvlLlvhiTTGWWn4yhGZi143jZUisNC_X8BINO0='

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
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
        "ATOMIC_REQUESTS": True,
    }
}


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

MEDIA_ROOT = "files"

MEDIA_URL = "media/"

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

PATH_TO_KITCHEN = r"C:\pdi-ce-9.3.0.0-428\data-integration\Kitchen.bat"

PATH_TO_PENTAHO_DATABASE = os.path.join( BASE_DIR, 'pentaho_files', 'BikeStores', 'data', 'database.sqlite' )

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
