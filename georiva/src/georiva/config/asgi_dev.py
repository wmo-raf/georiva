"""
ASGI entrypoint for the dev server (``uvicorn --reload``).

Does what ``runserver`` did on top of the ASGI app: with DEBUG on, serve static
files straight from the apps, with no collectstatic.  Production serves static
files through nginx and never imports this module.
"""

from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler

from .asgi import application as _application

application = ASGIStaticFilesHandler(_application)
