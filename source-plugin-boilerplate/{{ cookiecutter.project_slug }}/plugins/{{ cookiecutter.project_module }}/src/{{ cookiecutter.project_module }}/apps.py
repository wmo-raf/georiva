from django.apps import AppConfig


class {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}Config(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "{{ cookiecutter.project_module }}"
    verbose_name = "{{ cookiecutter.project_name }}"

    # No registration is needed here. GeoRiva auto-discovers every DataFeed
    # subclass (see models.py) and builds its admin form + setup wizard, and
    # adds this app to INSTALLED_APPS by folder discovery. Keep this class bare
    # unless you have signals/checks to wire up in ready().
