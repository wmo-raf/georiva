def setup(settings):
    """
    Called after georiva builds its Django settings but before Django starts.

    Registers this plugin as a Django app so its DataFeed model (and migrations)
    are discovered. georiva auto-builds the admin viewset for every DataFeed
    subclass, so no wagtail_hooks wiring is needed here.
    """
    if "{{ cookiecutter.project_module }}" not in settings.INSTALLED_APPS:
        settings.INSTALLED_APPS += ["{{ cookiecutter.project_module }}"]
