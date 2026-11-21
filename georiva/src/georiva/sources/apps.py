from django.apps import AppConfig


class SourcesConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "georiva.sources"
    label = "georivasources"
    verbose_name = "GeoRIVA Sources"

    def ready(self):
        from task_ferry.registry import job_type_registry

        from .job_types import LoaderJobType

        job_type_registry.register(LoaderJobType())

        from georiva.processing.signals import unit_completed

        from .derivation_invocation import on_unit_completed

        unit_completed.connect(on_unit_completed, dispatch_uid="sources.not_ready_wakeup")

        # A sweep reviving a run with no event of its own yet needs to know whose it is.
        from georiva.processing.invocation import register_origin_organisation_resolver

        from .derivation_invocation import organisation_for_origin

        register_origin_organisation_resolver(organisation_for_origin)
