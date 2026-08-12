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

        # Completion wake-up (ADR-0020): revive dependent products' parked
        # not_ready runs when the input they wait on is derived. Subscribing
        # here keeps ADR-0005's direction — the engine emits, sources listens.
        from georiva.processing.signals import unit_completed

        from .derivation_invocation import on_unit_completed

        unit_completed.connect(on_unit_completed, dispatch_uid="sources.not_ready_wakeup")
