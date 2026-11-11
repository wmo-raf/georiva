from django.apps import AppConfig


class ProcessingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "georiva.processing"
    label = "georivaprocessing"
    verbose_name = "GeoRIVA Processing"

    def ready(self):
        from django.db.models.signals import pre_delete

        # Register the built-in recipe families on the engine.
        from georiva.processing import recipes  # noqa: F401
        from georiva.processing.input_deletion import on_input_deleting

        # A derived item follows its inputs out (ADR 0041): read the lineage
        # before the cascade removes it.
        for sender in ("georivacore.Item", "georivastaging.StagingItem"):
            pre_delete.connect(on_input_deleting, sender=sender, dispatch_uid=f"input_deleting:{sender}")
