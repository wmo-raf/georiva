from django.urls import path
from wagtail import hooks


@hooks.register("register_admin_urls")
def register_lineage_urls():
    from .lineage_views import attempt_inputs

    return [path("derivations/attempts/<uuid:event_id>/inputs/", attempt_inputs, name="derivation_attempt_inputs")]
